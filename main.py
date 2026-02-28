from fastapi import FastAPI, UploadFile, File, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google.cloud import storage, bigquery
from typing import Optional
import io
import pandas as pd
from fastapi.responses import StreamingResponse
import uuid
import os
from datetime import datetime
import base64
from pydantic import BaseModel


app = FastAPI(title="CarbonSight API", version="1.0.0")

PROJECT_ID  = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
DATASET     = "sustainability_ds"
TABLE       = "operations"
REGION      = os.environ.get("REGION", "India")

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

DEFAULT_FACTORS = {
    "grid_electricity": 0.82,
    "renewable_energy": 0.05,
    "freight_truck":    0.0525,
    "ev_transport":     0.021,
    "fuel":             2.68,
    "courier":          0.18,
}

_factors_cache: dict | None = None


def get_active_factors() -> dict:
    global _factors_cache
    if _factors_cache is not None:
        return _factors_cache
    try:
        bq   = bigquery.Client()
        rows = list(bq.query(f"""
            SELECT factor_key, factor_value
            FROM `{DATASET}.app_settings`
            WHERE setting_type = 'emission_factor'
        """).result())
        if rows:
            loaded         = {r.factor_key: float(r.factor_value) for r in rows}
            _factors_cache = {**DEFAULT_FACTORS, **loaded}
        else:
            _factors_cache = dict(DEFAULT_FACTORS)
    except Exception:
        _factors_cache = dict(DEFAULT_FACTORS)
    return _factors_cache


@app.get("/")
def home():
    return FileResponse("static/index.html")


# ═══════════════════════════════════════════════════
#  SETTINGS
# ═══════════════════════════════════════════════════

class FactorUpdate(BaseModel):
    grid_electricity: float
    renewable_energy: float
    freight_truck:    float
    ev_transport:     float
    fuel:             float
    courier:          float


@app.get("/settings/factors")
def get_factors():
    factors = get_active_factors()
    return {
        "factors":       factors,
        "defaults":      DEFAULT_FACTORS,
        "is_customised": factors != DEFAULT_FACTORS
    }


@app.post("/settings/factors")
def save_factors(payload: FactorUpdate):
    global _factors_cache
    new_factors = {
        "grid_electricity": payload.grid_electricity,
        "renewable_energy": payload.renewable_energy,
        "freight_truck":    payload.freight_truck,
        "ev_transport":     payload.ev_transport,
        "fuel":             payload.fuel,
        "courier":          payload.courier,
    }
    try:
        bq = bigquery.Client()
        # Recreate table to bypass streaming buffer DELETE restriction
        bq.query(f"""
            CREATE OR REPLACE TABLE `{DATASET}.app_settings` (
                setting_type  STRING,
                factor_key    STRING,
                factor_value  FLOAT64,
                updated_at    TIMESTAMP,
                updated_by    STRING
            )
        """).result()
        for k, v in new_factors.items():
            bq.query(f"""
                INSERT INTO `{DATASET}.app_settings`
                (setting_type, factor_key, factor_value, updated_at, updated_by)
                VALUES ('emission_factor', '{k}', {v}, CURRENT_TIMESTAMP(), 'user')
            """).result()
    except Exception as e:
        return {"error": "Failed to save settings", "details": str(e)}
    _factors_cache = {**DEFAULT_FACTORS, **new_factors}
    return {
        "status":  "saved",
        "factors": _factors_cache,
        "message": "Emission factors updated successfully."
    }


@app.post("/settings/factors/reset")
def reset_factors():
    global _factors_cache
    try:
        bq = bigquery.Client()
        bq.query(f"""
            CREATE OR REPLACE TABLE `{DATASET}.app_settings` (
                setting_type  STRING,
                factor_key    STRING,
                factor_value  FLOAT64,
                updated_at    TIMESTAMP,
                updated_by    STRING
            )
        """).result()
    except Exception:
        pass
    _factors_cache = dict(DEFAULT_FACTORS)
    return {"status": "reset", "factors": DEFAULT_FACTORS, "message": "Factors restored to system defaults."}


# ═══════════════════════════════════════════════════
#  SCENARIOS
#  NOTE: /scenarios/prefill-style specific routes must
#  come BEFORE /{scenario_id} wildcard routes
# ═══════════════════════════════════════════════════

class ScenarioSave(BaseModel):
    name:        str
    description: str = ""
    levers:      dict
    result:      dict


@app.post("/scenarios")
def save_scenario(payload: ScenarioSave):
    import json as _json
    bq          = bigquery.Client()
    scenario_id = str(uuid.uuid4())
    try:
        levers_str = _json.dumps(payload.levers).replace("'", "\\'")
        result_str = _json.dumps(payload.result).replace("'", "\\'")
        name_str   = payload.name.replace("'", "\\'")
        desc_str   = payload.description.replace("'", "\\'")
        bq.query(f"""
            INSERT INTO `{DATASET}.simulation_scenarios`
            (scenario_id, name, description, levers_json, result_json, created_at)
            VALUES ('{scenario_id}', '{name_str}', '{desc_str}', '{levers_str}', '{result_str}', CURRENT_TIMESTAMP())
        """).result()
    except Exception as e:
        return {"error": "Save failed", "details": str(e)}
    return {"status": "saved", "scenario_id": scenario_id, "name": payload.name}


@app.get("/scenarios")
def list_scenarios():
    import json as _json
    bq = bigquery.Client()
    try:
        rows = list(bq.query(f"""
            SELECT scenario_id, name, description, levers_json, result_json, created_at
            FROM `{DATASET}.simulation_scenarios`
            ORDER BY created_at DESC
            LIMIT 50
        """).result())
    except Exception as e:
        return {"data": [], "error": str(e)}
    data = []
    for r in rows:
        try:
            levers = _json.loads(r.levers_json)
            result = _json.loads(r.result_json)
        except Exception:
            levers, result = {}, {}
        data.append({
            "scenario_id": r.scenario_id,
            "name":        r.name,
            "description": r.description,
            "levers":      levers,
            "result":      result,
            "created_at":  str(r.created_at),
        })
    return {"count": len(data), "data": data}


@app.get("/scenarios/{scenario_id}")
def get_scenario(scenario_id: str):
    import json as _json
    bq   = bigquery.Client()
    rows = list(bq.query(f"""
        SELECT scenario_id, name, description, levers_json, result_json, created_at
        FROM `{DATASET}.simulation_scenarios`
        WHERE scenario_id = '{scenario_id}'
        LIMIT 1
    """).result())
    if not rows:
        return {"error": "Not found"}
    r = rows[0]
    return {
        "scenario_id": r.scenario_id,
        "name":        r.name,
        "description": r.description,
        "levers":      _json.loads(r.levers_json),
        "result":      _json.loads(r.result_json),
        "created_at":  str(r.created_at),
    }


@app.delete("/scenarios/{scenario_id}")
def delete_scenario(scenario_id: str):
    try:
        bq = bigquery.Client()
        bq.query(f"""
            DELETE FROM `{DATASET}.simulation_scenarios`
            WHERE scenario_id = '{scenario_id}'
        """).result()
        return {"status": "deleted", "scenario_id": scenario_id}
    except Exception as e:
        return {"error": "Delete failed", "details": str(e)}


# ─────────────────────────────────────────
# UPLOAD CSV
# ─────────────────────────────────────────

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    filename = f"{uuid.uuid4()}_{file.filename}"
    try:
        storage_client = storage.Client()
        bucket         = storage_client.bucket(BUCKET_NAME)
        blob           = bucket.blob(filename)
        blob.upload_from_file(file.file)

        bq_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format        = bigquery.SourceFormat.CSV,
            skip_leading_rows    = 1,
            write_disposition    = "WRITE_APPEND",
            schema=[
                bigquery.SchemaField("product_id",   "STRING"),
                bigquery.SchemaField("units_sold",   "INTEGER"),
                bigquery.SchemaField("energy_kwh",   "FLOAT"),
                bigquery.SchemaField("transport_km", "FLOAT"),
                bigquery.SchemaField("record_date",  "DATE"),
            ],
            allow_quoted_newlines = True,
            ignore_unknown_values = True
        )
        uri      = f"gs://{BUCKET_NAME}/{filename}"
        load_job = bq_client.load_table_from_uri(uri, f"{DATASET}.{TABLE}", job_config=job_config)
        load_job.result()
        rows_loaded = load_job.output_rows

        bq_client.query(f"""
            INSERT INTO sustainability_ds.upload_log
            (upload_id, upload_time, file_name, rows_loaded, status)
            VALUES ('{filename}', CURRENT_TIMESTAMP(), '{file.filename}', {rows_loaded}, 'SUCCESS')
        """).result()
        return {"message": "Upload successful", "rows": rows_loaded}
    except Exception as e:
        return {"error": "Upload failed", "details": str(e)}


# ─────────────────────────────────────────
# UPLOAD UTILITY BILL
# ─────────────────────────────────────────

@app.post("/upload-bill")
async def upload_bill(bill_type: str, amount: float, units: float, region: str, month: str):
    if not month or len(month) != 7 or month[4] != "-":
        return {"error": "Invalid month format. Use YYYY-MM"}
    bq_client     = bigquery.Client()
    bill_id       = str(uuid.uuid4())
    af            = get_active_factors()
    FACTORS       = {"electricity": af["grid_electricity"], "fuel": af["fuel"], "courier": af["courier"]}
    estimated_co2 = units * FACTORS.get(bill_type, 0.0)
    try:
        bq_client.query(f"""
            INSERT INTO sustainability_ds.utility_bills
            (bill_id, bill_type, amount, units, region, month, estimated_co2, upload_time)
            VALUES ('{bill_id}', '{bill_type}', {amount}, {units}, '{region}',
                    PARSE_DATE('%Y-%m-%d', '{month}-01'), {estimated_co2}, CURRENT_TIMESTAMP())
        """).result()
    except Exception as e:
        return {"error": "Bill upload failed", "details": str(e)}
    return {"status": "success", "bill_id": bill_id, "estimated_co2": estimated_co2}


# ─────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────

@app.get("/metrics")
def get_metrics(since: Optional[str] = Query(None)):
    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    is_custom        = af != DEFAULT_FACTORS
    energy_ref       = "Custom Factor" if is_custom else "CEA India 2024"
    transport_ref    = "Custom Factor" if is_custom else "GLEC 2024"
    bq               = bigquery.Client()
    where            = f"WHERE o.record_date >= DATE('{since}')" if since else ""
    query = f"""
        SELECT
            o.product_id,
            IFNULL(p.product_name, o.product_id) AS product_name,
            IFNULL(p.category, 'Unknown')         AS category,
            SUM(o.units_sold)   AS units,
            SUM(o.energy_kwh)   AS energy,
            SUM(o.transport_km) AS km
        FROM sustainability_ds.operations o
        LEFT JOIN sustainability_ds.products p ON o.product_id = p.product_id
        {where}
        GROUP BY o.product_id, product_name, category
        ORDER BY product_name
    """
    data = []
    for r in bq.query(query).result():
        units         = r.units  or 0
        energy_co2    = (r.energy or 0) * energy_factor
        transport_co2 = (r.km    or 0) * transport_factor
        data.append({
            "product_id":       r.product_id,
            "product_name":     r.product_name,
            "category":         r.category,
            "total_units_sold": int(units),
            "energy_co2_kg":    round(float(energy_co2),    2),
            "transport_co2_kg": round(float(transport_co2), 2),
            "total_co2_kg":     round(float(energy_co2 + transport_co2), 2),
            "energy_ref":       energy_ref,
            "transport_ref":    transport_ref,
        })
    return {"count": len(data), "data": data}


# ─────────────────────────────────────────
# UPLOAD HISTORY
# ─────────────────────────────────────────

@app.get("/uploads")
def get_upload_history():
    bq   = bigquery.Client()
    data = [
        {"upload_id": r.upload_id, "upload_time": str(r.upload_time),
         "file_name": r.file_name, "rows_loaded": r.rows_loaded, "status": r.status}
        for r in bq.query("""
            SELECT upload_id, upload_time, file_name, rows_loaded, status
            FROM sustainability_ds.upload_log
            WHERE status != 'DELETED'
            ORDER BY upload_time DESC LIMIT 20
        """).result()
    ]
    return {"count": len(data), "data": data}


@app.delete("/uploads/{upload_id}")
def delete_upload(upload_id: str):
    bigquery.Client().query(f"""
        UPDATE sustainability_ds.upload_log SET status = 'DELETED'
        WHERE upload_id = '{upload_id}'
    """).result()
    return {"status": "deleted", "upload_id": upload_id}


# ─────────────────────────────────────────
# TRENDS
# ─────────────────────────────────────────

@app.get("/trends")
def get_trends():
    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    bq               = bigquery.Client()
    data, prev_cpu   = [], None
    for row in bq.query(f"""
        SELECT FORMAT_DATE('%Y-%m', record_date) AS month,
               SUM(energy_kwh * {energy_factor} + transport_km * {transport_factor}) AS total_co2,
               SUM(units_sold) AS total_units
        FROM sustainability_ds.operations
        GROUP BY month ORDER BY month
    """).result():
        total = float(row.total_co2   or 0)
        units = float(row.total_units or 0)
        cpu   = total / units if units > 0 else 0
        trend = round(((cpu - prev_cpu) / prev_cpu) * 100, 2) if prev_cpu else None
        prev_cpu = cpu
        data.append({"month": row.month, "co2_per_unit": round(cpu, 4), "efficiency_change": trend})
    return {"data": data}


# ─────────────────────────────────────────
# BILL INSIGHTS
# ─────────────────────────────────────────

@app.get("/bill-insights")
def get_bill_insights():
    bq   = bigquery.Client()
    data = [
        {"month": r.month, "region": r.region, "bill_type": r.bill_type, "estimated_co2": float(r.estimated_co2)}
        for r in bq.query("""
            SELECT FORMAT_DATE('%Y-%m', month) AS month, region, bill_type, estimated_co2
            FROM sustainability_ds.bill_emissions ORDER BY month
        """).result()
    ]
    return {"data": data}


# ─────────────────────────────────────────
# COMPANY KPIs
# ─────────────────────────────────────────

@app.get("/company-kpis")
def get_company_kpis():
    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    bq               = bigquery.Client()
    try:
        ops   = list(bq.query(f"SELECT SUM(energy_kwh * {energy_factor} + transport_km * {transport_factor}) AS c FROM sustainability_ds.operations").result())
        bills = list(bq.query("SELECT SUM(estimated_co2) AS c FROM sustainability_ds.bill_emissions").result())
        ops_co2   = float(ops[0].c   or 0) if ops   else 0
        bills_co2 = float(bills[0].c or 0) if bills else 0
        return {"total_company_co2": ops_co2 + bills_co2, "product_co2": ops_co2, "utility_co2": bills_co2}
    except Exception as e:
        return {"total_company_co2": 0, "error": str(e)}


# ─────────────────────────────────────────
# TOTAL FOOTPRINT
# ─────────────────────────────────────────

@app.get("/total-footprint")
def total_footprint():
    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    bq               = bigquery.Client()
    data = [
        {"month": r.month, "product_co2": float(r.product_co2),
         "utility_co2": float(r.utility_co2), "total_co2": float(r.total_co2)}
        for r in bq.query(f"""
            WITH product AS (
                SELECT FORMAT_DATE('%Y-%m', record_date) AS month,
                       SUM(energy_kwh * {energy_factor} + transport_km * {transport_factor}) AS co2
                FROM sustainability_ds.operations GROUP BY month
            ),
            utility AS (
                SELECT FORMAT_DATE('%Y-%m', month) AS month, SUM(estimated_co2) AS co2
                FROM sustainability_ds.bill_emissions GROUP BY month
            )
            SELECT COALESCE(p.month, u.month) AS month,
                   IFNULL(p.co2, 0) AS product_co2,
                   IFNULL(u.co2, 0) AS utility_co2,
                   IFNULL(p.co2, 0) + IFNULL(u.co2, 0) AS total_co2
            FROM product p FULL OUTER JOIN utility u ON p.month = u.month
            ORDER BY month
        """).result()
    ]
    return {"data": data}


# ─────────────────────────────────────────
# RESET ALL
# ─────────────────────────────────────────

@app.delete("/reset-all")
def reset_all_data():
    bq = bigquery.Client()
    bq.query("DELETE FROM sustainability_ds.operations    WHERE TRUE").result()
    bq.query("DELETE FROM sustainability_ds.utility_bills WHERE TRUE").result()
    bq.query("DELETE FROM sustainability_ds.upload_log    WHERE TRUE").result()
    return {"status": "all_data_cleared"}


# ─────────────────────────────────────────
# EXPORT EXCEL
# ─────────────────────────────────────────

@app.get("/export/excel")
def export_excel():
    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    bq               = bigquery.Client()
    df               = bq.query(f"""
        SELECT o.product_id,
               IFNULL(p.product_name, o.product_id) AS product_name,
               IFNULL(p.category, 'Unknown')         AS category,
               SUM(o.units_sold)                                                  AS units_sold,
               ROUND(SUM(o.energy_kwh    * {energy_factor}),    2)                AS energy_co2,
               ROUND(SUM(o.transport_km  * {transport_factor}), 2)                AS transport_co2,
               ROUND(SUM(o.energy_kwh    * {energy_factor}
                       + o.transport_km  * {transport_factor}), 2)                AS total_co2
        FROM sustainability_ds.operations o
        LEFT JOIN sustainability_ds.products p ON o.product_id = p.product_id
        GROUP BY o.product_id, product_name, category
    """).to_dataframe()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Product_Emissions")
    output.seek(0)
    return StreamingResponse(output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=carbon_report.xlsx"})

# ─────────────────────────────────────────
# EXPORT PDF  — Premium B2B Report
# ─────────────────────────────────────────

from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph,
    Spacer, PageBreak, Image, HRFlowable, KeepTogether
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT


# ── Brand colours (match the dark UI) ─────────────────────────
C_DARK    = HexColor("#0b0f1a")   # page background / cover
C_SURFACE = HexColor("#1c2537")   # card / table header
C_ACCENT  = HexColor("#22d3a5")   # teal highlight
C_ACCENT2 = HexColor("#3b82f6")   # blue highlight
C_WARN    = HexColor("#f59e0b")   # amber
C_TEXT    = HexColor("#1e293b")   # body text (dark on white pages)
C_MUTED   = HexColor("#64748b")   # secondary text
C_LIGHT   = HexColor("#f1f5f9")   # light row background
C_WHITE   = white
C_SCOPE1  = HexColor("#ef4444")
C_SCOPE2  = HexColor("#3b82f6")
C_SCOPE3  = HexColor("#a855f7")


def build_pdf_styles():
    base = getSampleStyleSheet()

    styles = {
        "cover_title": ParagraphStyle(
            "cover_title",
            fontName="Helvetica-Bold", fontSize=32,
            textColor=C_WHITE, leading=40, spaceAfter=8,
        ),
        "cover_sub": ParagraphStyle(
            "cover_sub",
            fontName="Helvetica", fontSize=14,
            textColor=C_ACCENT, leading=20, spaceAfter=6,
        ),
        "cover_meta": ParagraphStyle(
            "cover_meta",
            fontName="Helvetica", fontSize=10,
            textColor=HexColor("#94a3b8"), leading=16,
        ),
        "section_heading": ParagraphStyle(
            "section_heading",
            fontName="Helvetica-Bold", fontSize=14,
            textColor=C_TEXT, leading=20,
            spaceBefore=18, spaceAfter=6,
            borderPad=0,
        ),
        "sub_heading": ParagraphStyle(
            "sub_heading",
            fontName="Helvetica-Bold", fontSize=11,
            textColor=C_TEXT, leading=16,
            spaceBefore=10, spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body",
            fontName="Helvetica", fontSize=9,
            textColor=C_TEXT, leading=15, spaceAfter=6,
        ),
        "body_muted": ParagraphStyle(
            "body_muted",
            fontName="Helvetica", fontSize=8,
            textColor=C_MUTED, leading=13, spaceAfter=4,
        ),
        "kpi_value": ParagraphStyle(
            "kpi_value",
            fontName="Helvetica-Bold", fontSize=22,
            textColor=C_ACCENT, leading=26, alignment=TA_CENTER,
        ),
        "kpi_label": ParagraphStyle(
            "kpi_label",
            fontName="Helvetica", fontSize=8,
            textColor=C_MUTED, leading=12, alignment=TA_CENTER,
        ),
        "footer": ParagraphStyle(
            "footer",
            fontName="Helvetica", fontSize=7,
            textColor=C_MUTED, leading=10, alignment=TA_CENTER,
        ),
        "tag": ParagraphStyle(
            "tag",
            fontName="Helvetica-Bold", fontSize=7,
            textColor=C_WHITE, leading=10, alignment=TA_CENTER,
        ),
        "table_header": ParagraphStyle(
            "table_header",
            fontName="Helvetica-Bold", fontSize=8,
            textColor=C_WHITE, leading=11, alignment=TA_CENTER,
        ),
        "table_cell": ParagraphStyle(
            "table_cell",
            fontName="Helvetica", fontSize=8,
            textColor=C_TEXT, leading=11,
        ),
        "table_cell_r": ParagraphStyle(
            "table_cell_r",
            fontName="Helvetica", fontSize=8,
            textColor=C_TEXT, leading=11, alignment=TA_RIGHT,
        ),
        "methodology": ParagraphStyle(
            "methodology",
            fontName="Helvetica", fontSize=7.5,
            textColor=C_MUTED, leading=12,
            leftIndent=8, spaceAfter=3,
        ),
    }
    return styles


def cover_page(elems, styles, generated_at, total_co2, top_product, total_units):
    """Full dark cover page."""
    from reportlab.platypus import Table as RLTable, TableStyle as RLTableStyle

    W, H = A4

    # Dark cover background table (full page width)
    cover_bg = RLTable(
        [[Paragraph("", styles["body"])]],
        colWidths=[W - 40*mm],
        rowHeights=[60*mm],
    )
    cover_bg.setStyle(RLTableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), C_DARK),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
    ]))

    # Logo row
    logo_row = RLTable(
        [[Paragraph("🌿  CarbonSight", ParagraphStyle(
            "logo", fontName="Helvetica-Bold", fontSize=13,
            textColor=C_ACCENT, leading=16))]],
        colWidths=[W - 40*mm],
    )
    logo_row.setStyle(RLTableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), C_DARK),
        ("TOPPADDING",   (0,0), (-1,-1), 14),
        ("BOTTOMPADDING",(0,0), (-1,-1), 0),
        ("LEFTPADDING",  (0,0), (-1,-1), 8),
    ]))

    elems.append(logo_row)
    elems.append(Spacer(1, 10*mm))

    # Main title block
    title_tbl = RLTable(
        [[Paragraph("Sustainability<br/>Emissions Report", styles["cover_title"])],
         [Paragraph("Carbon Footprint Analysis &amp; Green Intervention Modelling", styles["cover_sub"])],
         [Paragraph(f"Generated: {generated_at} &nbsp;|&nbsp; Powered by Google Cloud BigQuery", styles["cover_meta"])]],
        colWidths=[W - 40*mm],
    )
    title_tbl.setStyle(RLTableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_DARK),
        ("LEFTPADDING",   (0,0), (-1,-1), 8),
        ("TOPPADDING",    (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    elems.append(title_tbl)
    elems.append(Spacer(1, 10*mm))

    # Teal divider line
    elems.append(HRFlowable(width="100%", thickness=2, color=C_ACCENT, spaceAfter=8*mm))

    # KPI summary cards on cover
    kpi_data = [[
        Paragraph(f"{total_co2:,.1f}", styles["kpi_value"]),
        Paragraph(f"{total_units:,}",  styles["kpi_value"]),
        Paragraph(top_product,          ParagraphStyle("kpip", fontName="Helvetica-Bold",
                                         fontSize=14, textColor=C_ACCENT,
                                         leading=18, alignment=TA_CENTER)),
    ], [
        Paragraph("kg CO2 Total Emissions", styles["kpi_label"]),
        Paragraph("Total Units Sold",        styles["kpi_label"]),
        Paragraph("Top Emitting Product",    styles["kpi_label"]),
    ]]
    kpi_tbl = RLTable(kpi_data, colWidths=[(W - 40*mm) / 3] * 3, rowHeights=[18*mm, 8*mm])
    kpi_tbl.setStyle(RLTableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_SURFACE),
        ("BACKGROUND",    (0, 0), (0, -1),  HexColor("#1a2e26")),
        ("BACKGROUND",    (1, 0), (1, -1),  HexColor("#1a2231")),
        ("BACKGROUND",    (2, 0), (2, -1),  HexColor("#1e1a2e")),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEAFTER",     (0, 0), (1, -1),  0.5, HexColor("#2d3a4a")),
        ("ROUNDEDCORNERS",(0, 0), (-1, -1), [6, 6, 6, 6]),
    ]))
    elems.append(kpi_tbl)
    elems.append(Spacer(1, 8*mm))

    # Disclaimer strip
    disc_tbl = RLTable(
        [[Paragraph(
            "This report is generated from operational data uploaded to CarbonSight. "
            "Emission factors follow CEA India 2024, GLEC Framework 2024, IPCC AR6, and GHG Protocol standards. "
            "Figures represent Scope 1, 2 &amp; 3 emissions per the GHG Protocol Corporate Standard.",
            styles["cover_meta"]
        )]],
        colWidths=[W - 40*mm],
    )
    disc_tbl.setStyle(RLTableStyle([
        ("BACKGROUND",   (0,0),(-1,-1), HexColor("#111827")),
        ("LEFTPADDING",  (0,0),(-1,-1), 10),
        ("RIGHTPADDING", (0,0),(-1,-1), 10),
        ("TOPPADDING",   (0,0),(-1,-1), 8),
        ("BOTTOMPADDING",(0,0),(-1,-1), 8),
    ]))
    elems.append(disc_tbl)
    elems.append(PageBreak())


def section_header(elems, styles, text):
    elems.append(HRFlowable(width="100%", thickness=1, color=C_ACCENT, spaceAfter=2*mm))
    elems.append(Paragraph(text, styles["section_heading"]))


def scope_summary_table(elems, styles, scope1, scope2, scope3, page_width):
    total = scope1 + scope2 + scope3 or 1

    def scope_cell(label, color, value, pct, desc):
        from reportlab.platypus import Table as T2, TableStyle as TS2
        inner = T2(
            [[Paragraph(label, ParagraphStyle("sl", fontName="Helvetica-Bold",
                        fontSize=8, textColor=color, leading=10))],
             [Paragraph(f"{value:,.1f} kg", ParagraphStyle("sv", fontName="Helvetica-Bold",
                        fontSize=13, textColor=C_TEXT, leading=16))],
             [Paragraph(f"{pct:.1f}% of total", ParagraphStyle("sp", fontName="Helvetica",
                        fontSize=7, textColor=C_MUTED, leading=10))],
             [Paragraph(desc, ParagraphStyle("sd", fontName="Helvetica",
                        fontSize=7, textColor=C_MUTED, leading=10))]],
            colWidths=[(page_width - 40*mm) / 3 - 4*mm],
        )
        inner.setStyle(TS2([
            ("TOPPADDING",    (0,0),(-1,-1), 3),
            ("BOTTOMPADDING", (0,0),(-1,-1), 2),
            ("LEFTPADDING",   (0,0),(-1,-1), 10),
            ("BACKGROUND",    (0,0),(-1,-1), C_LIGHT),
        ]))
        return inner

    col_w = (page_width - 40*mm) / 3

    from reportlab.platypus import Table as T3, TableStyle as TS3
    row = T3([[
        scope_cell("SCOPE 1 — Direct Combustion",   C_SCOPE1, scope1, scope1/total*100, "Own fleet fuel & on-site combustion"),
        scope_cell("SCOPE 2 — Purchased Electricity", C_SCOPE2, scope2, scope2/total*100, "Grid & renewable electricity consumed"),
        scope_cell("SCOPE 3 — Value Chain",           C_SCOPE3, scope3, scope3/total*100, "Outsourced logistics & courier"),
    ]], colWidths=[col_w]*3)
    row.setStyle(TS3([
        ("ALIGN",         (0,0),(-1,-1), "LEFT"),
        ("VALIGN",        (0,0),(-1,-1), "TOP"),
        ("LEFTPADDING",   (0,0),(-1,-1), 2),
        ("RIGHTPADDING",  (0,0),(-1,-1), 2),
        ("TOPPADDING",    (0,0),(-1,-1), 0),
        ("BOTTOMPADDING", (0,0),(-1,-1), 0),
    ]))
    elems.append(row)
    elems.append(Spacer(1, 4*mm))


@app.post("/export/pdf")
async def export_pdf(request: Request):
    import json as _json
    data = await request.json()

    def decode_image(b64):
        if not b64:
            return None
        try:
            _, enc = b64.split(",", 1)
            return io.BytesIO(base64.b64decode(enc))
        except Exception:
            return None

    trend_buf = decode_image(data.get("trend"))
    bill_buf  = decode_image(data.get("bill"))
    total_buf = decode_image(data.get("total"))

    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    fuel_factor      = af["fuel"]
    courier_factor   = af["courier"]

    bq = bigquery.Client()

    # ── Fetch product metrics ─────────────────────────────
    prod_rows = list(bq.query(f"""
        SELECT
            IFNULL(p.product_name, o.product_id)  AS product,
            IFNULL(p.category, 'Unknown')          AS category,
            SUM(o.units_sold)                                                   AS units,
            ROUND(SUM(o.energy_kwh   * {energy_factor}),    2)                 AS energy_co2,
            ROUND(SUM(o.transport_km * {transport_factor}), 2)                 AS transport_co2,
            ROUND(SUM(o.energy_kwh   * {energy_factor}
                    + o.transport_km * {transport_factor}), 2)                 AS total_co2
        FROM sustainability_ds.operations o
        LEFT JOIN sustainability_ds.products p ON o.product_id = p.product_id
        GROUP BY product, category ORDER BY total_co2 DESC
    """).result())

    # ── Fetch utility bill totals ─────────────────────────
    bill_rows = list(bq.query("""
        SELECT 
            bill_type, 
            SUM(estimated_co2) AS co2,
            SUM(units) AS units
        FROM sustainability_ds.utility_bills
        WHERE estimated_co2 IS NOT NULL
        GROUP BY bill_type
    """).result())
    bill_map = {
        r.bill_type: {
            "co2":   float(r.co2   or 0),
            "units": float(r.units or 0)
        } for r in bill_rows
    }
    # ── Fetch monthly trend ───────────────────────────────
    trend_rows = list(bq.query(f"""
        SELECT FORMAT_DATE('%b %Y', record_date) AS month,
               ROUND(SUM(energy_kwh * {energy_factor} + transport_km * {transport_factor}), 1) AS product_co2
        FROM sustainability_ds.operations
        GROUP BY FORMAT_DATE('%b %Y', record_date), record_date
        ORDER BY MIN(record_date)
        LIMIT 12
    """).result())

    # ── Compute summary KPIs ──────────────────────────────
    total_product_co2 = sum(float(r.total_co2 or 0) for r in prod_rows)
    total_utility_co2 = sum(v["co2"] for v in bill_map.values())
    total_co2         = total_product_co2 + total_utility_co2
    total_units       = sum(int(r.units or 0) for r in prod_rows)
    top_product       = prod_rows[0].product if prod_rows else "—"
    co2_per_unit      = total_product_co2 / total_units if total_units else 0

    # GHG Scopes (product only — bills classified separately)
    scope1_co2 = bill_map.get("fuel",        {}).get("co2", 0)
    scope2_co2 = total_product_co2 + bill_map.get("electricity", {}).get("co2", 0)
    scope3_co2 = bill_map.get("courier",     {}).get("co2", 0)

    generated_at = datetime.now().strftime("%d %B %Y, %H:%M")
    styles       = build_pdf_styles()
    W, H         = A4

    # ── Build document ────────────────────────────────────
    buffer = io.BytesIO()
    doc    = SimpleDocTemplate(
        buffer, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=16*mm,  bottomMargin=16*mm,
        title="CarbonSight Emissions Report",
        author="CarbonSight",
    )
    elems = []

    # ── PAGE 1: Cover ─────────────────────────────────────
    cover_page(elems, styles, generated_at, total_co2, top_product, total_units)

    # ── PAGE 2: Executive Summary ─────────────────────────
    section_header(elems, styles, "01 · Executive Summary")

    summary_text = (
        f"This report summarises the carbon footprint of your operations for the current reporting period. "
        f"Total emissions across all products and utility sources stand at "
        f"<b>{total_co2:,.1f} kg CO2e</b>, comprising "
        f"<b>{total_product_co2:,.1f} kg</b> from product operations (energy &amp; transport) and "
        f"<b>{total_utility_co2:,.1f} kg</b> from utility bills (electricity, fuel &amp; courier). "
        f"Across <b>{total_units:,} units sold</b>, the average carbon intensity is "
        f"<b>{co2_per_unit:.3f} kg CO2e per unit</b>. "
        f"The highest-emitting product line is <b>{top_product}</b>. "
        f"Emissions are classified per the GHG Protocol Corporate Standard into Scope 1 (direct combustion), "
        f"Scope 2 (purchased electricity), and Scope 3 (value-chain transport &amp; logistics)."
    )
    elems.append(Paragraph(summary_text, styles["body"]))
    elems.append(Spacer(1, 5*mm))

    # ── KPI row ───────────────────────────────────────────
    from reportlab.platypus import Table as T, TableStyle as TS

    col_w4 = (W - 40*mm) / 4
    kpi2_data = [[
        Paragraph(f"{total_co2:,.0f}", styles["kpi_value"]),
        Paragraph(f"{total_product_co2:,.0f}", styles["kpi_value"]),
        Paragraph(f"{total_utility_co2:,.0f}", styles["kpi_value"]),
        Paragraph(f"{co2_per_unit:.3f}", styles["kpi_value"]),
    ],[
        Paragraph("Total CO2e (kg)",      styles["kpi_label"]),
        Paragraph("Product CO2 (kg)",     styles["kpi_label"]),
        Paragraph("Utility CO2 (kg)",     styles["kpi_label"]),
        Paragraph("kg CO2 / Unit",        styles["kpi_label"]),
    ]]
    kpi2 = T(kpi2_data, colWidths=[col_w4]*4, rowHeights=[14*mm, 7*mm])
    kpi2.setStyle(TS([
        ("BACKGROUND",    (0,0),(-1,-1), C_LIGHT),
        ("BACKGROUND",    (0,0),(0,-1),  HexColor("#ecfdf5")),
        ("BACKGROUND",    (1,0),(1,-1),  HexColor("#eff6ff")),
        ("BACKGROUND",    (2,0),(2,-1),  HexColor("#fefce8")),
        ("BACKGROUND",    (3,0),(3,-1),  HexColor("#faf5ff")),
        ("ALIGN",         (0,0),(-1,-1), "CENTER"),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ("TOPPADDING",    (0,0),(-1,-1), 6),
        ("BOTTOMPADDING", (0,0),(-1,-1), 4),
        ("LINEAFTER",     (0,0),(2,-1),  0.5, HexColor("#e2e8f0")),
    ]))
    elems.append(kpi2)
    elems.append(Spacer(1, 6*mm))

    # ── GHG Scope breakdown ───────────────────────────────
    section_header(elems, styles, "02 · GHG Protocol Scope Breakdown")
    elems.append(Paragraph(
        "Emissions are categorised per the GHG Protocol Corporate Standard. "
        "Scope 1 covers direct combustion (own fleet &amp; on-site fuel). "
        "Scope 2 covers indirect emissions from purchased electricity. "
        "Scope 3 covers all other indirect emissions including outsourced freight and courier logistics.",
        styles["body"]
    ))
    elems.append(Spacer(1, 3*mm))
    scope_summary_table(elems, styles, scope1_co2, scope2_co2, scope3_co2, W)

    # ── Charts ────────────────────────────────────────────
    section_header(elems, styles, "03 · Emissions Trend Analysis")
    chart_w = (W - 40*mm)
    if trend_buf:
        elems.append(Image(trend_buf, width=chart_w, height=55*mm))
    elems.append(Spacer(1, 2*mm))
    elems.append(Paragraph(
        "Monthly CO2 efficiency trend (kg CO2 per unit shipped). A downward trajectory indicates "
        "improving operational efficiency. The 3-month rolling average smooths short-term volatility.",
        styles["body_muted"]
    ))

    if bill_buf or total_buf:
        elems.append(Spacer(1, 4*mm))
        chart_half = (W - 44*mm) / 2
        chart_row_data = [[
            Image(bill_buf,  width=chart_half, height=50*mm) if bill_buf  else Paragraph("No utility data", styles["body_muted"]),
            Image(total_buf, width=chart_half, height=50*mm) if total_buf else Paragraph("No footprint data", styles["body_muted"]),
        ]]
        chart_row = T(chart_row_data, colWidths=[chart_half + 2*mm, chart_half + 2*mm])
        chart_row.setStyle(TS([
            ("LEFTPADDING",  (0,0),(-1,-1), 0),
            ("RIGHTPADDING", (0,0),(-1,-1), 0),
            ("TOPPADDING",   (0,0),(-1,-1), 0),
            ("BOTTOMPADDING",(0,0),(-1,-1), 0),
            ("ALIGN",        (0,0),(-1,-1), "CENTER"),
        ]))
        elems.append(chart_row)
        elems.append(Spacer(1, 1*mm))
        cap_row = T([[
            Paragraph("Utility Bill Emissions by Type (kg CO2/month)", styles["body_muted"]),
            Paragraph("Total Company Carbon Footprint — Product vs Utility (kg CO2/month)", styles["body_muted"]),
        ]], colWidths=[chart_half + 2*mm, chart_half + 2*mm])
        cap_row.setStyle(TS([("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0)]))
        elems.append(cap_row)

    elems.append(PageBreak())

    # ── PAGE 3: Product Breakdown ─────────────────────────
    section_header(elems, styles, "04 · Product Emissions Breakdown")
    elems.append(Paragraph(
        "The table below shows per-product CO2 emissions calculated using the active emission factors. "
        "Energy emissions are classified as Scope 2; transport emissions as Scope 3.",
        styles["body"]
    ))
    elems.append(Spacer(1, 3*mm))

    # Table header
    prod_header = [
        Paragraph("Product",        styles["table_header"]),
        Paragraph("Category",       styles["table_header"]),
        Paragraph("Units Sold",     styles["table_header"]),
        Paragraph("Energy CO2 kg\n(Scope 2)", styles["table_header"]),
        Paragraph("Transport CO2 kg\n(Scope 3)", styles["table_header"]),
        Paragraph("Total CO2 kg",   styles["table_header"]),
        Paragraph("% of Total",     styles["table_header"]),
    ]
    prod_table_data = [prod_header]
    for r in prod_rows:
        pct = (float(r.total_co2 or 0) / total_product_co2 * 100) if total_product_co2 else 0
        prod_table_data.append([
            Paragraph(r.product,                  styles["table_cell"]),
            Paragraph(r.category,                 styles["table_cell"]),
            Paragraph(f"{int(r.units or 0):,}",   styles["table_cell_r"]),
            Paragraph(f"{float(r.energy_co2 or 0):,.2f}",    styles["table_cell_r"]),
            Paragraph(f"{float(r.transport_co2 or 0):,.2f}", styles["table_cell_r"]),
            Paragraph(f"{float(r.total_co2 or 0):,.2f}",     styles["table_cell_r"]),
            Paragraph(f"{pct:.1f}%",              styles["table_cell_r"]),
        ])

    col_widths = [45*mm, 28*mm, 22*mm, 26*mm, 28*mm, 24*mm, 18*mm]
    prod_tbl = T(prod_table_data, colWidths=col_widths, repeatRows=1)
    prod_tbl.setStyle(TS([
        ("BACKGROUND",    (0, 0), (-1, 0),  C_SURFACE),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_WHITE),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_WHITE, C_LIGHT]),
        ("GRID",          (0, 0), (-1, -1), 0.3, HexColor("#e2e8f0")),
        ("LINEBELOW",     (0, 0), (-1, 0),  1.5, C_ACCENT),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (1, -1),  6),
    ]))
    elems.append(prod_tbl)

    # ── Utility Bill Summary ──────────────────────────────
    if bill_map:
        elems.append(Spacer(1, 6*mm))
        section_header(elems, styles, "05 · Utility Bill Emissions Summary")
        bill_header = [
            Paragraph("Bill Type",       styles["table_header"]),
            Paragraph("Units Consumed",  styles["table_header"]),
            Paragraph("Emission Factor", styles["table_header"]),
            Paragraph("CO2 Emitted (kg)",styles["table_header"]),
        ]
        bill_table_data = [bill_header]
        factor_labels   = {
            "electricity": (f"{energy_factor} kg/kWh", "kWh"),
            "fuel":        (f"{fuel_factor} kg/L",     "litres"),
            "courier":     (f"{courier_factor} kg/kg", "kg"),
        }
        for btype, vals in bill_map.items():
            flabel, ulabel = factor_labels.get(btype, ("—", "units"))
            bill_table_data.append([
                Paragraph(btype.capitalize(),              styles["table_cell"]),
                Paragraph(f"{vals['units']:,.1f} {ulabel}",styles["table_cell_r"]),
                Paragraph(flabel,                          styles["table_cell_r"]),
                Paragraph(f"{vals['co2']:,.2f}",           styles["table_cell_r"]),
            ])
        bill_tbl = T(bill_table_data, colWidths=[40*mm, 45*mm, 45*mm, 45*mm], repeatRows=1)
        bill_tbl.setStyle(TS([
            ("BACKGROUND",    (0, 0), (-1, 0),  C_SURFACE),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  C_WHITE),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_WHITE, C_LIGHT]),
            ("GRID",          (0, 0), (-1, -1), 0.3, HexColor("#e2e8f0")),
            ("LINEBELOW",     (0, 0), (-1, 0),  1.5, C_ACCENT),
            ("ALIGN",         (0, 1), (-1, -1), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        elems.append(bill_tbl)

    elems.append(PageBreak())

    # ── PAGE 4: Methodology & Factors ────────────────────
    section_header(elems, styles, "06 · Emission Factors &amp; Methodology")
    elems.append(Paragraph(
        "All CO2 equivalent figures are calculated using the emission factors below. "
        "Factors are configurable per organisation and default to the values shown. "
        "Custom factors are noted where applied.",
        styles["body"]
    ))
    elems.append(Spacer(1, 3*mm))

    is_custom = af != DEFAULT_FACTORS
    factor_rows = [
        ["Grid Electricity",  f"{af['grid_electricity']} kg CO2/kWh",  "CEA India 2024",         "Scope 2"],
        ["Renewable Energy",  f"{af['renewable_energy']} kg CO2/kWh",  "IPCC SRREN lifecycle",   "Scope 2"],
        ["Freight Truck",     f"{af['freight_truck']} kg CO2/km",      "GLEC Framework 2024",    "Scope 3"],
        ["EV Fleet",          f"{af['ev_transport']} kg CO2/km",       "BEV India Grid 2024",    "Scope 3"],
        ["Diesel / Petrol",   f"{af['fuel']} kg CO2/litre",            "IPCC AR6 (fixed)",       "Scope 1"],
        ["Courier / Parcel",  f"{af['courier']} kg CO2/kg shipped",    "GHG Protocol 2023",      "Scope 3"],
    ]
    meth_header = [
        Paragraph("Factor",          styles["table_header"]),
        Paragraph("Value Applied",   styles["table_header"]),
        Paragraph("Source",          styles["table_header"]),
        Paragraph("GHG Scope",       styles["table_header"]),
    ]
    meth_data = [meth_header]
    for row in factor_rows:
        meth_data.append([Paragraph(c, styles["table_cell"]) for c in row])

    meth_tbl = T(meth_data, colWidths=[45*mm, 45*mm, 65*mm, 25*mm], repeatRows=1)
    meth_tbl.setStyle(TS([
        ("BACKGROUND",    (0, 0), (-1, 0),  C_SURFACE),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  C_WHITE),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [C_WHITE, C_LIGHT]),
        ("GRID",          (0, 0), (-1, -1), 0.3, HexColor("#e2e8f0")),
        ("LINEBELOW",     (0, 0), (-1, 0),  1.5, C_ACCENT),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    elems.append(meth_tbl)
    elems.append(Spacer(1, 3*mm))

    if is_custom:
        elems.append(Paragraph(
            "⚠  Custom emission factors are active for this organisation. "
            "Values shown above reflect the organisation-specific configuration.",
            ParagraphStyle("warn", fontName="Helvetica-Bold", fontSize=8,
                           textColor=C_WARN, leading=12, leftIndent=4)
        ))

    elems.append(Spacer(1, 6*mm))
    section_header(elems, styles, "07 · Standards &amp; Compliance Notes")

    standards = [
        ("GHG Protocol Corporate Standard",
         "The primary framework used to classify emissions into Scope 1, 2, and 3 categories. "
         "www.ghgprotocol.org"),
        ("CEA India Emission Factor 2024",
         "Grid electricity emission factor for India published by the Central Electricity Authority. "
         "Factor: 0.82 kg CO2/kWh (default)."),
        ("GLEC Framework 2024",
         "Global Logistics Emissions Council framework for transport emission calculations. "
         "Freight truck default: 0.0525 kg CO2/km."),
        ("IPCC AR6",
         "Sixth Assessment Report of the Intergovernmental Panel on Climate Change. "
         "Diesel combustion factor: 2.68 kg CO2/litre (fixed, not region-dependent)."),
        ("IPCC SRREN",
         "Special Report on Renewable Energy Sources. "
         "Solar/wind lifecycle factor: 0.05 kg CO2/kWh."),
    ]
    for title, desc in standards:
        elems.append(Paragraph(f"<b>{title}</b>", styles["methodology"]))
        elems.append(Paragraph(desc, styles["methodology"]))
        elems.append(Spacer(1, 2*mm))

    # ── Footer ────────────────────────────────────────────
    elems.append(Spacer(1, 8*mm))
    elems.append(HRFlowable(width="100%", thickness=0.5, color=C_MUTED))
    elems.append(Spacer(1, 2*mm))
    elems.append(Paragraph(
        f"CarbonSight Sustainability Report  ·  Generated {generated_at}  ·  "
        "Powered by Google Cloud BigQuery  ·  carbonsight.app",
        styles["footer"]
    ))

    doc.build(elems)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=carbonsight_report.pdf"}
    )


# ═══════════════════════════════════════════
#  SIMULATION ENGINE
# ═══════════════════════════════════════════

class SimulationInput(BaseModel):
    monthly_units:           float = 10000
    energy_kwh_per_unit:     float = 2.5
    transport_km_per_unit:   float = 150.0
    electricity_units:       float = 5000.0
    fuel_units:              float = 1000.0
    courier_units:           float = 800.0
    energy_reduction_pct:    float = 0.0
    transport_reduction_pct: float = 0.0
    renewable_energy_pct:    float = 0.0
    ev_fleet_pct:            float = 0.0
    packaging_reduction_pct: float = 0.0
    units_growth_pct:        float = 0.0


@app.post("/simulate")
def simulate(payload: SimulationInput):
    af      = get_active_factors()
    GRID    = af["grid_electricity"]
    RENEW   = af["renewable_energy"]
    TRUCK   = af["freight_truck"]
    EV      = af["ev_transport"]
    FUEL    = af["fuel"]
    COURIER = af["courier"]

    b_energy_co2      = payload.monthly_units    * payload.energy_kwh_per_unit  * GRID
    b_transport_co2   = payload.monthly_units    * payload.transport_km_per_unit * TRUCK
    b_electricity_co2 = payload.electricity_units * GRID
    b_fuel_co2        = payload.fuel_units        * FUEL
    b_courier_co2     = payload.courier_units     * COURIER
    b_product = b_energy_co2 + b_transport_co2
    b_utility = b_electricity_co2 + b_fuel_co2 + b_courier_co2
    b_total   = b_product + b_utility

    sim_units    = payload.monthly_units * (1 + payload.units_growth_pct / 100)
    sim_kwh_unit = (payload.energy_kwh_per_unit
                    * (1 - payload.energy_reduction_pct    / 100)
                    * (1 - payload.packaging_reduction_pct / 100 * 0.05))
    blend_e      = (payload.renewable_energy_pct / 100) * RENEW + (1 - payload.renewable_energy_pct / 100) * GRID
    s_energy_co2 = sim_units * sim_kwh_unit * blend_e

    sim_km_unit     = payload.transport_km_per_unit * (1 - payload.transport_reduction_pct / 100)
    blend_t         = (payload.ev_fleet_pct / 100) * EV + (1 - payload.ev_fleet_pct / 100) * TRUCK
    s_transport_co2 = sim_units * sim_km_unit * blend_t

    blend_elec        = (payload.renewable_energy_pct / 100) * RENEW + (1 - payload.renewable_energy_pct / 100) * GRID
    s_electricity_co2 = payload.electricity_units * blend_elec
    s_fuel_co2        = payload.fuel_units    * FUEL
    s_courier_co2     = payload.courier_units * COURIER
    s_product = s_energy_co2 + s_transport_co2
    s_utility = s_electricity_co2 + s_fuel_co2 + s_courier_co2
    s_total   = s_product + s_utility

    saved        = b_total - s_total
    red_pct      = (saved / b_total * 100) if b_total > 0 else 0
    trees_needed = round(s_total * 12 / 21)
    kwh_saved    = payload.monthly_units * payload.energy_kwh_per_unit - sim_units * sim_kwh_unit
    cost_inr     = kwh_saved * 7.0

    b_scope1 = b_fuel_co2;        s_scope1 = s_fuel_co2
    b_scope2 = b_energy_co2 + b_electricity_co2; s_scope2 = s_energy_co2 + s_electricity_co2
    b_scope3 = b_transport_co2 + b_courier_co2;  s_scope3 = s_transport_co2 + s_courier_co2

    return {
        "baseline": {
            "product_co2_kg": round(b_product, 2), "utility_co2_kg": round(b_utility, 2), "total_co2_kg": round(b_total, 2),
            "breakdown": {"energy_co2_kg": round(b_energy_co2,2), "transport_co2_kg": round(b_transport_co2,2),
                          "electricity_co2_kg": round(b_electricity_co2,2), "fuel_co2_kg": round(b_fuel_co2,2), "courier_co2_kg": round(b_courier_co2,2)},
            "ghg_scopes": {"scope1_kg": round(b_scope1,2), "scope2_kg": round(b_scope2,2), "scope3_kg": round(b_scope3,2)}
        },
        "simulated": {
            "product_co2_kg": round(s_product, 2), "utility_co2_kg": round(s_utility, 2), "total_co2_kg": round(s_total, 2),
            "breakdown": {"energy_co2_kg": round(s_energy_co2,2), "transport_co2_kg": round(s_transport_co2,2),
                          "electricity_co2_kg": round(s_electricity_co2,2), "fuel_co2_kg": round(s_fuel_co2,2), "courier_co2_kg": round(s_courier_co2,2)},
            "ghg_scopes": {"scope1_kg": round(s_scope1,2), "scope2_kg": round(s_scope2,2), "scope3_kg": round(s_scope3,2)}
        },
        "impact": {
            "co2_saved_kg": round(saved,2), "co2_saved_tonnes": round(saved/1000,3), "reduction_pct": round(red_pct,2),
            "annual_co2_saved_kg": round(saved*12,2), "trees_to_offset_remaining": trees_needed,
            "estimated_cost_savings_inr": round(cost_inr,2), "units_simulated": round(sim_units,0),
            "scope_savings": {"scope1_saved_kg": round(b_scope1-s_scope1,2), "scope2_saved_kg": round(b_scope2-s_scope2,2), "scope3_saved_kg": round(b_scope3-s_scope3,2)}
        },
        "levers_applied": {
            "energy_reduction_pct": payload.energy_reduction_pct, "transport_reduction_pct": payload.transport_reduction_pct,
            "renewable_energy_pct": payload.renewable_energy_pct, "ev_fleet_pct": payload.ev_fleet_pct,
            "packaging_reduction_pct": payload.packaging_reduction_pct, "units_growth_pct": payload.units_growth_pct
        }
    }


@app.get("/simulate/prefill")
def simulate_prefill():
    try:
        bq = bigquery.Client()
        r  = list(bq.query("""
            SELECT AVG(units_sold) AS avg_units,
                   SAFE_DIVIDE(SUM(energy_kwh),   SUM(units_sold)) AS energy_per_unit,
                   SAFE_DIVIDE(SUM(transport_km), SUM(units_sold)) AS km_per_unit
            FROM sustainability_ds.operations
            WHERE record_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        """).result())
        r = r[0] if r else None
        return {
            "monthly_units":         float(r.avg_units       or 10000) if r else 10000,
            "energy_kwh_per_unit":   float(r.energy_per_unit or 2.5)   if r else 2.5,
            "transport_km_per_unit": float(r.km_per_unit     or 150.0) if r else 150.0,
            "electricity_units": 5000.0, "fuel_units": 1000.0, "courier_units": 800.0,
        }
    except Exception:
        return {"monthly_units": 10000, "energy_kwh_per_unit": 2.5, "transport_km_per_unit": 150.0,
                "electricity_units": 5000.0, "fuel_units": 1000.0, "courier_units": 800.0}
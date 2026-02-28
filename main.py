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
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.colors import grey, HexColor
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
# EXPORT PDF
# ─────────────────────────────────────────

@app.post("/export/pdf")
async def export_pdf(request: Request):
    data = await request.json()

    def decode_image(b64):
        if not b64:
            return None
        _, enc = b64.split(",", 1)
        return io.BytesIO(base64.b64decode(enc))

    af               = get_active_factors()
    energy_factor    = af["grid_electricity"]
    transport_factor = af["freight_truck"]
    bq               = bigquery.Client()
    rows             = list(bq.query(f"""
        SELECT IFNULL(p.product_name, o.product_id) AS product,
               IFNULL(p.category, 'Unknown')         AS category,
               SUM(o.units_sold)                                                  AS units,
               ROUND(SUM(o.energy_kwh   * {energy_factor}),    2)                AS energy_co2,
               ROUND(SUM(o.transport_km * {transport_factor}), 2)                AS transport_co2,
               ROUND(SUM(o.energy_kwh   * {energy_factor}
                       + o.transport_km * {transport_factor}), 2)                AS total_co2
        FROM sustainability_ds.operations o
        LEFT JOIN sustainability_ds.products p ON o.product_id = p.product_id
        GROUP BY product, category ORDER BY total_co2 DESC
    """).result())

    buffer = io.BytesIO()
    doc    = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    elems  = []
    elems.append(Paragraph("CarbonSight — Sustainability Emissions Report", styles["Title"]))
    elems.append(Spacer(1, 15))
    elems.append(Paragraph(f"Generated: {datetime.now().strftime('%d %B %Y, %H:%M')}", styles["Normal"]))
    elems.append(Spacer(1, 20))
    for title, buf in [("Emission Trends", decode_image(data.get("trend"))),
                       ("Utility Emissions", decode_image(data.get("bill"))),
                       ("Total Carbon Footprint", decode_image(data.get("total")))]:
        elems.append(Paragraph(title, styles["Heading2"]))
        elems.append(Spacer(1, 10))
        if buf:
            elems.append(Image(buf, width=450, height=250))
        elems.append(Spacer(1, 20))
    elems.append(PageBreak())
    table_data = [["Product", "Category", "Units", "Energy CO₂", "Transport CO₂", "Total CO₂"]]
    for r in rows:
        table_data.append([r.product, r.category, str(int(r.units or 0)),
                           f"{(r.energy_co2 or 0):.2f}", f"{(r.transport_co2 or 0):.2f}", f"{(r.total_co2 or 0):.2f}"])
    t = Table(table_data, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",     (0,0), (-1,0),  HexColor("#1e293b")),
        ("TEXTCOLOR",      (0,0), (-1,0),  HexColor("#ffffff")),
        ("GRID",           (0,0), (-1,-1), 0.5, grey),
        ("FONT",           (0,0), (-1,0),  "Helvetica-Bold"),
        ("ALIGN",          (2,1), (-1,-1), "CENTER"),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [HexColor("#f8fafc"), HexColor("#ffffff")]),
    ]))
    elems.append(t)
    elems.append(Spacer(1, 20))
    elems.append(Paragraph("Generated by CarbonSight — Powered by Google Cloud", styles["Italic"]))
    doc.build(elems)
    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=carbonsight_report.pdf"})


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
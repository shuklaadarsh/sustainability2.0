from fastapi import FastAPI, UploadFile, File, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from google.cloud import storage, bigquery
from typing import Optional, List
import io
import pandas as pd
from fastapi.responses import StreamingResponse
import uuid
import os
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.colors import grey, black, lightgrey, HexColor
from reportlab.lib.units import inch
from datetime import datetime
import base64
from pydantic import BaseModel


app = FastAPI(title="CarbonSight API", version="1.0.0")

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
DATASET = "sustainability_ds"
TABLE = "operations"
REGION = os.environ.get("REGION", "India")

# Mount static safely
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# ─────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────

@app.get("/")
def home():
    return FileResponse("static/index.html")


# ─────────────────────────────────────────
# UPLOAD CSV
# ─────────────────────────────────────────

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    filename = f"{uuid.uuid4()}_{file.filename}"
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_file(file.file)

        bq_client = bigquery.Client()
        table_id = f"{DATASET}.{TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("units_sold", "INTEGER"),
                bigquery.SchemaField("energy_kwh", "FLOAT"),
                bigquery.SchemaField("transport_km", "FLOAT"),
                bigquery.SchemaField("record_date", "DATE"),
            ],
            allow_quoted_newlines=True,
            ignore_unknown_values=True
        )

        uri = f"gs://{BUCKET_NAME}/{filename}"
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        rows_loaded = load_job.output_rows

        bq_client.query(f"""
        INSERT INTO sustainability_ds.upload_log
        (upload_id, upload_time, file_name, rows_loaded, status)
        VALUES
        ('{filename}', CURRENT_TIMESTAMP(), '{file.filename}', {rows_loaded}, 'SUCCESS')
        """).result()

        return {"message": "Upload successful", "rows": rows_loaded}

    except Exception as e:
        return {"error": "Upload failed", "details": str(e)}


# ─────────────────────────────────────────
# UPLOAD UTILITY BILL
# ─────────────────────────────────────────

@app.post("/upload-bill")
async def upload_bill(
    bill_type: str,
    amount: float,
    units: float,
    region: str,
    month: str
):
    bq_client = bigquery.Client()
    bill_id = str(uuid.uuid4())

    if not month or len(month) != 7 or month[4] != "-":
        return {"error": "Invalid month format. Use YYYY-MM"}

    FACTORS = {
        "electricity": 0.82,
        "fuel": 2.68,
        "courier": 0.18
    }

    factor = FACTORS.get(bill_type, 0.0)
    estimated_co2 = units * factor

    bq_client.query(f"""
    INSERT INTO sustainability_ds.utility_bills
    (bill_id, bill_type, amount, units, region, month, upload_time)
    VALUES
    (
      '{bill_id}', '{bill_type}', {amount}, {units}, '{region}',
      PARSE_DATE('%Y-%m-%d', '{month}-01'),
      CURRENT_TIMESTAMP()
    )
    """).result()

    return {
        "status": "success",
        "bill_id": bill_id,
        "estimated_co2": estimated_co2
    }


# ─────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────

@app.get("/metrics")
def get_metrics(since: Optional[str] = Query(None)):
    energy_factor, energy_ref = get_emission_factor(REGION, "electricity")
    transport_factor, transport_ref = get_emission_factor("Global", "freight_truck")

    if not energy_factor:
        energy_factor = 0.82
        energy_ref = "Default"
    if not transport_factor:
        transport_factor = 0.0525
        transport_ref = "Default"

    bq = bigquery.Client()
    where = f"WHERE o.record_date >= DATE('{since}')" if since else ""

    query = f"""
    SELECT
      o.product_id,
      IFNULL(p.product_name, o.product_id) AS product_name,
      IFNULL(p.category, 'Unknown') AS category,
      SUM(o.units_sold) AS units,
      SUM(o.energy_kwh) AS energy,
      SUM(o.transport_km) AS km
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue p ON o.product_id = p.product_id
    {where}
    GROUP BY o.product_id, product_name, category
    ORDER BY product_name
    """

    rows = bq.query(query).result()
    data = []

    for r in rows:
        units = r.units or 0
        energy = r.energy or 0
        km = r.km or 0
        energy_co2 = energy * energy_factor
        transport_co2 = km * transport_factor

        data.append({
            "product_id": r.product_id,
            "product_name": r.product_name,
            "category": r.category,
            "total_units_sold": int(units),
            "energy_co2_kg": float(energy_co2),
            "transport_co2_kg": float(transport_co2),
            "total_co2_kg": float(energy_co2 + transport_co2),
            "energy_ref": energy_ref,
            "transport_ref": transport_ref
        })

    return {"count": len(data), "data": data}


# ─────────────────────────────────────────
# UPLOAD HISTORY
# ─────────────────────────────────────────

@app.get("/uploads")
def get_upload_history():
    bq_client = bigquery.Client()
    query = """
    SELECT upload_id, upload_time, file_name, rows_loaded, status
    FROM sustainability_ds.upload_log
    WHERE status != 'DELETED'
    ORDER BY upload_time DESC
    LIMIT 20
    """
    results = bq_client.query(query).result()
    data = [
        {
            "upload_id": row.upload_id,
            "upload_time": str(row.upload_time),
            "file_name": row.file_name,
            "rows_loaded": row.rows_loaded,
            "status": row.status
        }
        for row in results
    ]
    return {"count": len(data), "data": data}


@app.delete("/uploads/{upload_id}")
def delete_upload(upload_id: str):
    bq_client = bigquery.Client()
    bq_client.query(f"""
    UPDATE sustainability_ds.upload_log SET status = 'DELETED'
    WHERE upload_id = '{upload_id}'
    """).result()
    return {"status": "deleted", "upload_id": upload_id}


# ─────────────────────────────────────────
# EMISSION FACTORS
# ─────────────────────────────────────────

@app.get("/factors")
def list_factors():
    bq = bigquery.Client()
    query = """
    SELECT region, activity_type, factor, year, reference, created_at
    FROM sustainability_ds.emission_factors
    ORDER BY activity_type, year DESC
    """
    results = bq.query(query).result()
    data = [
        {
            "region": row.region,
            "activity_type": row.activity_type,
            "factor": float(row.factor),
            "year": int(row.year),
            "reference": row.reference,
            "created_at": str(row.created_at)
        }
        for row in results
    ]
    return {"data": data}


# ─────────────────────────────────────────
# TRENDS
# ─────────────────────────────────────────

@app.get("/trends")
def get_trends():
    bq_client = bigquery.Client()
    query = """
    SELECT
      FORMAT_DATE('%Y-%m', record_date) AS month,
      SUM((energy_kwh * 0.82) + (transport_km * 0.0525)) AS total_co2,
      SUM(units_sold) AS total_units
    FROM sustainability_ds.operations
    GROUP BY month ORDER BY month
    """
    results = bq_client.query(query).result()
    data = []
    prev_cpu = None

    for row in results:
        total = float(row.total_co2 or 0)
        units = float(row.total_units or 0)
        cpu = total / units if units > 0 else 0
        trend = round(((cpu - prev_cpu) / prev_cpu) * 100, 2) if prev_cpu else None
        prev_cpu = cpu
        data.append({
            "month": row.month,
            "co2_per_unit": round(cpu, 4),
            "efficiency_change": trend
        })

    return {"data": data}


# ─────────────────────────────────────────
# BILL INSIGHTS
# ─────────────────────────────────────────

@app.get("/bill-insights")
def get_bill_insights():
    bq_client = bigquery.Client()
    query = """
    SELECT FORMAT_DATE('%Y-%m', month) AS month, region, bill_type, estimated_co2
    FROM sustainability_ds.bill_emissions ORDER BY month
    """
    results = bq_client.query(query).result()
    data = [
        {
            "month": row.month,
            "region": row.region,
            "bill_type": row.bill_type,
            "estimated_co2": float(row.estimated_co2)
        }
        for row in results
    ]
    return {"data": data}


# ─────────────────────────────────────────
# COMPANY KPIs
# ─────────────────────────────────────────

@app.get("/company-kpis")
def get_company_kpis():
    bq_client = bigquery.Client()
    result = list(bq_client.query("""
    SELECT SUM(co2) AS total_co2 FROM sustainability_ds.company_emissions
    """).result())
    total = float(result[0].total_co2) if result and result[0].total_co2 else 0
    return {"total_company_co2": total}


# ─────────────────────────────────────────
# TOTAL FOOTPRINT
# ─────────────────────────────────────────

@app.get("/total-footprint")
def total_footprint():
    bq = bigquery.Client()
    query = """
    WITH product AS (
      SELECT FORMAT_DATE('%Y-%m', record_date) AS month,
             SUM(energy_kwh * 0.82 + transport_km * 0.0525) AS co2
      FROM sustainability_ds.operations GROUP BY month
    ),
    utility AS (
      SELECT FORMAT_DATE('%Y-%m', month) AS month,
             SUM(estimated_co2) AS co2
      FROM sustainability_ds.bill_emissions GROUP BY month
    )
    SELECT
      COALESCE(p.month, u.month) AS month,
      IFNULL(p.co2, 0) AS product_co2,
      IFNULL(u.co2, 0) AS utility_co2,
      IFNULL(p.co2, 0) + IFNULL(u.co2, 0) AS total_co2
    FROM product p
    FULL OUTER JOIN utility u ON p.month = u.month
    ORDER BY month
    """
    rows = bq.query(query).result()
    data = [
        {
            "month": r.month,
            "product_co2": float(r.product_co2),
            "utility_co2": float(r.utility_co2),
            "total_co2": float(r.total_co2)
        }
        for r in rows
    ]
    return {"data": data}


# ─────────────────────────────────────────
# RESET ALL
# ─────────────────────────────────────────

@app.delete("/reset-all")
def reset_all_data():
    bq_client = bigquery.Client()
    bq_client.query("DELETE FROM sustainability_ds.operations WHERE TRUE").result()
    bq_client.query("DELETE FROM sustainability_ds.utility_bills WHERE TRUE").result()
    bq_client.query("DELETE FROM sustainability_ds.upload_log WHERE TRUE").result()
    return {"status": "all_data_cleared"}


# ─────────────────────────────────────────
# HELPER: EMISSION FACTOR LOOKUP
# ─────────────────────────────────────────

def get_emission_factor(region, activity):
    bq = bigquery.Client()
    query = f"""
    SELECT factor, reference
    FROM sustainability_ds.emission_factors
    WHERE region = '{region}'
      AND activity_type = '{activity}'
    ORDER BY year DESC LIMIT 1
    """
    rows = list(bq.query(query).result())
    if not rows:
        return None, None
    return rows[0].factor, rows[0].reference


# ─────────────────────────────────────────
# EXPORT EXCEL
# ─────────────────────────────────────────

@app.get("/export/excel")
def export_excel():
    bq = bigquery.Client()
    metrics_query = """
    SELECT
      o.product_id,
      IFNULL(p.product_name, o.product_id) AS product_name,
      IFNULL(p.category, 'Unknown') AS category,
      SUM(o.units_sold) AS units_sold,
      SUM(o.energy_kwh * 0.82) AS energy_co2,
      SUM(o.transport_km * 0.0525) AS transport_co2,
      SUM(o.energy_kwh * 0.82 + o.transport_km * 0.0525) AS total_co2
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue p ON o.product_id = p.product_id
    GROUP BY o.product_id, product_name, category
    """
    rows = bq.query(metrics_query).to_dataframe()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        rows.to_excel(writer, index=False, sheet_name="Product_Emissions")
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=carbon_report.xlsx"}
    )


# ─────────────────────────────────────────
# EXPORT PDF
# ─────────────────────────────────────────

@app.post("/export/pdf")
async def export_pdf(request: Request):
    data = await request.json()

    def decode_image(img_base64):
        if not img_base64:
            return None
        header, encoded = img_base64.split(",", 1)
        return io.BytesIO(base64.b64decode(encoded))

    trend_buffer = decode_image(data.get("trend"))
    bill_buffer  = decode_image(data.get("bill"))
    total_buffer = decode_image(data.get("total"))

    bq = bigquery.Client()
    query = """
    SELECT
      IFNULL(p.product_name, o.product_id) AS product,
      IFNULL(p.category, 'Unknown') AS category,
      SUM(o.units_sold) AS units,
      SUM(o.energy_kwh * 0.82) AS energy_co2,
      SUM(o.transport_km * 0.0525) AS transport_co2,
      SUM(o.energy_kwh * 0.82 + o.transport_km * 0.0525) AS total_co2
    FROM sustainability_ds.operations o
    LEFT JOIN sustainability_ds.product_catalogue p ON o.product_id = p.product_id
    GROUP BY product, category ORDER BY total_co2 DESC
    """
    rows = list(bq.query(query).result())

    buffer = io.BytesIO()
    doc    = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph("CarbonSight — Sustainability Emissions Report", styles["Title"]))
    elements.append(Spacer(1, 15))
    elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%d %B %Y, %H:%M')}", styles["Normal"]))
    elements.append(Spacer(1, 20))

    for title, buf in [
        ("Emission Trends",       trend_buffer),
        ("Utility Emissions",     bill_buffer),
        ("Total Carbon Footprint", total_buffer)
    ]:
        elements.append(Paragraph(title, styles["Heading2"]))
        elements.append(Spacer(1, 10))
        if buf:
            elements.append(Image(buf, width=450, height=250))
        elements.append(Spacer(1, 20))

    elements.append(PageBreak())

    table_data = [["Product", "Category", "Units", "Energy CO₂", "Transport CO₂", "Total CO₂"]]
    for r in rows:
        table_data.append([
            r.product, r.category,
            str(int(r.units or 0)),
            f"{(r.energy_co2 or 0):.2f}",
            f"{(r.transport_co2 or 0):.2f}",
            f"{(r.total_co2 or 0):.2f}",
        ])

    table = Table(table_data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  HexColor("#1e293b")),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  HexColor("#ffffff")),
        ("GRID",          (0, 0), (-1, -1), 0.5, grey),
        ("FONT",          (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("ALIGN",         (2, 1), (-1, -1), "CENTER"),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [HexColor("#f8fafc"), HexColor("#ffffff")]),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 20))
    elements.append(Paragraph("Generated by CarbonSight — Powered by Google Cloud", styles["Italic"]))

    doc.build(elements)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=carbonsight_report.pdf"}
    )


# ═══════════════════════════════════════════
#  SIMULATION ENGINE  ← NEW
# ═══════════════════════════════════════════

class SimulationInput(BaseModel):
    # ── Baseline operational inputs ───────────────────────────
    monthly_units:           float = 10000   # total units shipped per month
    energy_kwh_per_unit:     float = 2.5     # kWh consumed per unit produced/packed
    transport_km_per_unit:   float = 150.0   # avg km per shipment
    electricity_units:       float = 5000.0  # monthly electricity kWh (facility)
    fuel_units:              float = 1000.0  # monthly diesel/petrol litres
    courier_units:           float = 800.0   # monthly courier weight kg

    # ── Green levers (0–100 %) ────────────────────────────────
    energy_reduction_pct:    float = 0.0   # operational energy efficiency gain
    transport_reduction_pct: float = 0.0   # route optimisation / consolidation
    renewable_energy_pct:    float = 0.0   # % electricity from solar/wind
    ev_fleet_pct:            float = 0.0   # % delivery fleet switched to EV
    packaging_reduction_pct: float = 0.0   # packaging material reduction
    units_growth_pct:        float = 0.0   # planned business growth


@app.post("/simulate")
def simulate(payload: SimulationInput):
    """
    100 % in-memory simulation — nothing written to BigQuery.
    Returns baseline vs projected CO2 with full breakdown and impact metrics.
    """

    # ── Published emission factors ───────────────────────────
    GRID_FACTOR      = 0.82    # kg CO2 / kWh  (India grid average)
    RENEWABLE_FACTOR = 0.05    # kg CO2 / kWh  (solar/wind lifecycle)
    TRUCK_FACTOR     = 0.0525  # kg CO2 / km   (diesel HGV)
    EV_FACTOR        = 0.021   # kg CO2 / km   (BEV on India grid)
    FUEL_FACTOR      = 2.68    # kg CO2 / litre diesel
    COURIER_FACTOR   = 0.18    # kg CO2 / kg   parcel courier

    # ── BASELINE ─────────────────────────────────────────────
    b_energy_co2      = payload.monthly_units * payload.energy_kwh_per_unit * GRID_FACTOR
    b_transport_co2   = payload.monthly_units * payload.transport_km_per_unit * TRUCK_FACTOR
    b_electricity_co2 = payload.electricity_units * GRID_FACTOR
    b_fuel_co2        = payload.fuel_units * FUEL_FACTOR
    b_courier_co2     = payload.courier_units * COURIER_FACTOR
    b_product         = b_energy_co2 + b_transport_co2
    b_utility         = b_electricity_co2 + b_fuel_co2 + b_courier_co2
    b_total           = b_product + b_utility

    # ── APPLY LEVERS ─────────────────────────────────────────

    # Volume after growth
    sim_units = payload.monthly_units * (1 + payload.units_growth_pct / 100)

    # Energy: reduce kWh/unit + packaging effect (5 % proxy) + renewable blend
    sim_kwh_unit = (
        payload.energy_kwh_per_unit
        * (1 - payload.energy_reduction_pct / 100)
        * (1 - payload.packaging_reduction_pct / 100 * 0.05)
    )
    blended_energy_factor = (
        (payload.renewable_energy_pct / 100) * RENEWABLE_FACTOR +
        (1 - payload.renewable_energy_pct / 100) * GRID_FACTOR
    )
    s_energy_co2 = sim_units * sim_kwh_unit * blended_energy_factor

    # Transport: shorter routes + EV blend
    sim_km_unit = payload.transport_km_per_unit * (1 - payload.transport_reduction_pct / 100)
    blended_transport_factor = (
        (payload.ev_fleet_pct / 100) * EV_FACTOR +
        (1 - payload.ev_fleet_pct / 100) * TRUCK_FACTOR
    )
    s_transport_co2 = sim_units * sim_km_unit * blended_transport_factor

    # Utility: grid electricity gets renewable benefit; fuel/courier unchanged
    sim_electricity_factor = (
        (payload.renewable_energy_pct / 100) * RENEWABLE_FACTOR +
        (1 - payload.renewable_energy_pct / 100) * GRID_FACTOR
    )
    s_electricity_co2 = payload.electricity_units * sim_electricity_factor
    s_fuel_co2        = payload.fuel_units * FUEL_FACTOR
    s_courier_co2     = payload.courier_units * COURIER_FACTOR

    s_product = s_energy_co2 + s_transport_co2
    s_utility = s_electricity_co2 + s_fuel_co2 + s_courier_co2
    s_total   = s_product + s_utility

    # ── IMPACT METRICS ───────────────────────────────────────
    saved       = b_total - s_total
    red_pct     = (saved / b_total * 100) if b_total > 0 else 0
    # 1 mature tree absorbs ~21 kg CO2/yr
    trees_needed = round(s_total * 12 / 21)
    # Simple cost proxy: kWh saved × India avg tariff ₹7
    kwh_saved    = (
        payload.monthly_units * payload.energy_kwh_per_unit -
        sim_units * sim_kwh_unit
    )
    cost_inr     = kwh_saved * 7.0

    return {
        "baseline": {
            "product_co2_kg":  round(b_product, 2),
            "utility_co2_kg":  round(b_utility, 2),
            "total_co2_kg":    round(b_total, 2),
            "breakdown": {
                "energy_co2_kg":      round(b_energy_co2, 2),
                "transport_co2_kg":   round(b_transport_co2, 2),
                "electricity_co2_kg": round(b_electricity_co2, 2),
                "fuel_co2_kg":        round(b_fuel_co2, 2),
                "courier_co2_kg":     round(b_courier_co2, 2),
            }
        },
        "simulated": {
            "product_co2_kg":  round(s_product, 2),
            "utility_co2_kg":  round(s_utility, 2),
            "total_co2_kg":    round(s_total, 2),
            "breakdown": {
                "energy_co2_kg":      round(s_energy_co2, 2),
                "transport_co2_kg":   round(s_transport_co2, 2),
                "electricity_co2_kg": round(s_electricity_co2, 2),
                "fuel_co2_kg":        round(s_fuel_co2, 2),
                "courier_co2_kg":     round(s_courier_co2, 2),
            }
        },
        "impact": {
            "co2_saved_kg":              round(saved, 2),
            "co2_saved_tonnes":          round(saved / 1000, 3),
            "reduction_pct":             round(red_pct, 2),
            "annual_co2_saved_kg":       round(saved * 12, 2),
            "trees_to_offset_remaining": trees_needed,
            "estimated_cost_savings_inr": round(cost_inr, 2),
            "units_simulated":           round(sim_units, 0)
        },
        "levers_applied": {
            "energy_reduction_pct":    payload.energy_reduction_pct,
            "transport_reduction_pct": payload.transport_reduction_pct,
            "renewable_energy_pct":    payload.renewable_energy_pct,
            "ev_fleet_pct":            payload.ev_fleet_pct,
            "packaging_reduction_pct": payload.packaging_reduction_pct,
            "units_growth_pct":        payload.units_growth_pct
        }
    }


# ─────────────────────────────────────────
# SIMULATION PREFILL — pull live averages
# ─────────────────────────────────────────

@app.get("/simulate/prefill")
def simulate_prefill():
    """
    Pull last-90-day averages from BigQuery to pre-populate the simulator.
    Returns safe defaults if no data exists.
    """
    try:
        bq = bigquery.Client()
        result = list(bq.query("""
        SELECT
          AVG(units_sold)                                  AS avg_units,
          SAFE_DIVIDE(SUM(energy_kwh),    SUM(units_sold)) AS energy_per_unit,
          SAFE_DIVIDE(SUM(transport_km),  SUM(units_sold)) AS km_per_unit
        FROM sustainability_ds.operations
        WHERE record_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        """).result())

        r = result[0] if result else None

        return {
            "monthly_units":         float(r.avg_units       or 10000) if r else 10000,
            "energy_kwh_per_unit":   float(r.energy_per_unit or 2.5)   if r else 2.5,
            "transport_km_per_unit": float(r.km_per_unit     or 150.0) if r else 150.0,
            "electricity_units":     5000.0,
            "fuel_units":            1000.0,
            "courier_units":         800.0,
        }
    except Exception:
        return {
            "monthly_units":         10000,
            "energy_kwh_per_unit":   2.5,
            "transport_km_per_unit": 150.0,
            "electricity_units":     5000.0,
            "fuel_units":            1000.0,
            "courier_units":         800.0,
        }
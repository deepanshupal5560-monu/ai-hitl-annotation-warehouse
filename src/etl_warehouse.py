from datetime import datetime, timezone
import json
import logging
import os
import sqlite3
import pandas as pd
import requests


class AnnotationWarehouseETL:

  def __init__(self, config_path="config.json"):
    with open(config_path, "r", encoding="utf-8") as f:
      self.config = json.load(f)

    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        filename=self.config["log_file_path"],
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    self.db_path = self.config["database_path"]
    self.thresholds = self.config["quality_thresholds"]
    self.taxonomy = set(self.config["approved_taxonomy"])
    self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()

  def initialize_star_schema(self):
    """Creates Star Schema Dimension, Fact, DLQ, and Watermark tables."""
    conn = sqlite3.connect(self.db_path)
    cur = conn.cursor()

    cur.executescript("""
            CREATE TABLE IF NOT EXISTS dim_annotators (
                annotator_id TEXT PRIMARY KEY,
                annotator_name TEXT NOT NULL,
                experience_tier TEXT NOT NULL,
                region TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_ai_models (
                model_id TEXT PRIMARY KEY,
                model_version TEXT NOT NULL,
                domain TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fact_annotation_events (
                event_id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                annotator_id TEXT NOT NULL,
                model_id TEXT NOT NULL,
                final_label TEXT NOT NULL,
                transcript_snippet TEXT NOT NULL,
                duration_sec REAL NOT NULL,
                human_confidence REAL NOT NULL,
                ai_predicted_label TEXT NOT NULL,
                ai_agreement_score REAL NOT NULL,
                audit_flag TEXT NOT NULL,
                ingested_at_utc TEXT NOT NULL,
                FOREIGN KEY (annotator_id) REFERENCES dim_annotators(annotator_id),
                FOREIGN KEY (model_id) REFERENCES dim_ai_models(model_id)
            );

            CREATE TABLE IF NOT EXISTS dlq_rejected_records (
                dlq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                batch_id TEXT,
                rejection_stage TEXT NOT NULL,
                failure_reason TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                quarantined_at_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS etl_watermarks (
                batch_id TEXT PRIMARY KEY,
                processed_at_utc TEXT NOT NULL,
                valid_count INTEGER NOT NULL,
                dlq_count INTEGER NOT NULL
            );
        """)

    annotators = [
        ("ANN-01", "Deepanshu Pal", "Senior QA", "India-North"),
        ("ANN-02", "Riya Sharma", "Mid-Level", "India-North"),
        ("ANN-03", "Kabir Verma", "Junior", "India-West"),
        ("ANN-04", "Ananya Nair", "Senior QA", "India-South"),
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO dim_annotators VALUES (?, ?, ?, ?);",
        annotators,
    )

    models = [
        ("MOD-SENSI-V2", "SensiAudio-Classifier-v2.4", "Acoustic Event & Safety"),
        ("MOD-RLHF-V3", "Hinglish-ASR-Evaluator-v3.1", "Speech & Intent"),
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO dim_ai_models VALUES (?, ?, ?);",
        models,
    )

    conn.commit()
    conn.close()
    logging.info("Star Schema Warehouse initialized successfully.")

  def simulate_raw_ingestion_batch(self, batch_id="BATCH-2026-11-25-01"):
    """Simulates multi-source audio/transcript annotation stream."""
    raw_events = [
        {
            "event_id": "EVT-9001",
            "batch_id": batch_id,
            "annotator_id": "ANN-01",
            "model_id": "MOD-SENSI-V2",
            "human_label": "alert_cough",
            "transcript": "Patient dry cough detected twice near bedside mic",
            "duration_sec": 3.4,
            "human_confidence": 0.96,
        },
        {
            "event_id": "EVT-9002",
            "batch_id": batch_id,
            "annotator_id": "ANN-02",
            "model_id": "MOD-RLHF-V3",
            "human_label": "speech_clear",
            "transcript": "Please schedule the follow up call for tomorrow morning",
            "duration_sec": 5.2,
            "human_confidence": 0.93,
        },
        {
            "event_id": "EVT-9003",
            "batch_id": batch_id,
            "annotator_id": "ANN-04",
            "model_id": "MOD-SENSI-V2",
            "human_label": "alert_fall",
            "transcript": "Sudden heavy thud impact followed by distress call",
            "duration_sec": 4.1,
            "human_confidence": 0.95,
        },
        {
            "event_id": "EVT-9004",
            "batch_id": batch_id,
            "annotator_id": "ANN-03",
            "model_id": "MOD-SENSI-V2",
            "human_label": "silence",
            "transcript": "Loud heavy construction drilling and traffic horn",
            "duration_sec": 6.0,
            "human_confidence": 0.89,
        },
        {
            "event_id": "EVT-9001",
            "batch_id": batch_id,
            "annotator_id": "ANN-01",
            "model_id": "MOD-SENSI-V2",
            "human_label": "alert_cough",
            "transcript": "Patient dry cough detected twice near bedside mic",
            "duration_sec": 3.4,
            "human_confidence": 0.96,
        },
        {
            "event_id": "EVT-9005",
            "batch_id": batch_id,
            "annotator_id": "ANN-03",
            "model_id": "MOD-SENSI-V2",
            "human_label": "alert_fall",
            "transcript": "Unclear muffled audio",
            "duration_sec": 2.5,
            "human_confidence": 0.58,
        },
        {
            "event_id": "EVT-9006",
            "batch_id": batch_id,
            "annotator_id": "ANN-02",
            "model_id": "MOD-SENSI-V2",
            "human_label": "background_noise",
            "transcript": "Ceiling fan hum and distant television audio",
            "duration_sec": 8.5,
            "human_confidence": 0.91,
        },
        {
            "event_id": "EVT-9007",
            "batch_id": batch_id,
            "annotator_id": "ANN-99",
            "model_id": "MOD-RLHF-V3",
            "human_label": "speech_clear",
            "transcript": "Test audio check",
            "duration_sec": 2.0,
            "human_confidence": 0.90,
        },
        {
            "event_id": "EVT-9008",
            "batch_id": batch_id,
            "annotator_id": "ANN-01",
            "model_id": "MOD-RLHF-V3",
            "human_label": "speech_clear",
            "transcript": "Confirming order dispatch from Noida hub",
            "duration_sec": 4.8,
            "human_confidence": 0.97,
        },
    ]
    return raw_events

  def ai_quality_judge(self, transcript, human_label):
    """Uses Gemini API (if GEMINI_API_KEY is set) or semantic NLP fallback to audit label accuracy."""
    if self.gemini_api_key:
      url = (
          "https://generativelanguage.googleapis.com/v1beta/models/"
          f"{self.config['gemini_model']}:generateContent?key={self.gemini_api_key}"
      )
      prompt = (
          "Classify this audio event transcript into one label from "
          f"{list(self.taxonomy)}. Return ONLY JSON: "
          '{"ai_label": "<label>", "agreement_score": <0.0_to_1.0>}. '
          f"Human label: '{human_label}'. Transcript: '{transcript}'."
      )
      try:
        resp = requests.post(
            url,
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=8,
        )
        if resp.status_code == 200:
          text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
          cleaned = text.replace("```json", "").replace("```", "").strip()
          parsed = json.loads(cleaned)
          return (
              parsed.get("ai_label", human_label),
              float(parsed.get("agreement_score", 0.90)),
              "GEMINI_LLM_AUDITED",
          )
      except Exception as exc:
        logging.warning("Gemini API fallback triggered: %s", exc)

    text_lower = transcript.lower()
    if "cough" in text_lower:
      inferred = "alert_cough"
    elif "thud" in text_lower or "fall" in text_lower or "impact" in text_lower:
      inferred = "alert_fall"
    elif (
        "drilling" in text_lower
        or "fan" in text_lower
        or "noise" in text_lower
        or "horn" in text_lower
    ):
      inferred = "background_noise"
    elif "quiet" in text_lower or "no sound" in text_lower:
      inferred = "silence"
    else:
      inferred = "speech_clear"

    agreement = 0.95 if inferred == human_label else 0.35
    return inferred, agreement, "AI_VERIFIED"

  def run_pipeline(self, batch_id="BATCH-2026-11-25-01"):
    self.initialize_star_schema()
    conn = sqlite3.connect(self.db_path)
    cur = conn.cursor()

    cur.execute(
        "SELECT batch_id FROM etl_watermarks WHERE batch_id = ?;", (batch_id,)
    )
    if cur.fetchone():
      print(f"[SKIPPED] Batch {batch_id} already processed (Watermark hit).")
      conn.close()
      return

    cur.execute("SELECT annotator_id FROM dim_annotators;")
    valid_annotators = {r[0] for r in cur.fetchall()}

    raw_events = self.simulate_raw_ingestion_batch(batch_id)
    seen_ids = set()
    valid_rows = []
    dlq_rows = []
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for ev in raw_events:
      eid = ev.get("event_id")
      label = str(ev.get("human_label", "")).strip().lower()
      dur = float(ev.get("duration_sec", 0))
      conf = float(ev.get("human_confidence", 0))

      if eid in seen_ids:
        dlq_rows.append(
            (eid, batch_id, "SCHEMA_DEDUP", "Duplicate Event ID", json.dumps(ev), now_utc)
        )
        continue
      if ev.get("annotator_id") not in valid_annotators:
        dlq_rows.append((
            eid,
            batch_id,
            "FK_INTEGRITY",
            f"Unknown Annotator ID ({ev.get('annotator_id')})",
            json.dumps(ev),
            now_utc,
        ))
        continue
      if label not in self.taxonomy:
        dlq_rows.append((
            eid,
            batch_id,
            "TAXONOMY_CHECK",
            f"Invalid Label ({label})",
            json.dumps(ev),
            now_utc,
        ))
        continue
      if not (
          self.thresholds["min_audio_duration_sec"]
          <= dur
          <= self.thresholds["max_audio_duration_sec"]
      ):
        dlq_rows.append((
            eid,
            batch_id,
            "RANGE_CHECK",
            f"Duration Out of Bounds ({dur}s)",
            json.dumps(ev),
            now_utc,
        ))
        continue
      if conf < self.thresholds["min_confidence_score"]:
        dlq_rows.append((
            eid,
            batch_id,
            "CONFIDENCE_GATE",
            f"Low Human Confidence ({conf})",
            json.dumps(ev),
            now_utc,
        ))
        continue

      ai_label, agreement, audit_flag = self.ai_quality_judge(
          ev["transcript"], label
      )
      if (1.0 - agreement) > self.thresholds["max_ai_disagreement_delta"]:
        dlq_rows.append((
            eid,
            batch_id,
            "AI_DRIFT_AUDIT",
            f"Human ({label}) vs AI ({ai_label}) Disagreement",
            json.dumps(ev),
            now_utc,
        ))
        continue

      seen_ids.add(eid)
      valid_rows.append((
          eid,
          batch_id,
          ev["annotator_id"],
          ev["model_id"],
          label,
          ev["transcript"],
          dur,
          conf,
          ai_label,
          agreement,
          audit_flag,
          now_utc,
      ))

    cur.executemany(
        """
            INSERT INTO fact_annotation_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        valid_rows,
    )
    cur.executemany(
        """
            INSERT INTO dlq_rejected_records (
                event_id, batch_id, rejection_stage, failure_reason, raw_payload, quarantined_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?);
        """,
        dlq_rows,
    )
    cur.execute(
        "INSERT INTO etl_watermarks VALUES (?, ?, ?, ?);",
        (batch_id, now_utc, len(valid_rows), len(dlq_rows)),
    )
    conn.commit()

    dlq_df = pd.read_sql_query("SELECT * FROM dlq_rejected_records;", conn)
    dlq_df.to_csv(self.config["dlq_export_path"], index=False)

    print(
        f"[WAREHOUSE LOAD COMPLETE] Batch: {batch_id} | Fact Rows:"
        f" {len(valid_rows)} | DLQ Quarantined: {len(dlq_rows)}"
    )

    with open("sql/warehouse_analytics.sql", "r", encoding="utf-8") as sql_file:
      queries = [
          q.strip() for q in sql_file.read().split(";") if q.strip()
      ]

    for idx, query in enumerate(queries, 1):
      print(f"\n=== WAREHOUSE ANALYTICS REPORT {idx} ===")
      print(pd.read_sql_query(query, conn).to_string(index=False))

    conn.close()


if __name__ == "__main__":
  etl = AnnotationWarehouseETL()
  etl.run_pipeline()
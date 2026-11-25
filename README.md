# AI-Assisted Human-in-the-Loop (HITL) Data Engineering Warehouse

An enterprise-grade Python, SQL, and AI quality-assurance data engineering pipeline built for audio and transcript annotation workflows. It enforces Star Schema modeling, foreign-key integrity, AI-as-a-Judge semantic auditing (via Google Gemini API with deterministic fallback), Dead-Letter Queue (DLQ) quarantine routing, and idempotent batch watermarking.

## Architecture Overview
```text
[Raw Annotation Stream] 
        │
        ▼
[Stage 1: Schema, Dedup & FK Integrity Gate] ──(Invalid)──► [Dead-Letter Queue (DLQ)]
        │ (Valid)
        ▼
[Stage 2: AI Quality Auditor (Gemini API)] ────(Drift)────► [Human Re-Annotation CSV]
        │ (Verified)
        ▼
[Stage 3: Star Schema SQLite Warehouse] ──► [SQL CTE & Window Function Analytics]
-- QUERY 1: Annotator Precision Leaderboard using CTE and Window Function (RANK)
WITH annotator_metrics AS (
    SELECT 
        a.annotator_id,
        a.annotator_name,
        a.experience_tier,
        COUNT(f.event_id) AS total_approved_clips,
        ROUND(AVG(f.human_confidence), 3) AS avg_human_conf,
        ROUND(AVG(f.ai_agreement_score), 3) AS avg_ai_agreement,
        ROUND(SUM(f.duration_sec), 2) AS total_audio_sec
    FROM fact_annotation_events f
    INNER JOIN dim_annotators a ON f.annotator_id = a.annotator_id
    GROUP BY a.annotator_id, a.annotator_name, a.experience_tier
)
SELECT 
    RANK() OVER (ORDER BY avg_ai_agreement DESC, total_approved_clips DESC) AS quality_rank,
    annotator_id,
    annotator_name,
    experience_tier,
    total_approved_clips,
    avg_human_conf,
    avg_ai_agreement,
    total_audio_sec
FROM annotator_metrics;

-- QUERY 2: AI Model Drift & Taxonomy Distribution Report
SELECT 
    m.model_version,
    f.final_label,
    COUNT(f.event_id) AS verified_events,
    ROUND(AVG(f.ai_agreement_score), 3) AS mean_agreement,
    SUM(CASE WHEN f.audit_flag = 'AI_VERIFIED' THEN 1 ELSE 0 END) AS llm_audited_count
FROM fact_annotation_events f
INNER JOIN dim_ai_models m ON f.model_id = m.model_id
GROUP BY m.model_version, f.final_label
ORDER BY verified_events DESC;

-- QUERY 3: Dead-Letter Queue (DLQ) Root-Cause & Severity Breakdown
SELECT 
    rejection_stage,
    failure_reason,
    COUNT(*) AS quarantined_records
FROM dlq_rejected_records
GROUP BY rejection_stage, failure_reason
ORDER BY quarantined_records DESC;
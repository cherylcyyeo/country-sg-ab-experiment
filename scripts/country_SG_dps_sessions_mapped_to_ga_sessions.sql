WITH raw_sessions AS (
  SELECT
    events_ga_session_id,
    has_transaction,
    total_transactions,
    sessions.location,
    ga_location,
    sessions.variant,
    events.event_action,
    events.vendor_code,
    CASE 
      WHEN dps_zone.name IN ('Jurongwest', 'Bukitpanjang', 'Woodlands', 'Yishun', 'Sengkang') THEN 'Tier_1'
      WHEN dps_zone.name IN ('Far_east', 'Ang Mo Kio', 'Serangoon', 'Jurong east') THEN 'Tier_2'
      WHEN dps_zone.name IN ('Geylang', 'Bedok') THEN 'Tier_3'
      WHEN dps_zone.name IN ('Bukit timah', 'Sg_south') THEN 'Tier_4'
    END AS zone_tier,
    dps_zone.name AS zone_name,
    EXTRACT(HOUR FROM DATE_ADD(sessions.dps_session_timestamp, INTERVAL 8 HOUR)) AS hour_,
    EXTRACT(DATE FROM DATE_ADD(sessions.dps_session_timestamp, INTERVAL 8 HOUR)) AS date_local,
  FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_ga_sessions`
  -- LEFT JOIN UNNEST(schemes) schemes
  LEFT JOIN UNNEST(events) events
  LEFT JOIN UNNEST(dps_zone) dps_zone
  WHERE entity_id = 'FP_SG'
    AND sessions.experiment_id = 43
  ORDER BY events_ga_session_id
),

-- Assign session to zone based on price sensitivity (as arranged in experiment high --> low)
sessions_dedup_zone AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY events_ga_session_id, event_action, vendor_code, variant ORDER BY zone_tier) AS priority
  FROM raw_sessions
),

sessions_zone_cleaned AS (
  SELECT * EXCEPT(priority)
  FROM sessions_dedup_zone
  WHERE priority = 1
),

-- Assign variant based on the highest occurrence (i.e. more variant 3 instances than 2)
sessions_dedup_variant AS (
  SELECT 
    events_ga_session_id,
    variant,
    ROW_NUMBER() OVER (PARTITION BY events_ga_session_id ORDER BY COUNT(variant) DESC) AS priority,
  FROM sessions_zone_cleaned
  GROUP BY events_ga_session_id, variant
),

sessions_variant_cleaned AS (
  SELECT * EXCEPT(priority)
  FROM sessions_dedup_variant
  WHERE priority = 1
),

sessions_final AS (
SELECT ga.*
FROM sessions_zone_cleaned AS ga
LEFT JOIN sessions_variant_cleaned ON ga.events_ga_session_id = sessions_variant_cleaned.events_ga_session_id
  AND ga.variant = sessions_variant_cleaned.variant
WHERE sessions_variant_cleaned.variant IS NOT NULL
),

funnel_intermediate AS (
SELECT
  variant,
  zone_tier,
  zone_name,
  hour_,
  date_local,
  events_ga_session_id,
  -- COUNT(DISTINCT events_ga_session_id) AS cvr_denominator,
  -- COUNT(DISTINCT CASE WHEN event_action = 'transaction' THEN events_ga_session_id END) AS cvr_numerator,
  COUNT(DISTINCT IF(event_action IN ("shop_list.loaded", "shop_list.updated"), events_ga_session_id, NULL)) AS list_visited,
  COUNT(DISTINCT IF(event_action = "shop_details.loaded", events_ga_session_id, NULL)) AS details_visited,
  COUNT(DISTINCT IF(event_action = "checkout.loaded", events_ga_session_id, NULL)) AS checkout_visited,
  COUNT(DISTINCT IF(event_action = "transaction", events_ga_session_id, NULL)) AS transaction_done,
FROM sessions_final
GROUP BY 1,2,3,4,5,6
),

funnel_final AS (
  SELECT
    variant,
    zone_tier,
    zone_name,
    hour_,
    date_local,
    COUNT(DISTINCT IF(list_visited > 0, events_ga_session_id, NULL)) AS mcvr2_denominator, --mcvr2
    COUNT(DISTINCT IF(list_visited > 0 AND details_visited > 0, events_ga_session_id, NULL)) AS mcvr2_numerator, --mcvr2
    COUNT(DISTINCT IF(details_visited > 0, events_ga_session_id, NULL)) AS mcvr3_denominator, --mcvr3
    COUNT(DISTINCT IF(details_visited > 0 AND checkout_visited > 0, events_ga_session_id, NULL)) AS mcvr3_numerator, --mcvr3
    COUNT(DISTINCT IF(checkout_visited > 0, events_ga_session_id, NULL)) AS mcvr4_denominator, --mcvr4
    COUNT(DISTINCT IF(checkout_visited > 0 AND transaction_done > 0, events_ga_session_id, NULL)) AS mcvr4_numerator --mcvr4
  FROM funnel_intermediate
  GROUP BY 1,2,3,4,5
),

conversion AS (
  SELECT 
    variant,
    zone_tier,
    zone_name,
    hour_,
    date_local,
    COUNT(DISTINCT events_ga_session_id) AS cvr_denominator,
    COUNT(DISTINCT CASE WHEN event_action = 'transaction' THEN events_ga_session_id END) AS cvr_numerator,
  FROM sessions_final
  GROUP BY 1,2,3,4,5
)

SELECT
  conversion.*,
  mcvr2_denominator,
  mcvr2_numerator,
  mcvr3_denominator,
  mcvr3_numerator,
  mcvr4_denominator,
  mcvr4_numerator
FROM conversion
FULL JOIN funnel_final ON (conversion.variant = funnel_final.variant
  AND conversion.zone_tier = funnel_final.zone_tier
  AND conversion.zone_name = funnel_final.zone_name
  AND conversion.hour_ = funnel_final.hour_
  AND conversion.date_local = funnel_final.date_local)
  AND conversion.variant IS NOT NULL
  AND conversion.zone_name IS NOT NULL

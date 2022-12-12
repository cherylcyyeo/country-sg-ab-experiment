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
    CASE WHEN ST_WITHIN(ga_location, ST_GEOGFROMGEOJSON('{"type":"Polygon","coordinates":[[[103.770246505737, 1.30022331781257], [103.77007484436, 1.29619031091753], [103.770632743835, 1.29528931913695], [103.770611286163, 1.29445268362526], [103.770740032196, 1.29404509186573], [103.771190643311, 1.29370185664898], [103.773036003113, 1.29299393386782], [103.773937225342, 1.29196422765192], [103.774237632751, 1.29220020202995], [103.7757396698, 1.29157808771335], [103.77715587616, 1.29151373105121], [103.778722286224, 1.2902909541611], [103.779237270355, 1.28919689012957], [103.779923915863, 1.28915398564818], [103.78181219101, 1.28773813735632], [103.784151077271, 1.28591469521579], [103.791060447693, 1.28357639691782], [103.794386386871, 1.2821605455327], [103.795309066772, 1.27525289574291], [103.80206823349, 1.27289313535016], [103.806059360504, 1.27353670658069], [103.806960582733, 1.27302184960913], [103.809106349945, 1.27274296870659], [103.811037540436, 1.27280732584063], [103.811745643616, 1.27235682586867], [103.825371265411, 1.27046901560717], [103.823890686035, 1.27257134967466], [103.823096752167, 1.27351525420894], [103.822860717773, 1.27463077730375], [103.823182582855, 1.27608953754468], [103.82363319397, 1.27870672296143], [103.826293945312, 1.27731232122691], [103.828053474426, 1.27769846332149], [103.829126358032, 1.27913576949664], [103.825006484985, 1.28140971493491], [103.826680183411, 1.28338332632055], [103.827302455902, 1.28460610652376], [103.827173709869, 1.28585033841057], [103.82504940033, 1.2864295495993], [103.822603225708, 1.28825299137169], [103.821830749512, 1.28857477507846], [103.820714950562, 1.28859622732412], [103.817346096039, 1.28887510650143], [103.816595077515, 1.28966883937733], [103.815216422081, 1.29026413887182], [103.815414905548, 1.29142792216583], [103.815650939941, 1.29260779408579], [103.816637992859, 1.29722001541705], [103.817539215088, 1.30027694818005], [103.815908432007, 1.30048074356605], [103.814588785172, 1.30082934089869], [103.81472826004, 1.30217546245411], [103.81573677063, 1.30232562736394], [103.816680908203, 1.30200384540333], [103.818032741547, 1.30097414285347], [103.81867647171, 1.30144608990769], [103.819363117218, 1.3018322283409], [103.819191455841, 1.30668040585285], [103.816165924072, 1.30665895376054], [103.813333511353, 1.30633717235406], [103.81208896637, 1.30715235183709], [103.810114860535, 1.30820350393766], [103.80784034729, 1.30648733701555], [103.807389736176, 1.30668040585284], [103.806467056274, 1.30687347467529], [103.805673122406, 1.30700218721535], [103.802518844605, 1.30691637885603], [103.798377513886, 1.30841802472041], [103.797175884247, 1.30951208042707], [103.7952876091, 1.30938336801577], [103.795217871666, 1.30931364879022], [103.792937994003, 1.31021463555555], [103.791580796242, 1.31181817667543], [103.791425228119, 1.31170019040731], [103.791146278381, 1.31077775211934], [103.790717124939, 1.31064903977306], [103.790245056152, 1.31208632726411], [103.789300918579, 1.31328764154847], [103.787648677826, 1.31442459917861], [103.786211013794, 1.31601204858858], [103.785331249237, 1.3155830083071], [103.784708976746, 1.31511106391224], [103.785395622253, 1.31351288791251], [103.787262439728, 1.31030580681733], [103.783807754517, 1.31147494387997], [103.780245780945, 1.31262262835522], [103.77897977829, 1.3127942446801], [103.77726316452, 1.31431733904694], [103.773572444916, 1.31704174496294], [103.77067565918, 1.31910113643506], [103.767242431641, 1.32270506740827], [103.76531124115, 1.32354169349328], [103.761491775513, 1.313180381368], [103.764045238495, 1.31079920417639], [103.766105175018, 1.30816059977892], [103.763530254364, 1.30648733701555], [103.766620159149, 1.29998734418216], [103.770246505737, 1.30022331781257]]]}', make_valid => true)) THEN 'Queenstown' ELSE dps_zone.name END AS zone_name,
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

-- Micro conversion 
funnel_intermediate AS (
SELECT
  variant,
  zone_tier,
  zone_name,
  hour_,
  date_local,
  events_ga_session_id,
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

-- Overall conversion
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

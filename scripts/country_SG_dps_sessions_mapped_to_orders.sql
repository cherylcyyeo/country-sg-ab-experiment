WITH acc AS (
  SELECT
    acc1.uuid,
    accounting.discount_local,
    accounting.vouchers_local,
    gfv_local
  FROM
    `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_accounting` acc1
  CROSS JOIN
    UNNEST(accounting) AS accounting
  WHERE created_date_utc >= '2022-11-01'
    AND global_entity_id = 'FP_SG'
    AND accounting.is_order_last_entry = TRUE
),

vouchers AS (
  SELECT
    uuid,
    CASE WHEN type LIKE '%free_delivery%' THEN TRUE ELSE FALSE END AS is_voucher_fd,
    attributions_foodpanda_ratio AS voucher_fp_ratio,
    attributions_foodpanda_ratio/100 * value_local AS subsidised_vouchers
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_vouchers` vouchers
  WHERE global_entity_id = 'FP_SG'
    AND created_date_utc >= '2022-11-01'
),

discounts AS (
  SELECT
    uuid,
    CASE WHEN discount_type LIKE '%free_delivery%' THEN TRUE ELSE FALSE END AS is_discount_fd,
    attributions_foodpanda_ratio AS discounts_fp_ratio,
    attributions_foodpanda_ratio/100 * discount_amount_local AS subsidised_discounts
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_discounts` discounts
  WHERE global_entity_id = 'FP_SG'
    AND created_date_utc >= '2022-11-01'
),

subscriptions AS (
  SELECT
  uuid,
  is_free_delivery_subscription_order
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_sb_subscriptions`
  WHERE global_entity_id = 'FP_SG'
  AND created_date_utc >= '2022-11-01'
),

vendors AS (
  SELECT *
  FROM (
    SELECT DISTINCT
      vendor_code, name, IFNULL(chain_code, vendor_code) chain_code, IFNULL(chain_name, name) chain_name, tags.title,
      CASE 
        WHEN tags.id = 't6mx9vx' THEN 'Low-High'
        WHEN tags.id = 't8ah4uo' THEN 'High-High'
        WHEN tags.id = 't3yo1ix' THEN 'Low-Low'
        WHEN tags.id = 't1rp2zk' THEN 'High-Low'
      END AS target_groups
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors`
  LEFT JOIN UNNEST(tags) AS tags 
    ON tags.id IS NOT NULL

    WHERE global_entity_id = 'FP_SG'
    AND vertical = 'Restaurant'
    AND is_active = true
    AND is_test = false
  )
  WHERE target_groups IS NOT NULL
),

pd_orders AS (
  SELECT
  uuid,
  code,
  delivery_fee_local
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders`
  WHERE global_entity_id = 'FP_SG'
  AND is_valid_order = TRUE
  AND is_test_order = FALSE
  AND created_date_utc >= '2022-11-01'
),

order_level AS (
  SELECT
    test_name,
    target_group,
    platform_order_code AS order_code,
    order_report_date,
    date_local,
    order_hour AS hour_,
    CASE WHEN EXTRACT(HOUR FROM order_report_time) IN (7, 8, 9, 10) THEN "Breakfast"
    WHEN EXTRACT(HOUR FROM order_report_time) IN (11, 12, 13) THEN "Lunch"
    WHEN EXTRACT(HOUR FROM order_report_time) IN (14, 15, 16) THEN "Teabreak"
    WHEN EXTRACT(HOUR FROM order_report_time) IN (17, 18, 19) THEN "Dinner"
    else "Wee hours" end as time_of_day,
    order_report_time AS date_timestamp,
    variant AS variant_transaction,
    ga_vendor_code AS vendor_code,
    vendors.chain_code,
    CAST(dps_minimum_order_value_local AS FLOAT64) AS dps_minimum_order_value_local,
    target_groups,
    CASE 
      WHEN target_group IN ('Target Group 1', 'Target Group 2', 'Target Group 3', 'Target Group 10', 'Target Group 11', 'Target Group 12', 'Target Group 19', 'Target Group 20', 'Target Group 21', 'Target Group 28', 'Target Group 29', 'Target Group 30') THEN 'tier_1'
      WHEN target_group IN ('Target Group 4', 'Target Group 13', 'Target Group 22', 'Target Group 31') THEN 'tier_2'
      WHEN target_group IN ('Target Group 5', 'Target Group 6', 'Target Group 14', 'Target Group 15', 'Target Group 23', 'Target Group 24', 'Target Group 32', 'Target Group 33') THEN 'tier_3'
      WHEN target_group IN ('Target Group 7', 'Target Group 8', 'Target Group 9', 'Target Group 16', 'Target Group 17', 'Target Group 18', 'Target Group 25', 'Target Group 26', 'Target Group 27', 'Target Group 34', 'Target Group 35', 'Target Group 36') THEN 'tier_4'
    END AS zone_tiers,
    gpo.pd_customer_uuid,
    CAST(gpo.final_df_wo_gst AS FLOAT64) AS final_df_wo_gst,
    CAST(gpo.gfv_local AS FLOAT64) AS gfv_local,
    CAST(gpo.gmv_local AS FLOAT64) AS gmv_local,
    CAST(mov_customer_fee_local AS FLOAT64) AS difference_to_minimum_local,
    CAST(gpo.commission_local AS FLOAT64) AS commission_local,
    CAST(gpo.commission_base_local AS FLOAT64) AS commission_base_local,
    CAST(gpo.total_rider_salary AS FLOAT64) AS total_rider_salary,
    CAST(gpo.gpo_excl_wastage AS FLOAT64) AS gpo_excl_wastage,
    gpo.pickup_distance_manhattan_in_meters,
    gpo.dropoff_distance_manhattan_in_meters,
    gpo.delivery_distance_in_meters,
    gpo.drive_time,
    CASE WHEN gpo.zone_name = 'Queenstown' THEN 'Queenstown' ELSE orders.zone_name END AS zone_name,
    treatment,
    discount_dh_local,
    discount_other_local,
    voucher_dh_local,
    voucher_other_local,
    CASE 
        WHEN subscriptions.is_free_delivery_subscription_order = TRUE THEN FALSE
        WHEN is_voucher_fd = TRUE OR is_discount_fd = TRUE THEN FALSE
        WHEN (pd_orders.delivery_fee_local = 0.93 OR pd_orders.delivery_fee_local = 0.99) THEN FALSE
      ELSE TRUE END AS has_surge,
    surge_event,
    travel_time_distance_km AS pd_dist,
    actual_delivery_time,

  FROM `fulfillment-dwh-production.curated_data_shared.dps_ab_test_orders_v2` orders

  INNER JOIN vendors
  ON orders.ga_vendor_code = vendors.vendor_code

  LEFT JOIN `fulfillment-dwh-production.pandata_report.country_SG_rs_gross_profit_per_order` gpo
  ON orders.platform_order_code = gpo.order_code

  LEFT JOIN pd_orders
  ON orders.platform_order_code = pd_orders.code

  LEFT JOIN acc ON pd_orders.uuid = acc.uuid

  LEFT JOIN vouchers ON pd_orders.uuid = vouchers.uuid

  LEFT JOIN discounts ON pd_orders.uuid = discounts.uuid

  LEFT JOIN subscriptions ON pd_orders.uuid = subscriptions.uuid

  WHERE country_code = 'sg'
    AND test_name = 'SG_20221104_R_BD_R_LovedBrandsAFVTiered'
    AND variant IN ('Control', 'Variation1', 'Variation2', 'Variation3')
    AND target_group IS NOT NULL
)

SELECT *,
  CASE WHEN has_surge = TRUE AND (surge_event IS NOT NULL OR surge_event != 'no_surge') THEN TRUE ELSE FALSE END AS has_surge_fee,
FROM order_level

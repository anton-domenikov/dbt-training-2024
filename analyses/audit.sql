{% set old_etl_relation=ref('refactoring_customer_orders') %}

{% set dbt_relation=ref('refactorED_customers_orders') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["loaded_at"],
    primary_key="order_id"
) }}
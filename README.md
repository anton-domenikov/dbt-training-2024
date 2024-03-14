Final result of the practice session in [Refactoring SQL for Modularity](https://courses.getdbt.com/courses/refactoring-sql-for-modularity) , used dbt-audit-helper

Query ran:
```
{% set old_etl_relation=ref('refactoring_customer_orders') %}

{% set dbt_relation=ref('refactorED_customers_orders') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["loaded_at"],
    primary_key="order_id"
) }}
```

Result:
```
IN_A,IN_B,COUNT,PERCENT_OF_TOTAL
true,true,99,100.0
```
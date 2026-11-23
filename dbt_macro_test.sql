Macros
-- valid_range
{% test range_validation(model, column_name, min_value, max_value %}
  
  select *
  from {{model}}
  where {{column_name}} < min_value OR  {{column_name}} > max_value

{% endtest %}

-- no future data test
{% test no_future_dates (model, column_name) %}
  
  SELECT *
  FROM {{model}}
  WHERE column_name > current_date

  {% endtest %}

-- singular test check_date_format
WITH cte as (
  SELECT order_date,
  case 
    when to_date(order_date::text,'YYYY-MM-DD') IS NOT NULL  then 'Valid'
    ELSE 'Not Valid'
  END AS data_format_status
FROM {{ ref(customer_order) }}
WHERE order_date is not null
)
SELECT * from cte where data_format_status = 'Not Valid'

-- order less than 100
SELECT *
FROM {{ ref(customer_order) }}
WHERE order_amount < 100

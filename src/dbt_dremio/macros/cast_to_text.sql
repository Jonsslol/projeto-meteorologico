{% macro cast_to_text(column_name) %}
    cast({{ column_name }} as VARCHAR)
{% endmacro %}
tables = {
    "small_table": ["id", "value", "ts"],
    "mid_table": ["id", "col1", "col2", "ts"],
    "big_sales_table": ["id", "user_id", "amount", "transaction_date"],
    "huge_events": ["event_id", "ts", "payload", "user_id"],
    "user_master": ["user_id", "name", "region", "created_at"],
    "orders": ["order_id", "user_id", "price", "order_date"],
    "products": ["product_id", "name", "category", "price"],
    "clickstream": ["event_id", "user_id", "event_type", "ts"],
    "dim_date": ["date", "is_holiday", "year", "month"],
    "small_lookup": ["id", "val"]
}

table_sizes = {
    "small_table": 100,
    "mid_table": 50_000,
    "big_sales_table": 10_000_000,
    "huge_events": 50_000_000,
    "user_master": 1_000_000,
    "orders": 7_000_000,
    "products": 500_000,
    "clickstream": 20_000_000,
    "dim_date": 3650,
    "small_lookup": 1000
}

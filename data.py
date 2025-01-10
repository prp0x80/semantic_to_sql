query_semantic_layer_data = [
    (
        {"metrics": ["total_revenue"]},
        {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ]
        },
    ),
    (
        {"metrics": ["total_revenue"], "dimensions": ["status"]},
        {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [{"name": "status", "sql": "status", "table": "order_items"}],
        },
    ),
    (
        {
            "metrics": ["total_revenue"],
            "filters": [{"field": "status", "operator": "=", "value": "Complete"}],
        },
        {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [{"name": "status", "sql": "status", "table": "order_items"}],
        },
    ),
    (
        {
            "metrics": ["count_of_orders"],
            "filters": [{"field": "num_of_item", "operator": ">", "value": 1}],
        },
        {
            "metrics": [
                {"name": "count_of_orders",
                    "sql": "COUNT(order_id)", "table": "orders"}
            ],
            "dimensions": [
                {"name": "num_of_item", "sql": "num_of_item", "table": "orders"}
            ],
        },
    ),
    (
        {
            "metrics": ["count_of_orders"],
            "filters": [
                {"field": "status", "operator": "=", "value": "Complete"},
                {"field": "gender", "operator": "=", "value": "F"},
            ],
        },
        {
            "metrics": [
                {"name": "count_of_orders",
                    "sql": "COUNT(order_id)", "table": "orders"}
            ],
            "dimensions": [
                {"name": "num_of_item", "sql": "num_of_item", "table": "orders"},
                {"name": "gender", "sql": "gender", "table": "orders"},
                {"name": "status", "sql": "status", "table": "orders"},
            ],
        },
    ),
    (
        {
            "metrics": ["total_revenue"],
            "dimensions": ["order_id"],
            "filters": [{"field": "total_revenue", "operator": ">", "value": 1000}],
        },
        {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [
                {"name": "order_id", "sql": "order_id", "table": "order_items"}
            ],
        },
    ),
    (
        {
            "metrics": ["total_revenue"],
            "dimensions": ["order_id", "gender", "status"],
            "filters": [{"field": "total_revenue", "operator": ">", "value": 1000}],
        },
        {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [
                {"name": "order_id", "sql": "order_id", "table": "order_items"},
                {"name": "gender", "sql": "gender", "table": "orders"},
                {"name": "status", "sql": "status", "table": "orders"},
            ],
            "joins": [
                {
                    "one": "orders",
                    "many": "order_items",
                    "join": "order_items.order_id = orders.order_id",
                }
            ],
        },
    ),
    (
        {"metrics": ["total_revenue"], "dimensions": ["ordered_date__week"]},
        {
            "metrics": [
                {
                    "name": "total_revenue",
                    "sql": "SUM(sale_price)",
                    "table": "order_items",
                }
            ],
            "dimensions": [
                {"name": "ordered_date", "sql": "created_at", "table": "orders"}
            ],
            "joins": [
                {
                    "one": "orders",
                    "many": "order_items",
                    "join": "order_items.order_id = orders.order_id",
                }
            ],
        },
    ),
]

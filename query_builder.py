#   SELECT
#     [ WITH differential_privacy_clause ]
#     [ { ALL | DISTINCT } ]
#     [ AS { STRUCT | VALUE } ]
#     select_list
#   [ FROM from_clause[, ...] ]
#   [ WHERE bool_expression ]
#   [ GROUP BY group_by_specification ]
#   [ HAVING bool_expression ]
#   [ QUALIFY bool_expression ]
#   [ WINDOW window_clause ]

SELECT = "SELECT "
FROM = "FROM "
WHERE = "WHERE "
GROUP_BY = "GROUP BY "
HAVING = "HAVING "


def preprocess_query_semantic_data(query: dict, semantic_layer: dict) -> dict[str, list]:
    filters = query.get("filters")
    metrics = semantic_layer.get("metrics", [])
    dimensions = semantic_layer.get("dimensions", [])
    joins = semantic_layer.get("joins")

    if not metrics:
        raise ValueError("Metrics cannot be empty")

    data = {}
    data["metrics"] = metrics

    if dimensions:
        data["dimensions"] = dimensions
    if filters:
        data["filters"] = filters
    if joins:
        data["joins"] = joins

    return data


def get_all_tables_and_columns(metrics: list[dict], dimensions: list[dict]) -> tuple[list[str], list[str]]:
    projections = metrics + dimensions
    tables = list({x["table"] for x in projections})
    columns = list({f'{x["table"]}.{x["sql"]}' for x in dimensions})
    return tables, columns


def right_strip(input_string: str, string_to_be_removed: str) -> str:
    if input_string.endswith(string_to_be_removed):
        return input_string[:-len(string_to_be_removed)].rstrip()
    return input_string


def build_select(metrics: list[dict], dimensions: list[dict]) -> str:
    m = ""
    for metric in metrics:
        m += f'{metric["sql"]} AS {metric["name"]}, '

    d = ""
    for dimension in dimensions:
        schema = dimension["table"]
        column_name = dimension["sql"]
        alias = dimension["name"]
        d += f'{schema}.{column_name}{" AS " +
                                      alias if alias != column_name else ""}, '

    if d:
        return SELECT + m + right_strip(d, ", ")
    return SELECT + right_strip(m, ", ")


def build_from(joins: list[dict], tables: list[str]) -> str:
    if len(tables) == 1:
        from_stmt = FROM + tables[0]
    else:
        j = ""
        for join in joins:
            j += f'{join["one"]} JOIN {join["many"]} ON {join["join"]}\n'
        from_stmt = FROM + j.strip()
    return from_stmt


def build_groupby(columns: list[str]) -> str:
    return GROUP_BY + ", ".join(columns)


def build_where(filters: list[dict], columns: list[str]) -> str:
    where_stmt = ""
    columns = [c.split(".")[1] for c in columns]
    for filter in filters:
        if isinstance(filter["value"], int) or isinstance(filter["value"], float):
            value = filter["value"]
        else:
            value = "'" + filter["value"] + "'"
        if filter["field"] in columns:
            where_stmt += f'{filter["field"]
                             } {filter["operator"]} {value} AND '
    where_stmt = WHERE + right_strip(where_stmt, "AND ")
    return where_stmt


def build_having(filters: list[dict], columns: list[str]) -> str:
    having_stmt = ""
    columns = [c.split(".")[1] for c in columns]
    for filter in filters:
        if isinstance(filter["value"], int) or isinstance(filter["value"], float):
            value = filter["value"]
        else:
            value = "'" + filter["value"] + "'"
        if filter["field"] not in columns:
            having_stmt += f'{filter["field"]
                              } {filter["operator"]} {value} AND '
    having_stmt = HAVING + right_strip(having_stmt, "AND ")
    return having_stmt


def has_metric_filter(filters: list[dict], columns: list[str]) -> bool:
    filter_fields = [x["field"] for x in filters]
    column_names = [x.split(".")[1] for x in columns]
    return any([f not in column_names for f in filter_fields])

def build_query(query: dict, semantic_layer: dict) -> str:
    # preprocess the data
    data = preprocess_query_semantic_data(query, semantic_layer)

    # get the metrics, dimensions, filters and joins
    metrics = data.get("metrics")
    dimensions = data.get("dimensions", [])
    filters = data.get("filters", [])
    joins = data.get("joins", [])

    # get all tables and columns
    tables, columns = get_all_tables_and_columns(metrics, dimensions)

    # build the SQL query
    select_stmt = build_select(metrics, dimensions)
    # print(select_stmt)

    from_stmt = build_from(joins, tables)
    # print(from_stmt)

    # if the filter is on non-metric columns, we need a where statement
    where_stmt = ""
    if filters:
        if not has_metric_filter(filters, columns):
            where_stmt = build_where(filters, columns)
            # print(where_stmt)

    # since metric is always present
    # a group by statement is needed if there are other columns in addition to the metrics
    group_by_stmt = ""
    if len(columns) > 0:
        group_by_stmt = build_groupby(columns)
        # print(group_by_stmt)

    # if the filter is on a metric column, we need a having statement
    having_stmt = ""
    if filters:
        if has_metric_filter(filters, columns):
            having_stmt = build_having(filters, columns)
            # print(having_stmt)

    # combine all the statements
    sql_stmt = []
    sql_stmt.append(select_stmt)
    sql_stmt.append(from_stmt)
    if where_stmt:
        sql_stmt.append(where_stmt)
    if group_by_stmt:
        sql_stmt.append(group_by_stmt)
    if having_stmt:
        sql_stmt.append(having_stmt)

    return sql_stmt

if __name__ == "__main__":
    from pprint import pprint
    from data import query_semantic_layer_data
    from run_sql import query_bigquery

    # iterate over the query semantic layer data
    for idx, (query, semantic_layer) in enumerate(query_semantic_layer_data, 1):
        
        # build the query
        sql_stmt = build_query(query, semantic_layer)
        
        print(f"Query#{idx}")
        
        # print the input data
        pprint(query)
        print()
        pprint(semantic_layer)
        
        # print the SQL statement
        print(f'SQL Query: {" ".join(sql_stmt)}')
        print()

        # run the SQL statement
        print("Results:")
        query_bigquery(" ".join(sql_stmt))
        print()
        print("="*100)


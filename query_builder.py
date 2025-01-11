"""
File: query_builder.py
Description: A script to convert query-semantic pairs to SQL queries and run them on BigQuery

Author: Prashant Patel <prashant.patel@gmx.com>
Source: https://github.com/prp0x80/semantic_to_sql
"""

# https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
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

# NOTE:
# The solution is limited to the queries provided and may not scale beyond the types of query-semantic pairs provided as input
# However, the solution is written in a way that it should require minimum efforts to implement a change
# Based on the data provided, a query with a metric is the most simple query that can exist, and each query-semantic pair has a metric
# Hence, it's assumed that we would atleast neeed a metric to build a SQL query.
# This observation is purely based on the data provided - in real world, projecting column data from a table could be seen as the most lean query.
# Due to the nature of our application, aggregations makes more sense over details.

# SELECT clause can have column names, alias, and metrics
SELECT = "SELECT "

# FROM clause can have a table or multiple tables. In case of multiple tables we would expect a join clause
FROM = "FROM "

# WHERE clause is added to the query when we have conditions in the query (except filter on metrics)
# The conditions are in filters key - made up of column name, operator, and value.
# For multiple filter condition, we assume and use AND logial operator for chaining the filter conditions.
WHERE = "WHERE "

# GROUP BY is added to the query when we have additional columns along with the metric in SELECT clause
GROUP_BY = "GROUP BY "

# HAVING is added to the query when we have metrics-based filter condition
HAVING = "HAVING "


def preprocess_query_semantic_data(query: dict, semantic_layer: dict) -> dict[str, list]:
    
    # remove the metrics key from query, as we have more structured metric definition in semantic layer
    query.pop("metrics", None)

    # merge query and semantic dictionary to create a single data dictionary
    data = {**query, **semantic_layer}

    return data


def get_all_tables_and_columns(metrics: list[dict], dimensions: list[dict]) -> tuple[list[str], list[str]]:
    
    projections = metrics + dimensions

    # get all the tables from metrics and dimensions
    tables = list({x["table"] for x in projections})
    if not tables:
        raise ValueError("Atleast one table is required, but no tables were found.")
    
    # get all the columns from dimensions only, as metrics are created using columns
    columns = list({f'{x["table"]}.{x["sql"]}' for x in dimensions})
    
    return tables, columns


def right_strip(input_string: str, string_to_be_removed: str) -> str:
    """Removes characters from the end of the string"""

    if input_string.endswith(string_to_be_removed):
        return input_string[:-len(string_to_be_removed)].rstrip()
    
    return input_string


def build_select(metrics: list[dict], dimensions: list[dict]) -> str:
    
    # comma-separated string of metrics
    m = ""
    for metric in metrics:
        m += f'{metric["sql"]} AS {metric["name"]}, '

    # comma-separated string of dimensions/columns
    d = ""
    for dimension in dimensions:
        schema = dimension["table"]
        column_name = dimension["sql"]
        alias = dimension["name"]
        
        # use alias if the alias name is not same as the column name
        d += f'{schema}.{column_name}{" AS " + alias if alias != column_name else ""}, '

    # return select clause with metrics and dimensions if dimensions exists
    if d:
        return SELECT + m + right_strip(d, ", ")

    # return select clause with metrics
    return SELECT + right_strip(m, ", ")


def build_from(joins: list[dict], tables: list[str]) -> str:
    
    # for a single table we return from clause with the table
    if len(tables) == 1:
        from_stmt = FROM + tables[0]

    # for more than one table, we build from clause using joins
    else:
        j = ""
        for join in joins:
            j += f'{join["one"]} JOIN {join["many"]} ON {join["join"]}\n'
        from_stmt = FROM + j.strip()
    return from_stmt


def build_groupby(columns: list[str]) -> str | None:
    
    # we need group by only if there are columns along with the metrics
    if len(columns) > 0:
        return GROUP_BY + ", ".join(columns)


def build_where(filters: list[dict], columns: list[str]) -> str | None:

    where_stmt = ""

    # where clause would only exists if there are filters in the data
    if filters:
        columns = [c.split(".")[1] for c in columns]        
        for filter in filters:

            # value formatting based on it's type
            if isinstance(filter["value"], int) or isinstance(filter["value"], float):
                value = filter["value"]
            else:
                value = "'" + filter["value"] + "'"

            # only create where statement if the filter has column in field and not metric
            if filter["field"] in columns:
                where_stmt += f'{filter["field"]} {filter["operator"]} {value} AND '
        
        # create where statement if it's applicable
        if where_stmt:
            where_stmt = WHERE + right_strip(where_stmt, "AND ")
    
    return where_stmt


def build_having(filters: list[dict], columns: list[str]) -> str | None:

    having_stmt = ""

    # having clause would only exists if there are filters on metrics in data
    if filters and has_metric_filter(filters, columns):
        columns = [c.split(".")[1] for c in columns]
        for filter in filters:

            # value formatting based on it's type
            if isinstance(filter["value"], int) or isinstance(filter["value"], float):
                value = filter["value"]
            else:
                value = "'" + filter["value"] + "'"
            
            # only create having statement if the filter has metric
            if filter["field"] not in columns:
                having_stmt += f'{filter["field"]} {filter["operator"]} {value} AND '
        
        # create having statement
        having_stmt = HAVING + right_strip(having_stmt, "AND ")
    
    return having_stmt


def has_metric_filter(filters: list[dict], columns: list[str]) -> bool:
    """Returns true, if any of the column from filters is not in column names"""
    filter_fields = [x["field"] for x in filters]
    column_names = [x.split(".")[1] for x in columns]
    return any([f not in column_names for f in filter_fields])

def build_query(query: dict, semantic_layer: dict) -> str:

    # preprocess the data
    data = preprocess_query_semantic_data(query, semantic_layer)

    # get the metrics, dimensions, filters, and joins
    metrics = data.get("metrics")
    dimensions = data.get("dimensions", [])
    filters = data.get("filters", [])
    joins = data.get("joins", [])

    # get all tables and columns
    tables, columns = get_all_tables_and_columns(metrics, dimensions)

    # build the SQL query
    select_stmt = build_select(metrics, dimensions)
    from_stmt = build_from(joins, tables)
    where_stmt = build_where(filters, columns)
    group_by_stmt = build_groupby(columns)
    having_stmt = build_having(filters, columns)

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

    return " ".join(sql_stmt)

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
        print(f'SQL Query: {sql_stmt}')
        print()

        # run the SQL statement
        print("Results:")
        query_bigquery(sql_stmt)
        print()
        print("="*100)

import streamlit as st
from data import query_semantic_layer_data
from query_builder import build_query
from run_sql import query_bigquery

st.set_page_config(page_title="Demo", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)

data = {f"Query#{idx}": pairs for idx, pairs in enumerate(query_semantic_layer_data, 1)}

query_id = st.sidebar.selectbox(label="Select query", options=data.keys())

query, semantic_layer = data[query_id]

left_column, right_column = st.columns(2)

with left_column:
    
    st.markdown("### Query")
    st.write(query)

    st.markdown("### Semantic Layer")
    st.write(semantic_layer)

with right_column:

    st.markdown("### SQL Statement")
    sql_statement = build_query(query, semantic_layer)
    st.code(sql_statement, language="sql", wrap_lines=True)

    st.markdown("### Results")
    st.code(query_bigquery(sql_statement), wrap_lines=False)

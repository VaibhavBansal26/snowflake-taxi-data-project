import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()

# ——————————————————————————————————————————————————————————————————————————
#  Streamlit page config
# ——————————————————————————————————————————————————————————————————————————
st.set_page_config(
    page_title="❄️ Taxi Data Project (Snowflake + Snowpark + Streamlit)",
    page_icon="❄️",
    layout="wide",
)

st.title("❄️ Taxi Data Project (Snowflake Connector + Streamlit)")

# ——————————————————————————————————————————————————————————————————————————
#  Snowflake connection using connector (no Snowpark session)
# ——————————————————————————————————————————————————————————————————————————
# Store your Snowflake credentials in .streamlit/secrets.toml under [snowflake]
# sf_creds = st.secrets["snowflake"]


@st.cache_resource
def get_snowflake_connection():
    creds = st.secrets["snowflake"]
    conn = snowflake.connector.connect(
        user      = creds["user"],
        password  = creds["password"],
        account   = creds["account"],
        warehouse = creds["warehouse"],
        database  = creds["database"],
        schema    = creds["schema"],
        role      = creds.get("role"),
        login_timeout=60,
    )
    return conn

# @st.cache_resource
# def get_snowflake_connection(env_path=None):
#     if env_path:
#         load_dotenv(dotenv_path=env_path)
#     else:
#         load_dotenv()

#     password = os.getenv("SNOWFLAKE_PASSWORD")
#     account = os.getenv("SNOWFLAKE_ACCOUNT")
#     user = os.getenv("SNOWFLAKE_USER")
#     warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
#     database = os.getenv("SNOWFLAKE_DATABASE")
#     schema = os.getenv("SNOWFLAKE_SCHEMA")
#     role = os.getenv("SNOWFLAKE_ROLE")

#     conn = snowflake.connector.connect(
#         user=user,
#         password=password,
#         account=account,
#         warehouse=warehouse,
#         database=database,
#         schema=schema,
#         role=role,
#     )
#     return conn

# === Step 1: Load environment variables from a custom path ===
# dotenv_path = r".env"
# load_dotenv(dotenv_path=dotenv_path)


conn = get_snowflake_connection()
cur = conn.cursor()

# ——————————————————————————————————————————————————————————————————————————
#  Actual vs Predicted Rides by Hour for Selected Locations
# ——————————————————————————————————————————————————————————————————————————
st.markdown("---")
st.subheader("📊 Actual vs Predicted Rides by Hour for All Locations")

# Mapping of location IDs to descriptive names
location_info = {
    132: ("JFK Airport", "Queens"),
    237: ("Upper East Side South", "Manhattan"),
    161: ("Lincoln Square East", "Manhattan"),
     43: ("Grand Central", "Manhattan"),
}

db = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")

for loc, (name, borough) in location_info.items():
    st.markdown(f"**{name} ({borough}) — Location {loc}**")
    sql = f"""
        SELECT
            t.PICKUP_HOUR       AS HOUR,
            t.RIDES             AS ACTUAL_RIDES,
            p.PREDICTED_RIDES   AS PREDICTED_RIDES
        FROM {db}.{schema}.YELLOW_TAXI_DATA_TRANSFORMED t
        LEFT JOIN {db}.{schema}.YELLOW_TAXI_DATA_PREDICTIONS p
          ON t.PICKUP_HOUR         = p.PICKUP_HOUR
         AND t.PICKUP_LOCATION_ID = p.PICKUP_LOCATION_ID
        WHERE
            t.PICKUP_LOCATION_ID = {loc}
          AND t.PICKUP_HOUR >= DATEADD(year, -1, CURRENT_DATE - INTERVAL '30 day')
          AND t.PICKUP_HOUR <  DATEADD(year, -1, CURRENT_DATE + INTERVAL '10 day')
        ORDER BY t.PICKUP_HOUR
    """
    try:
        cur.execute(sql)
        df_loc = cur.fetch_pandas_all()
        if df_loc.empty:
            st.write(f"No data found for {name} ({loc})")
        else:
            df_loc = df_loc.set_index('HOUR')
            st.line_chart(df_loc, use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load data for {name} ({loc}): {e}")

# ——————————————————————————————————————————————————————————————————————————
#  Bonus: custom SQL runner
# ——————————————————————————————————————————————————————————————————————————
st.markdown("---")
st.subheader("📝 Run your own SQL")
def run_custom_query(query: str) -> pd.DataFrame:
    cur.execute(query)
    return cur.fetch_pandas_all()

n_rows = 1000
default_table = "YELLOW_TAXI_DATA_PREDICTIONS"
default_query = f"SELECT * FROM {db}.{schema}.{default_table} LIMIT {n_rows}"
sql = st.text_area("Enter a SELECT query:", default_query, height=150)

if st.button("Execute SQL"):
    try:
        custom_df = run_custom_query(sql)
        st.write(f"Returned {len(custom_df)} rows")
        st.dataframe(custom_df, use_container_width=True)
    except Exception as e:
        st.error(f"Query failed: {e}")
st.markdown(
    "<div style='text-align:center;color:gray;font-size:0.95em;'>"
    "Vaibhav Bansal &copy; 2025"
    "</div>",
    unsafe_allow_html=True,
)
if st.button("🚀 Visit my portfolio"):
    st.write("[Go now →](https://www.vaibhavbansal.in)")

import streamlit as st
import pandas as pd
import psycopg2
import requests
import plotly.express as px

# Step 1: Load transaction data from PostgreSQL
@st.cache_data
def load_data():
    conn = psycopg2.connect(
        dbname="phonepe_data",
        user="postgres",
        password="0000",
        host="localhost",
        port="5432"
    )
    query = """
    SELECT state, SUM(transaction_amount) AS total_amount
    FROM aggregated_transaction
    GROUP BY state
    ORDER BY total_amount DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Step 2: Load GeoJSON for India states
@st.cache_data
def load_geojson():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/" \
          "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    return requests.get(url).json()

# Step 3: Prepare and map states
def map_states(df):
    # Your DB state format → GeoJSON format
    state_name_map = {
        "andaman-&-nicobar-islands": "Andaman and Nicobar Islands",
        "andhra-pradesh": "Andhra Pradesh",
        "arunachal-pradesh": "Arunachal Pradesh",
        "assam": "Assam",
        "bihar": "Bihar",
        "chandigarh": "Chandigarh",
        "chhattisgarh": "Chhattisgarh",
        "dadra-&-nagar-haveli-&-daman-&-diu": "Dadra and Nagar Haveli and Daman and Diu",
        "delhi": "Delhi",
        "goa": "Goa",
        "gujarat": "Gujarat",
        "haryana": "Haryana",
        "himachal-pradesh": "Himachal Pradesh",
        "jammu-&-kashmir": "Jammu and Kashmir",
        "jharkhand": "Jharkhand",
        "karnataka": "Karnataka",
        "kerala": "Kerala",
        "ladakh": "Ladakh",
        "lakshadweep": "Lakshadweep",
        "madhya-pradesh": "Madhya Pradesh",
        "maharashtra": "Maharashtra",
        "manipur": "Manipur",
        "meghalaya": "Meghalaya",
        "mizoram": "Mizoram",
        "nagaland": "Nagaland",
        "odisha": "Odisha",
        "puducherry": "Puducherry",
        "punjab": "Punjab",
        "rajasthan": "Rajasthan",
        "sikkim": "Sikkim",
        "tamil-nadu": "Tamil Nadu",
        "telangana": "Telangana",
        "tripura": "Tripura",
        "uttar-pradesh": "Uttar Pradesh",
        "uttarakhand": "Uttarakhand",
        "west-bengal": "West Bengal"
    }

    df['state_mapped'] = df['state'].replace(state_name_map)

    # Full list of Indian states (to ensure all shown)
    all_states = list(state_name_map.values())
    full_states_df = pd.DataFrame({'state_mapped': all_states})

    # Merge and fill missing with 0
    merged_df = full_states_df.merge(df[['state_mapped', 'total_amount']], on='state_mapped', how='left')
    merged_df['total_amount'] = merged_df['total_amount'].fillna(0)

    return merged_df

# Step 4: Create map
def create_map(merged_df, geojson):
    fig = px.choropleth(
        merged_df,
        geojson=geojson,
        featureidkey='properties.ST_NM',
        locations='state_mapped',
        color='total_amount',
        color_continuous_scale='Blues',
        hover_name='state_mapped',
        labels={'total_amount': 'Transaction Amount (₹)'},
        title='PhonePe Transaction Amount by State'
    )
    fig.update_geos(fitbounds="locations", visible=False)
    return fig

# Step 5: Streamlit dashboard
def main():
    st.set_page_config(page_title="PhonePe Dashboard", layout="wide")
    st.title("📍 PhonePe Transaction Insights - India Map")

    df = load_data()
    geojson = load_geojson()
    merged_df = map_states(df)
    fig = create_map(merged_df, geojson)

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📊 Transaction Amount by State")
    st.dataframe(merged_df.sort_values(by='total_amount', ascending=False).reset_index(drop=True))

if __name__ == "__main__":
    main()
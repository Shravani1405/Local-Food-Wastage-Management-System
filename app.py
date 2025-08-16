# app.py
import os
import sqlite3
from datetime import date, datetime

import pandas as pd
import streamlit as st
import altair as alt

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Local Food Wastage Management System",
    page_icon="🥗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# Constants & helpers
# -----------------------------
DB_PATH = os.getenv("FOOD_WASTE_DB", "food_waste.db")

def get_conn():
    # One connection per session
    if "conn" not in st.session_state:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        st.session_state.conn = conn
    return st.session_state.conn

@st.cache_data(show_spinner=False, ttl=60)
def fetch_df(sql: str, params: tuple | list = ()):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    return df

def run_write(sql: str, params: tuple | list = ()):
    """Execute INSERT/UPDATE/DELETE."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    return cur.lastrowid

def kpi_card(label, value, help_text=None):
    c = st.container()
    c.metric(label, value, help=help_text)

def download_button_for_df(df: pd.DataFrame, label="Download CSV"):
    return st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="export.csv",
        mime="text/csv",
        use_container_width=True
    )

# -----------------------------
# 15 Queries (SQLite-ready)
# -----------------------------
QUERIES = {
    "1. All Providers": "SELECT * FROM providers ORDER BY Name",
    "2. All Receivers": "SELECT * FROM receivers ORDER BY Name",
    "3. All Food Listings": "SELECT * FROM food_listings ORDER BY Expiry_Date",
    "4. All Claims": "SELECT * FROM claims ORDER BY Claim_ID DESC",
    "5. Expiring soon (≤ N days)": """
        SELECT Food_ID, Food_Name, Quantity, Expiry_Date, Location, Food_Type, Meal_Type, Provider_ID
        FROM food_listings
        WHERE julianday(Expiry_Date) - julianday('now') <= ?
        ORDER BY Expiry_Date ASC
    """,
    "6. Total quantity by city": """
        SELECT Location AS City, SUM(Quantity) AS Total_Quantity
        FROM food_listings
        GROUP BY Location
        ORDER BY Total_Quantity DESC
    """,
    "7. Top 5 providers by quantity": """
        SELECT p.Provider_ID, p.Name, SUM(f.Quantity) AS Total_Quantity
        FROM food_listings f
        JOIN providers p ON f.Provider_ID = p.Provider_ID
        GROUP BY p.Provider_ID, p.Name
        ORDER BY Total_Quantity DESC
        LIMIT 5
    """,
    "8. Number of claims by receiver": """
        SELECT r.Receiver_ID, r.Name, COUNT(c.Claim_ID) AS Total_Claims
        FROM claims c
        JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
        GROUP BY r.Receiver_ID, r.Name
        ORDER BY Total_Claims DESC
    """,
    "9. Unclaimed food items": """
        SELECT f.Food_ID, f.Food_Name, f.Quantity, f.Expiry_Date, f.Location, f.Food_Type, f.Meal_Type
        FROM food_listings f
        LEFT JOIN claims c ON f.Food_ID = c.Food_ID
        WHERE c.Claim_ID IS NULL
        ORDER BY f.Expiry_Date
    """,
    "10. Claim fulfillment rate per provider (%)": """
        SELECT p.Name,
               ROUND(100.0 * SUM(CASE WHEN c.Status='Completed' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT f.Food_ID),0), 2)
                 AS Fulfillment_Rate
        FROM providers p
        JOIN food_listings f ON p.Provider_ID = f.Provider_ID
        LEFT JOIN claims c ON f.Food_ID = c.Food_ID
        GROUP BY p.Provider_ID, p.Name
        ORDER BY Fulfillment_Rate DESC
    """,
    "11. Average days to expiry": """
        SELECT ROUND(AVG(julianday(Expiry_Date) - julianday('now')), 2) AS Avg_Days_To_Expiry
        FROM food_listings
    """,
    "12. Receivers in a given city": "SELECT * FROM receivers WHERE City = ? ORDER BY Name",
    "13. Food type availability (count & qty)": """
        SELECT Food_Type, COUNT(*) AS Item_Count, SUM(Quantity) AS Total_Quantity
        FROM food_listings
        GROUP BY Food_Type
        ORDER BY Total_Quantity DESC
    """,
    "14. Providers with zero claims": """
        SELECT p.Provider_ID, p.Name
        FROM providers p
        LEFT JOIN food_listings f ON p.Provider_ID = f.Provider_ID
        LEFT JOIN claims c ON f.Food_ID = c.Food_ID
        GROUP BY p.Provider_ID, p.Name
        HAVING COUNT(c.Claim_ID) = 0
        ORDER BY p.Name
    """,
    "15. Quantity trend by expiry month": """
        SELECT strftime('%Y-%m', Expiry_Date) AS Month, SUM(Quantity) AS Total_Quantity
        FROM food_listings
        GROUP BY Month
        ORDER BY Month
    """,
}

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.title("🔎 Filters")

# Lists for filters
city_list = fetch_df("SELECT DISTINCT Location FROM food_listings WHERE Location IS NOT NULL ORDER BY 1")["Location"].tolist()
provider_list = fetch_df("SELECT Provider_ID, Name FROM providers ORDER BY Name")
food_type_list = fetch_df("SELECT DISTINCT Food_Type FROM food_listings WHERE Food_Type IS NOT NULL ORDER BY 1")["Food_Type"].tolist()
meal_type_list = fetch_df("SELECT DISTINCT Meal_Type FROM food_listings WHERE Meal_Type IS NOT NULL ORDER BY 1")["Meal_Type"].tolist()

city_filter = st.sidebar.multiselect("City", city_list)
provider_filter_name = st.sidebar.multiselect("Provider", provider_list["Name"].tolist())
food_type_filter = st.sidebar.multiselect("Food Type", food_type_list)
meal_type_filter = st.sidebar.multiselect("Meal Type", meal_type_list)

# Build dynamic WHERE for base food view
where_clauses = []
params: list = []

if city_filter:
    where_clauses.append(f"Location IN ({','.join('?' for _ in city_filter)})")
    params.extend(city_filter)
if provider_filter_name:
    # map names to IDs
    prov_ids = provider_list[provider_list["Name"].isin(provider_filter_name)]["Provider_ID"].tolist()
    if prov_ids:
        where_clauses.append(f"Provider_ID IN ({','.join('?' for _ in prov_ids)})")
        params.extend(prov_ids)
if food_type_filter:
    where_clauses.append(f"Food_Type IN ({','.join('?' for _ in food_type_filter)})")
    params.extend(food_type_filter)
if meal_type_filter:
    where_clauses.append(f"Meal_Type IN ({','.join('?' for _ in meal_type_filter)})")
    params.extend(meal_type_filter)

WHERE = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

# -----------------------------
# Top Nav
# -----------------------------
tabs = st.tabs(["📊 Dashboard", "🗂️ Query Browser (15)", "🧰 CRUD", "📇 Provider Directory", "ℹ️ Help"])

# -----------------------------
# Dashboard
# -----------------------------
with tabs[0]:
    st.subheader("📊 Dashboard — Available Food Overview")
    colA, colB, colC, colD = st.columns(4)

    # KPI queries
    df_total_qty = fetch_df(f"SELECT SUM(Quantity) AS qty FROM food_listings {WHERE}", params)
    df_expiring_3 = fetch_df(
        f"""SELECT COUNT(*) AS n FROM food_listings
            {WHERE + (' AND ' if WHERE else ' WHERE ')} (julianday(Expiry_Date)-julianday('now')) <= 3""",
        params
    ) if WHERE else fetch_df("SELECT COUNT(*) AS n FROM food_listings WHERE julianday(Expiry_Date)-julianday('now') <= 3")

    df_providers_n = fetch_df("SELECT COUNT(*) AS n FROM providers")
    df_receivers_n = fetch_df("SELECT COUNT(*) AS n FROM receivers")

    kpi_card("Total Quantity", int(df_total_qty["qty"].fillna(0).iloc[0]))
    colB.metric("Expiring ≤ 3 days", int(df_expiring_3["n"].iloc[0]))
    colC.metric("Providers", int(df_providers_n["n"].iloc[0]))
    colD.metric("Receivers", int(df_receivers_n["n"].iloc[0]))

    # Base filtered data
    food_df = fetch_df(f"SELECT * FROM food_listings {WHERE} ORDER BY Expiry_Date", params)
    # Join provider for contact display
    prov_df = fetch_df("SELECT Provider_ID, Name, Contact, City FROM providers")
    food_df = food_df.merge(prov_df, on="Provider_ID", how="left", suffixes=("", "_Provider"))

    st.markdown("#### Filtered Food Listings")
    st.dataframe(food_df, use_container_width=True, height=350)
    download_button_for_df(food_df, "Download filtered food CSV")

    # Charts
    charts_cols = st.columns(2)
    with charts_cols[0]:
        by_city = fetch_df(
            f"SELECT Location AS City, SUM(Quantity) AS Total_Quantity FROM food_listings {WHERE} GROUP BY Location ORDER BY Total_Quantity DESC",
            params
        )
        if not by_city.empty:
            topn = st.slider("Top N cities", 5, min(25, len(by_city)), min(10, len(by_city)))
            chart = (
                alt.Chart(by_city.head(topn))
                .mark_bar()
                .encode(
                    x=alt.X("Total_Quantity:Q", title="Quantity"),
                    y=alt.Y("City:N", sort="-x", title="City"),
                    color=alt.Color("City:N", legend=None),
                    tooltip=["City", "Total_Quantity"]
                )
                .properties(height=400)
            )
            st.altair_chart(chart, use_container_width=True)

    with charts_cols[1]:
        by_type = fetch_df(
            f"SELECT Food_Type, SUM(Quantity) AS Total_Quantity FROM food_listings {WHERE} GROUP BY Food_Type ORDER BY Total_Quantity DESC",
            params
        )
        if not by_type.empty:
            chart2 = (
                alt.Chart(by_type)
                .mark_arc(innerRadius=60)
                .encode(theta="Total_Quantity:Q", color="Food_Type:N", tooltip=["Food_Type", "Total_Quantity"])
            )
            st.altair_chart(chart2, use_container_width=True)

    # Trend
    trend = fetch_df(
        f"SELECT strftime('%Y-%m', Expiry_Date) AS Month, SUM(Quantity) AS Total_Quantity FROM food_listings {WHERE} GROUP BY Month ORDER BY Month",
        params
    )
    if not trend.empty:
        st.markdown("#### Quantity Trend by Expiry Month")
        line = (
            alt.Chart(trend)
            .mark_line(point=True)
            .encode(x="Month:T", y="Total_Quantity:Q", tooltip=["Month", "Total_Quantity"])
            .properties(height=320)
        )
        st.altair_chart(line, use_container_width=True)

# -----------------------------
# Query Browser (15)
# -----------------------------
with tabs[1]:
    st.subheader("🗂️ Query Browser")
    qname = st.selectbox("Choose a query", list(QUERIES.keys()), index=0)

    if qname == "5. Expiring soon (≤ N days)":
        days = st.slider("Days threshold", 1, 30, 3)
        df = fetch_df(QUERIES[qname], (days,))
    elif qname == "12. Receivers in a given city":
        city_opt = st.selectbox("City", city_list)
        df = fetch_df(QUERIES[qname], (city_opt,))
    else:
        df = fetch_df(QUERIES[qname])

    st.dataframe(df, use_container_width=True, height=420)
    download_button_for_df(df)

# -----------------------------
# CRUD
# -----------------------------
with tabs[2]:
    st.subheader("🧰 CRUD Operations")

    crud_tabs = st.tabs(["➕ Add", "✏️ Update", "🗑️ Delete"])

    # ---- ADD ----
    with crud_tabs[0]:
        add_sub = st.tabs(["Provider", "Food Listing", "Claim"])

        # Add Provider
        with add_sub[0]:
            st.markdown("##### Add Provider")
            with st.form("add_provider"):
                name = st.text_input("Name")
                ptype = st.selectbox("Type", ["Restaurant", "Grocery Store", "Supermarket", "Caterer", "Other"])
                address = st.text_input("Address")
                city = st.text_input("City")
                contact = st.text_input("Contact")
                submitted = st.form_submit_button("Create Provider")
            if submitted:
                if name and city:
                    run_write(
                        "INSERT INTO providers (Name, Type, Address, City, Contact) VALUES (?,?,?,?,?)",
                        (name, ptype, address, city, contact),
                    )
                    st.success(f"Provider '{name}' added.")
                    st.cache_data.clear()
                else:
                    st.error("Name and City are required.")

        # Add Food Listing
        with add_sub[1]:
            st.markdown("##### Add Food Listing")
            provs = fetch_df("SELECT Provider_ID, Name FROM providers ORDER BY Name")
            with st.form("add_food"):
                provider = st.selectbox("Provider", provs["Name"].tolist())
                provider_id = int(provs[provs["Name"] == provider]["Provider_ID"].iloc[0]) if not provs.empty else None
                food_name = st.text_input("Food Name")
                qty = st.number_input("Quantity", min_value=1, value=10)
                exp = st.date_input("Expiry Date", value=date.today())
                provider_type = st.text_input("Provider Type (optional)")
                location = st.text_input("Location (City)")
                ftype = st.selectbox("Food Type", ["Vegetarian", "Non-Vegetarian", "Vegan", "Other"])
                mtype = st.selectbox("Meal Type", ["Breakfast", "Lunch", "Dinner", "Snacks", "Other"])
                submit_food = st.form_submit_button("Create Food Listing")
            if submit_food:
                if provider_id and food_name and location:
                    days_to_exp = (pd.to_datetime(exp) - pd.Timestamp.today()).days
                    qty_cat = "Small" if qty < 5 else ("Medium" if qty <= 20 else "Large")
                    run_write("""
                        INSERT INTO food_listings
                        (Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type, Days_To_Expiry, Quantity_Category)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (food_name, int(qty), str(exp), provider_id, provider_type, location, ftype, mtype, int(days_to_exp), qty_cat))
                    st.success(f"Food '{food_name}' added for {provider}.")
                    st.cache_data.clear()
                else:
                    st.error("Provider, Food Name, and Location are required.")

        # Add Claim
        with add_sub[2]:
            st.markdown("##### Add Claim")
            unclaimed = fetch_df("""
                SELECT f.Food_ID, f.Food_Name, f.Location, f.Quantity, f.Expiry_Date
                FROM food_listings f
                LEFT JOIN claims c ON f.Food_ID = c.Food_ID
                WHERE c.Claim_ID IS NULL
                ORDER BY f.Expiry_Date
            """)
            recs = fetch_df("SELECT Receiver_ID, Name FROM receivers ORDER BY Name")

            with st.form("add_claim"):
                food_select = st.selectbox("Food Item", unclaimed.apply(lambda r: f"#{r.Food_ID} — {r.Food_Name} ({r.Location})", axis=1).tolist())
                food_id = int(food_select.split("—")[0].replace("#","").strip()) if not unclaimed.empty else None
                receiver = st.selectbox("Receiver", recs["Name"].tolist())
                receiver_id = int(recs[recs["Name"] == receiver]["Receiver_ID"].iloc[0]) if not recs.empty else None
                status = st.selectbox("Status", ["Pending", "Completed", "Cancelled"], index=0)
                submit_claim = st.form_submit_button("Create Claim")
            if submit_claim:
                if food_id and receiver_id:
                    run_write("INSERT INTO claims (Food_ID, Receiver_ID, Status) VALUES (?,?,?)",
                              (food_id, receiver_id, status))
                    st.success("Claim created.")
                    st.cache_data.clear()
                else:
                    st.error("Choose a food item and receiver.")

    # ---- UPDATE ----
    with crud_tabs[1]:
        upd_sub = st.tabs(["Provider Contact", "Food Quantity", "Claim Status"])

        # Update Provider Contact
        with upd_sub[0]:
            provs = fetch_df("SELECT Provider_ID, Name, Contact FROM providers ORDER BY Name")
            if not provs.empty:
                sel = st.selectbox("Provider", provs["Name"].tolist())
                pid = int(provs.loc[provs["Name"] == sel, "Provider_ID"].iloc[0])
                old = provs.loc[provs["Name"] == sel, "Contact"].iloc[0]
                new = st.text_input("New Contact", value=old if pd.notna(old) else "")
                if st.button("Update Contact"):
                    run_write("UPDATE providers SET Contact=? WHERE Provider_ID=?", (new, pid))
                    st.success("Contact updated.")
                    st.cache_data.clear()
            else:
                st.info("No providers found.")

        # Update Food Quantity
        with upd_sub[1]:
            foods = fetch_df("SELECT Food_ID, Food_Name, Quantity FROM food_listings ORDER BY Food_ID DESC")
            if not foods.empty:
                choice = st.selectbox("Food", foods.apply(lambda r: f"#{r.Food_ID} — {r.Food_Name}", axis=1).tolist())
                fid = int(choice.split("—")[0].replace("#","").strip())
                cur_qty = int(foods[foods["Food_ID"] == fid]["Quantity"].iloc[0])
                new_qty = st.number_input("New Quantity", min_value=0, value=cur_qty)
                if st.button("Update Quantity"):
                    run_write("UPDATE food_listings SET Quantity=? WHERE Food_ID=?", (int(new_qty), fid))
                    st.success("Quantity updated.")
                    st.cache_data.clear()
            else:
                st.info("No food listings found.")

        # Update Claim Status
        with upd_sub[2]:
            cls = fetch_df("""
                SELECT c.Claim_ID, c.Status, f.Food_Name, r.Name AS Receiver
                FROM claims c
                JOIN food_listings f ON c.Food_ID = f.Food_ID
                JOIN receivers r ON c.Receiver_ID = r.Receiver_ID
                ORDER BY c.Claim_ID DESC
            """)
            if not cls.empty:
                rowlbl = cls.apply(lambda r: f"#{r.Claim_ID} — {r.Food_Name} → {r.Receiver} ({r.Status})", axis=1).tolist()
                sel = st.selectbox("Claim", rowlbl)
                claim_id = int(sel.split("—")[0].replace("#","").strip())
                new_status = st.selectbox("New Status", ["Pending", "Completed", "Cancelled"])
                if st.button("Update Status"):
                    run_write("UPDATE claims SET Status=? WHERE Claim_ID=?", (new_status, claim_id))
                    st.success("Status updated.")
                    st.cache_data.clear()
            else:
                st.info("No claims found.")

    # ---- DELETE ----
    with crud_tabs[2]:
        del_sub = st.tabs(["Provider", "Food Listing", "Claim"])

        # Delete Provider (safe)
        with del_sub[0]:
            provs = fetch_df("SELECT Provider_ID, Name FROM providers ORDER BY Name")
            if not provs.empty:
                sel = st.selectbox("Provider", provs["Name"].tolist(), key="delprov")
                pid = int(provs.loc[provs["Name"] == sel, "Provider_ID"].iloc[0])
                st.warning("Deleting a provider with food listings will fail unless you remove their listings first.")
                if st.button("Delete Provider"):
                    try:
                        run_write("DELETE FROM providers WHERE Provider_ID=?", (pid,))
                        st.success("Provider deleted.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Delete failed (likely FK constraint): {e}")
            else:
                st.info("No providers to delete.")

        # Delete Food Listing
        with del_sub[1]:
            foods = fetch_df("SELECT Food_ID, Food_Name FROM food_listings ORDER BY Food_ID DESC")
            if not foods.empty:
                sel = st.selectbox("Food", foods.apply(lambda r: f"#{r.Food_ID} — {r.Food_Name}", axis=1).tolist(), key="delfood")
                fid = int(sel.split("—")[0].replace("#","").strip())
                if st.button("Delete Food Listing"):
                    try:
                        run_write("DELETE FROM food_listings WHERE Food_ID=?", (fid,))
                        st.success("Food listing deleted.")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")
            else:
                st.info("No food listings to delete.")

        # Delete Claim
        with del_sub[2]:
            cls = fetch_df("SELECT Claim_ID FROM claims ORDER BY Claim_ID DESC")
            if not cls.empty:
                cid = st.selectbox("Claim ID", cls["Claim_ID"].tolist(), key="delclaim")
                if st.button("Delete Claim"):
                    run_write("DELETE FROM claims WHERE Claim_ID=?", (int(cid),))
                    st.success("Claim deleted.")
                    st.cache_data.clear()
            else:
                st.info("No claims to delete.")

# -----------------------------
# Provider Directory (Contacts)
# -----------------------------
with tabs[3]:
    st.subheader("📇 Provider Directory & Contacts")
    city_pick = st.selectbox("Filter by City", ["All"] + city_list)
    if city_pick == "All":
        provs = fetch_df("SELECT * FROM providers ORDER BY City, Name")
    else:
        provs = fetch_df("SELECT * FROM providers WHERE City=? ORDER BY Name", (city_pick,))
    st.dataframe(provs, use_container_width=True, height=420)
    download_button_for_df(provs, "Download providers CSV")

    st.markdown("#### Quick Contact Cards")
    if not provs.empty:
        grid_cols = st.columns(3)
        for idx, row in provs.head(9).iterrows():
            with grid_cols[idx % 3]:
                st.info(f"**{row['Name']}**  \n📍 {row['City']}  \n📞 {row['Contact'] or 'N/A'}  \n🏷️ {row['Type'] or '—'}")

# -----------------------------
# Help / Deployment
# -----------------------------
with tabs[4]:
    st.subheader("How to Use & Deploy")
    st.markdown(
        """
**Running locally**
```bash
pip install streamlit pandas altair
streamlit run app.py
""")
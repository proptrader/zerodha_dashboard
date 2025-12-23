import streamlit as st
import pandas as pd
import datetime
import time
import schedule
import threading
from kiteconnect import KiteConnect
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

# --- Page Configuration ---
st.set_page_config(
    page_title="Kite Trade & Investment Dashboard",
    page_icon="📈",
    layout="wide"
)

# --- Session State Initialization ---
if 'scheduler_running' not in st.session_state:
    st.session_state['scheduler_running'] = False
if 'last_run_log' not in st.session_state:
    st.session_state['last_run_log'] = []

# --- Helper Functions ---

def load_google_sheet(json_key_path, sheet_name):
    """Connects to Google Sheets using Service Account."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name)
        return sheet
    except Exception as e:
        return str(e)

def get_kite_data(api_key, access_token):
    """Fetches holdings and orders from Kite."""
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        # 1. Fetch Holdings (Equity & MF usually appear here if in Demat)
        holdings = kite.holdings()
        df_holdings = pd.DataFrame(holdings)

        # 2. Fetch Trades/Orders (For FNO/Daily logic)
        # Note: 'orders' gives order book, 'trades' gives executed trades
        trades = kite.trades()
        df_trades = pd.DataFrame(trades)
        
        return df_holdings, df_trades, None
    except Exception as e:
        return None, None, str(e)

def update_spreadsheet_logic(creds_file, sheet_name, api_key, access_token):
    """Core logic to fetch data and update the sheet."""
    log_msg = f"[{datetime.datetime.now()}] Starting automated data fetch..."
    st.session_state['last_run_log'].insert(0, log_msg)
    
    # Connect to Kite
    df_holdings, df_trades, error = get_kite_data(api_key, access_token)
    
    if error:
        st.session_state['last_run_log'].insert(0, f"Error fetching Kite data: {error}")
        return

    # Connect to GSheets
    sheet_obj = load_google_sheet(creds_file, sheet_name)
    if isinstance(sheet_obj, str): # Error happened
        st.session_state['last_run_log'].insert(0, f"Error GSheets: {sheet_obj}")
        return

    try:
        # Update Stocks/Holdings Sheet
        try:
            ws_holdings = sheet_obj.worksheet("Holdings")
        except:
            ws_holdings = sheet_obj.add_worksheet(title="Holdings", rows="100", cols="20")
        
        if not df_holdings.empty:
            ws_holdings.clear()
            ws_holdings.update([df_holdings.columns.values.tolist()] + df_holdings.values.tolist())
            st.session_state['last_run_log'].insert(0, "✅ Holdings updated successfully.")

        # Update FNO/Trades Sheet
        try:
            ws_trades = sheet_obj.worksheet("Trades")
        except:
            ws_trades = sheet_obj.add_worksheet(title="Trades", rows="100", cols="20")

        if not df_trades.empty:
            # Filter for FNO if needed based on 'segment' column usually present in Kite response
            # df_fno = df_trades[df_trades['instrument_token'] ...] 
            ws_trades.clear()
            ws_trades.update([df_trades.columns.values.tolist()] + df_trades.values.tolist())
            st.session_state['last_run_log'].insert(0, "✅ Trades updated successfully.")
            
    except Exception as e:
        st.session_state['last_run_log'].insert(0, f"Error writing to sheet: {e}")

# --- Background Scheduler ---
def run_scheduler(creds, sheet, key, token, time_str):
    schedule.every().day.at(time_str).do(update_spreadsheet_logic, creds, sheet, key, token)
    while True:
        schedule.run_pending()
        time.sleep(60)

# --- Sidebar Configuration ---
st.sidebar.header("⚙️ Configuration")

with st.sidebar.expander("🔑 API Credentials", expanded=True):
    kite_api_key = st.text_input("Kite API Key", type="password")
    kite_access_token = st.text_input("Kite Access Token", type="password")
    gsheet_json = st.text_input("Google Json Path", value="credentials.json")
    gsheet_name = st.text_input("Google Sheet Name", value="MyTradingJournal")

with st.sidebar.expander("⏰ Scheduler Settings"):
    schedule_time = st.time_input("Daily Run Time", datetime.time(17, 30))
    enable_scheduler = st.checkbox("Enable Background Scheduler")

if enable_scheduler and not st.session_state['scheduler_running']:
    if kite_api_key and kite_access_token:
        t = threading.Thread(target=run_scheduler, args=(gsheet_json, gsheet_name, kite_api_key, kite_access_token, schedule_time.strftime("%H:%M")))
        t.start()
        st.session_state['scheduler_running'] = True
        st.sidebar.success(f"Scheduler running at {schedule_time.strftime('%H:%M')}")
    else:
        st.sidebar.warning("Enter API Keys to start scheduler")

# --- Main Dashboard ---

st.title("📊 Personal Wealth Dashboard")

# Tabs for organization
tab1, tab2, tab3 = st.tabs(["Dashboard", "Data Preview", "System Logs"])

# Data Fetching for Display (On Demand)
if kite_api_key and kite_access_token:
    holdings, trades, err = get_kite_data(kite_api_key, kite_access_token)
    
    if err:
        st.error(f"Failed to fetch data: {err}")
    else:
        # --- TAB 1: DASHBOARD ---
        with tab1:
            col1, col2, col3 = st.columns(3)
            
            # Key Metrics
            total_invested = 0
            current_value = 0
            pnl = 0
            
            if not holdings.empty:
                # Kite usually returns 'average_price' * 'quantity' for invested
                holdings['invested_val'] = holdings['average_price'] * holdings['quantity']
                # Kite usually returns 'last_price' * 'quantity' for current
                holdings['current_val'] = holdings['last_price'] * holdings['quantity']
                
                total_invested = holdings['invested_val'].sum()
                current_value = holdings['current_val'].sum()
                pnl = current_value - total_invested
            
            col1.metric("Total Invested", f"₹{total_invested:,.2f}")
            col2.metric("Current Value", f"₹{current_value:,.2f}")
            col3.metric("Overall P&L", f"₹{pnl:,.2f}", delta=f"{(pnl/total_invested)*100:.2f}%" if total_invested > 0 else "0%")

            st.divider()

            # Charts
            c1, c2 = st.columns(2)
            
            with c1:
                st.subheader("Asset Allocation")
                if not holdings.empty and 'instrument_token' in holdings.columns:
                    # Grouping by symbol or sector if available, simplified here by symbol
                    st.bar_chart(holdings.set_index('tradingsymbol')['current_val'])
                else:
                    st.info("No holdings data to visualize")

            with c2:
                st.subheader("Day's P&L (Realized)")
                if not trades.empty:
                    # This is a simplification. Real PNL calc is complex.
                    # Visualizing transaction volume here as proxy for activity
                    if 'transaction_type' in trades.columns:
                        tx_counts = trades['transaction_type'].value_counts()
                        st.bar_chart(tx_counts)
                else:
                    st.info("No trades executed today")

        # --- TAB 2: DATA PREVIEW ---
        with tab2:
            st.subheader("Holdings (Investments)")
            st.dataframe(holdings, use_container_width=True)
            
            st.subheader("Trades (Daily FNO/Eq)")
            st.dataframe(trades, use_container_width=True)
            
            if st.button("Force Sync to Google Sheet Now"):
                with st.spinner("Uploading to Drive..."):
                    update_spreadsheet_logic(gsheet_json, gsheet_name, kite_api_key, kite_access_token)
                st.success("Manual Sync Complete!")

else:
    with tab1:
        st.info("👈 Please enter your Kite API Key and Access Token in the sidebar to load data.")
        # Mock Data for visuals when no API key
        st.subheader("Mock View (Connect API to see real data)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Invested", "₹1,50,000")
        col2.metric("Current Value", "₹1,65,000")
        col3.metric("Overall P&L", "₹15,000", delta="10%")


# --- TAB 3: LOGS ---
with tab3:
    st.write("Scheduler Logs:")
    for log in st.session_state['last_run_log']:
        st.text(log)
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
if 'accounts_config' not in st.session_state:
    st.session_state['accounts_config'] = []
if 'authenticated_accounts' not in st.session_state:
    st.session_state['authenticated_accounts'] = {}
if 'account_request_tokens' not in st.session_state:
    st.session_state['account_request_tokens'] = {}

# --- Helper Functions ---

def load_accounts_config():
    """Load account configurations from config.json file."""
    try:
        with open('config.json', 'r') as f:
            accounts = json.load(f)
            return accounts
    except FileNotFoundError:
        st.error("config.json file not found. Please create it with account configurations.")
        return []
    except json.JSONDecodeError as e:
        st.error(f"Error parsing config.json: {str(e)}")
        return []
    except Exception as e:
        st.error(f"Error loading config.json: {str(e)}")
        return []

def authenticate_account(account_config, user_request_token=None):
    """
    Authenticate a single account using KiteConnect.
    
    Args:
        account_config: Dictionary containing account configuration
        user_request_token: Optional request token provided by user
    
    Returns:
        tuple: (kite_object, status_message) or (None, error_message)
    """
    try:
        account_id = account_config.get('account_id')
        api_key = account_config.get('api_key')
        secret_api_key = account_config.get('secret_api_key')
        
        if not api_key or not secret_api_key:
            return None, f"Missing API credentials for {account_id}"
        
        # Create KiteConnect object
        kite = KiteConnect(api_key=api_key)
        
        # Determine which request_token to use
        request_token = user_request_token if user_request_token else account_config.get('request_token', '')
        
        if not request_token or request_token.strip() == '':
            return None, f"Request token required for {account_id}"
        
        # Generate session using request_token
        data = kite.generate_session(request_token, api_secret=secret_api_key)
        access_token = data.get('access_token')
        
        if not access_token:
            return None, f"Failed to get access token for {account_id}"
        
        # Set access token
        kite.set_access_token(access_token)
        
        # Verify connection by fetching profile
        profile = kite.profile()
        
        return kite, f"Connected: {account_id}"
        
    except Exception as e:
        error_msg = str(e)
        return None, f"Authentication failed for {account_config.get('account_id', 'unknown')}: {error_msg}"

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

def get_kite_data(kite):
    """
    Fetches holdings and orders from Kite using authenticated KiteConnect object.
    
    Args:
        kite: Authenticated KiteConnect object
    
    Returns:
        tuple: (df_holdings, df_trades, error_message)
    """
    try:
        # 1. Fetch Holdings (Equity & MF usually appear here if in Demat)
        holdings = kite.holdings()
        df_holdings = pd.DataFrame(holdings) if holdings else pd.DataFrame()

        # 2. Fetch Trades/Orders (For FNO/Daily logic)
        # Note: 'orders' gives order book, 'trades' gives executed trades
        trades = kite.trades()
        df_trades = pd.DataFrame(trades) if trades else pd.DataFrame()
        
        return df_holdings, df_trades, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), str(e)

def get_all_accounts_data():
    """
    Fetch data for all authenticated accounts.
    
    Returns:
        dict: {account_id: {'holdings': df, 'trades': df, 'error': str}}
    """
    accounts_data = {}
    
    for account_id, kite in st.session_state['authenticated_accounts'].items():
        holdings, trades, error = get_kite_data(kite)
        accounts_data[account_id] = {
            'holdings': holdings,
            'trades': trades,
            'error': error
        }
    
    return accounts_data

def aggregate_accounts_data(accounts_data):
    """
    Aggregate holdings and trades from all accounts.
    
    Args:
        accounts_data: Dict from get_all_accounts_data()
    
    Returns:
        tuple: (aggregated_holdings_df, aggregated_trades_df)
    """
    all_holdings = []
    all_trades = []
    
    for account_id, data in accounts_data.items():
        if data.get('error'):
            continue
            
        holdings = data.get('holdings', pd.DataFrame())
        trades = data.get('trades', pd.DataFrame())
        
        if not holdings.empty:
            holdings['account_id'] = account_id
            all_holdings.append(holdings)
        
        if not trades.empty:
            trades['account_id'] = account_id
            all_trades.append(trades)
    
    aggregated_holdings = pd.concat(all_holdings, ignore_index=True) if all_holdings else pd.DataFrame()
    aggregated_trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    
    return aggregated_holdings, aggregated_trades

def update_spreadsheet_logic(creds_file, sheet_name):
    """
    Core logic to fetch data from all authenticated accounts and update the sheet.
    Creates separate worksheets per account_id.
    """
    log_msg = f"[{datetime.datetime.now()}] Starting automated data fetch..."
    st.session_state['last_run_log'].insert(0, log_msg)
    
    if not st.session_state['authenticated_accounts']:
        st.session_state['last_run_log'].insert(0, "No authenticated accounts to sync")
        return
    
    # Fetch data for all authenticated accounts
    accounts_data = get_all_accounts_data()
    
    # Connect to GSheets
    sheet_obj = load_google_sheet(creds_file, sheet_name)
    if isinstance(sheet_obj, str):  # Error happened
        st.session_state['last_run_log'].insert(0, f"Error GSheets: {sheet_obj}")
        return
    
    try:
        # Process each account's data
        for account_id, data in accounts_data.items():
            if data.get('error'):
                st.session_state['last_run_log'].insert(0, f"⚠️ Skipping {account_id}: {data['error']}")
                continue
            
            df_holdings = data.get('holdings', pd.DataFrame())
            df_trades = data.get('trades', pd.DataFrame())
            
            # Create account-specific worksheet names
            holdings_ws_name = f"Holdings_{account_id}"
            trades_ws_name = f"Trades_{account_id}"
            
            # Update Holdings Sheet for this account
            try:
                ws_holdings = sheet_obj.worksheet(holdings_ws_name)
            except:
                ws_holdings = sheet_obj.add_worksheet(
                    title=holdings_ws_name, rows="100", cols="20"
                )
            
            if not df_holdings.empty:
                # Ensure account_id column exists
                if 'account_id' not in df_holdings.columns:
                    df_holdings['account_id'] = account_id
                
                ws_holdings.clear()
                ws_holdings.update(
                    [df_holdings.columns.values.tolist()] + df_holdings.values.tolist()
                )
                st.session_state['last_run_log'].insert(
                    0, f"✅ Holdings updated for {account_id}"
                )
            else:
                st.session_state['last_run_log'].insert(
                    0, f"ℹ️ No holdings data for {account_id}"
                )

            # Update Trades Sheet for this account
            try:
                ws_trades = sheet_obj.worksheet(trades_ws_name)
            except:
                ws_trades = sheet_obj.add_worksheet(
                    title=trades_ws_name, rows="100", cols="20"
                )

            if not df_trades.empty:
                # Ensure account_id column exists
                if 'account_id' not in df_trades.columns:
                    df_trades['account_id'] = account_id
                
                ws_trades.clear()
                ws_trades.update(
                    [df_trades.columns.values.tolist()] + df_trades.values.tolist()
                )
                st.session_state['last_run_log'].insert(
                    0, f"✅ Trades updated for {account_id}"
                )
            else:
                st.session_state['last_run_log'].insert(
                    0, f"ℹ️ No trades data for {account_id}"
                )
        
        st.session_state['last_run_log'].insert(0, "✅ All accounts synced successfully")
            
    except Exception as e:
        st.session_state['last_run_log'].insert(0, f"Error writing to sheet: {e}")

# --- Background Scheduler ---
def run_scheduler(creds, sheet, time_str):
    """Run scheduler for all authenticated accounts."""
    schedule.every().day.at(time_str).do(update_spreadsheet_logic, creds, sheet)
    while True:
        schedule.run_pending()
        time.sleep(60)

# --- Sidebar Configuration ---
st.sidebar.header("⚙️ Configuration")

# Load accounts config
if not st.session_state['accounts_config']:
    st.session_state['accounts_config'] = load_accounts_config()

# Google Sheets Configuration
with st.sidebar.expander("📊 Google Sheets", expanded=True):
    gsheet_json = st.text_input("Google Json Path", value="heroic-muse-377907-482b72703bd0.json")
    gsheet_name = st.text_input("Google Sheet Name", value="Wind_TM_Trades")

# Account Management
with st.sidebar.expander("🔑 Account Management", expanded=True):
    if not st.session_state['accounts_config']:
        st.warning("No accounts found in config.json")
    else:
        for idx, account in enumerate(st.session_state['accounts_config']):
            account_id = account.get('account_id', f'Account_{idx}')
            is_connected = account_id in st.session_state['authenticated_accounts']
            request_token = account.get('request_token', '')
            
            st.markdown(f"**{account_id}**")
            
            # Show connection status
            if is_connected:
                st.success("✅ Connected")
            else:
                st.warning("❌ Disconnected")
            
            # Request token input (if empty or user wants to update)
            if not request_token or request_token.strip() == '':
                token_key = f"req_token_{account_id}"
                user_token = st.text_input(
                    f"Request Token for {account_id}",
                    key=token_key,
                    type="password",
                    help="Enter request token to authenticate"
                )
                if user_token:
                    st.session_state['account_request_tokens'][account_id] = user_token
            else:
                # Show existing token option to update
                token_key = f"req_token_{account_id}"
                user_token = st.text_input(
                    f"Request Token for {account_id}",
                    value=request_token,
                    key=token_key,
                    type="password",
                    help="Update request token if needed"
                )
                if user_token and user_token != request_token:
                    st.session_state['account_request_tokens'][account_id] = user_token
            
            # Connect button
            connect_key = f"connect_{account_id}"
            if st.button(f"{'Reconnect' if is_connected else 'Connect'} {account_id}", key=connect_key):
                # Get request token: prioritize user input, then session state, then config
                user_req_token = st.session_state['account_request_tokens'].get(account_id)
                if not user_req_token:
                    # Fall back to config token if user hasn't provided one
                    user_req_token = account.get('request_token', '')
                
                kite_obj, status_msg = authenticate_account(account, user_req_token)
                
                if kite_obj:
                    st.session_state['authenticated_accounts'][account_id] = kite_obj
                    st.session_state['last_run_log'].insert(0, f"[{datetime.datetime.now()}] {status_msg}")
                    st.success(status_msg)
                    st.rerun()
                else:
                    st.error(status_msg)
                    st.session_state['last_run_log'].insert(0, f"[{datetime.datetime.now()}] {status_msg}")
            
            # Disconnect button
            if is_connected:
                disconnect_key = f"disconnect_{account_id}"
                if st.button(f"Disconnect {account_id}", key=disconnect_key):
                    del st.session_state['authenticated_accounts'][account_id]
                    st.session_state['last_run_log'].insert(0, f"[{datetime.datetime.now()}] Disconnected: {account_id}")
                    st.rerun()
            
            st.divider()

with st.sidebar.expander("⏰ Scheduler Settings"):
    schedule_time = st.time_input("Daily Run Time", datetime.time(17, 30))
    enable_scheduler = st.checkbox("Enable Background Scheduler")

if enable_scheduler and not st.session_state['scheduler_running']:
    if st.session_state['authenticated_accounts']:
        t = threading.Thread(
            target=run_scheduler,
            args=(gsheet_json, gsheet_name, schedule_time.strftime("%H:%M"))
        )
        t.start()
        st.session_state['scheduler_running'] = True
        st.sidebar.success(f"Scheduler running at {schedule_time.strftime('%H:%M')}")
    else:
        st.sidebar.warning("Connect at least one account to start scheduler")

# --- Main Dashboard ---

st.title("📊 Personal Wealth Dashboard")

# Tabs for organization
tab1, tab2, tab3 = st.tabs(["Dashboard", "Data Preview", "System Logs"])

# Data Fetching for Display (On Demand)
if st.session_state['authenticated_accounts']:
    # Fetch data for all authenticated accounts
    accounts_data = get_all_accounts_data()
    
    # Aggregate data from all accounts
    holdings, trades = aggregate_accounts_data(accounts_data)
    
    # Check for errors
    has_errors = any(data.get('error') for data in accounts_data.values())
    if has_errors:
        for account_id, data in accounts_data.items():
            if data.get('error'):
                st.error(f"Error fetching data for {account_id}: {data['error']}")
    
    # --- TAB 1: DASHBOARD ---
    with tab1:
        # Show account selector/view toggle
        view_mode = st.radio("View Mode", ["Aggregated", "Per Account"], horizontal=True)
        
        if view_mode == "Aggregated":
            col1, col2, col3 = st.columns(3)
            
            # Key Metrics (Aggregated)
            total_invested = 0
            current_value = 0
            pnl = 0
            
            if not holdings.empty:
                # Check if required columns exist
                if 'average_price' in holdings.columns and 'quantity' in holdings.columns:
                    holdings['invested_val'] = holdings['average_price'] * holdings['quantity']
                if 'last_price' in holdings.columns and 'quantity' in holdings.columns:
                    holdings['current_val'] = holdings['last_price'] * holdings['quantity']
                
                if 'invested_val' in holdings.columns:
                    total_invested = holdings['invested_val'].sum()
                if 'current_val' in holdings.columns:
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
                if not holdings.empty and 'tradingsymbol' in holdings.columns and 'current_val' in holdings.columns:
                    st.bar_chart(holdings.set_index('tradingsymbol')['current_val'])
                else:
                    st.info("No holdings data to visualize")

            with c2:
                st.subheader("Day's P&L (Realized)")
                if not trades.empty:
                    if 'transaction_type' in trades.columns:
                        tx_counts = trades['transaction_type'].value_counts()
                        st.bar_chart(tx_counts)
                    else:
                        st.info("No transaction type data available")
                else:
                    st.info("No trades executed today")
        
        else:  # Per Account view
            for account_id in st.session_state['authenticated_accounts'].keys():
                account_data = accounts_data.get(account_id, {})
                account_holdings = account_data.get('holdings', pd.DataFrame())
                account_trades = account_data.get('trades', pd.DataFrame())
                
                with st.expander(f"📊 {account_id}", expanded=True):
                    col1, col2, col3 = st.columns(3)
                    
                    total_invested = 0
                    current_value = 0
                    pnl = 0
                    
                    if not account_holdings.empty:
                        if 'average_price' in account_holdings.columns and 'quantity' in account_holdings.columns:
                            account_holdings['invested_val'] = account_holdings['average_price'] * account_holdings['quantity']
                        if 'last_price' in account_holdings.columns and 'quantity' in account_holdings.columns:
                            account_holdings['current_val'] = account_holdings['last_price'] * account_holdings['quantity']
                        
                        if 'invested_val' in account_holdings.columns:
                            total_invested = account_holdings['invested_val'].sum()
                        if 'current_val' in account_holdings.columns:
                            current_value = account_holdings['current_val'].sum()
                        pnl = current_value - total_invested
                    
                    col1.metric("Total Invested", f"₹{total_invested:,.2f}")
                    col2.metric("Current Value", f"₹{current_value:,.2f}")
                    col3.metric("Overall P&L", f"₹{pnl:,.2f}", delta=f"{(pnl/total_invested)*100:.2f}%" if total_invested > 0 else "0%")

        # --- TAB 2: DATA PREVIEW ---
        with tab2:
            if view_mode == "Aggregated":
                st.subheader("Holdings (All Accounts)")
                if not holdings.empty:
                    st.dataframe(holdings, width='stretch')
                else:
                    st.info("No holdings data available")
                
                st.subheader("Trades (All Accounts)")
                if not trades.empty:
                    st.dataframe(trades, width='stretch')
                else:
                    st.info("No trades data available")
            else:
                for account_id in st.session_state['authenticated_accounts'].keys():
                    account_data = accounts_data.get(account_id, {})
                    account_holdings = account_data.get('holdings', pd.DataFrame())
                    account_trades = account_data.get('trades', pd.DataFrame())
                    
                    st.subheader(f"Holdings - {account_id}")
                    if not account_holdings.empty:
                        st.dataframe(account_holdings, width='stretch')
                    else:
                        st.info(f"No holdings data for {account_id}")
                    
                    st.subheader(f"Trades - {account_id}")
                    if not account_trades.empty:
                        st.dataframe(account_trades, width='stretch')
                    else:
                        st.info(f"No trades data for {account_id}")
                    
                    st.divider()
            
            if st.button("Force Sync to Google Sheet Now"):
                with st.spinner("Uploading to Drive..."):
                    update_spreadsheet_logic(gsheet_json, gsheet_name)
                st.success("Manual Sync Complete!")

else:
    with tab1:
        st.info("👈 Please connect at least one account in the sidebar to load data.")
        # Mock Data for visuals when no accounts connected
        st.subheader("Mock View (Connect accounts to see real data)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Invested", "₹1,50,000")
        col2.metric("Current Value", "₹1,65,000")
        col3.metric("Overall P&L", "₹15,000", delta="10%")


# --- TAB 3: LOGS ---
with tab3:
    st.write("Scheduler Logs:")
    for log in st.session_state['last_run_log']:
        st.text(log)
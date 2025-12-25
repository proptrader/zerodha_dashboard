import streamlit as st
import pandas as pd
import datetime
import time
import schedule
import threading
from kiteconnect import KiteConnect
import gspread
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
if 'auto_auth_attempted' not in st.session_state:
    st.session_state['auto_auth_attempted'] = False

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

def save_accounts_config(accounts):
    """Save account configurations to config.json file."""
    try:
        with open('config.json', 'w') as f:
            json.dump(accounts, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving config.json: {str(e)}")
        return False

def update_account_access_token(account_id, access_token):
    """Update access_token for a specific account in config.json and session state."""
    accounts = st.session_state.get('accounts_config', [])
    for account in accounts:
        if account.get('account_id') == account_id:
            account['access_token'] = access_token
            if save_accounts_config(accounts):
                # Update session state to keep it in sync
                st.session_state['accounts_config'] = accounts
                return True
    return False

def authenticate_account(account_config, user_request_token=None, try_access_token_first=True):
    """
    Authenticate a single account using KiteConnect.
    First tries existing access_token, then falls back to request_token if needed.
    
    Args:
        account_config: Dictionary containing account configuration
        user_request_token: Optional request token provided by user
        try_access_token_first: If True, try existing access_token first
    
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
        
        # First, try using existing access_token if available
        if try_access_token_first:
            existing_access_token = account_config.get('access_token', '')
            if existing_access_token and existing_access_token.strip():
                try:
                    kite.set_access_token(existing_access_token)
                    # Verify connection by fetching profile
                    profile = kite.profile()
                    # Access token is valid, return authenticated kite object
                    return kite, f"Connected: {account_id} (using saved access_token)"
                except Exception as e:
                    # Access token is invalid/expired, continue to generate new one
                    pass
        
        # Access token not available or invalid, generate new one using request_token
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
        
        # Save access_token to config.json
        update_account_access_token(account_id, access_token)
        
        return kite, f"Connected: {account_id}"
        
    except Exception as e:
        error_msg = str(e)
        return None, f"Authentication failed for {account_config.get('account_id', 'unknown')}: {error_msg}"

def load_google_sheet(json_key_path, sheet_name):
    """Connects to Google Sheets using Service Account."""
    try:
        gc = gspread.service_account(filename=json_key_path)
        sheet = gc.open(sheet_name)
        return sheet
    except Exception as e:
        return str(e)

def get_kite_data(kite):
    """
    Fetches holdings and orders from Kite using authenticated KiteConnect object.
    
    Args:
        kite: Authenticated KiteConnect object
    
    Returns:
        tuple: (df_holdings, df_trades, df_mf_holdings, error_message)
    """
    try:
        # 1. Fetch Holdings (Equity & MF usually appear here if in Demat)
        holdings = kite.holdings()
        df_holdings = pd.DataFrame(holdings) if holdings else pd.DataFrame()

        # 2. Fetch Trades/Orders (For FNO/Daily logic)
        # Note: 'orders' gives order book, 'trades' gives executed trades
        trades = kite.trades()
        df_trades = pd.DataFrame(trades) if trades else pd.DataFrame()
        
        # 3. Fetch Mutual Fund Holdings
        mf_holdings = kite.mf_holdings()
        df_mf_holdings = pd.DataFrame(mf_holdings) if mf_holdings else pd.DataFrame()
        
        return df_holdings, df_trades, df_mf_holdings, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), str(e)

def get_account_margins(kite):
    """
    Fetches margin data (cash_balance and opening_balance) from Kite API.
    
    Args:
        kite: Authenticated KiteConnect object
    
    Returns:
        dict: {"cash_balance": value, "opening_balance": value} or None if error
    """
    try:
        funds = kite.margins("equity")
        cash_balance = funds.get("available", {}).get("cash", 0)
        opening_balance = funds.get("available", {}).get("opening_balance", 0)
        return {"cash_balance": cash_balance, "opening_balance": opening_balance}
    except Exception as e:
        return None

def get_all_accounts_data():
    """
    Fetch data for all authenticated accounts.
    
    Returns:
        dict: {account_id: {'holdings': df, 'trades': df, 'mf_holdings': df, 'error': str}}
    """
    accounts_data = {}
    
    for account_id, kite in st.session_state['authenticated_accounts'].items():
        holdings, trades, mf_holdings, error = get_kite_data(kite)
        accounts_data[account_id] = {
            'holdings': holdings,
            'trades': trades,
            'mf_holdings': mf_holdings,
            'error': error
        }
    
    return accounts_data

def aggregate_accounts_data(accounts_data):
    """
    Aggregate holdings, trades, and MF holdings from all accounts.
    
    Args:
        accounts_data: Dict from get_all_accounts_data()
    
    Returns:
        tuple: (aggregated_holdings_df, aggregated_trades_df, aggregated_mf_holdings_df)
    """
    all_holdings = []
    all_trades = []
    all_mf_holdings = []
    
    for account_id, data in accounts_data.items():
        if data.get('error'):
            continue
            
        holdings = data.get('holdings', pd.DataFrame())
        trades = data.get('trades', pd.DataFrame())
        mf_holdings = data.get('mf_holdings', pd.DataFrame())
        
        if not holdings.empty:
            holdings['account_id'] = account_id
            all_holdings.append(holdings)
        
        if not trades.empty:
            trades['account_id'] = account_id
            all_trades.append(trades)
        
        if not mf_holdings.empty:
            mf_holdings['account_id'] = account_id
            all_mf_holdings.append(mf_holdings)
    
    aggregated_holdings = pd.concat(all_holdings, ignore_index=True) if all_holdings else pd.DataFrame()
    aggregated_trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    aggregated_mf_holdings = pd.concat(all_mf_holdings, ignore_index=True) if all_mf_holdings else pd.DataFrame()
    
    return aggregated_holdings, aggregated_trades, aggregated_mf_holdings

def update_spreadsheet_logic(creds_file, sheet_name):
    """
    Core logic to fetch data from all authenticated accounts and update the sheet.
    Creates separate worksheets per account_id.
    """
    # #region agent log
    import json
    try:
        with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"dash.py:225","message":"update_spreadsheet_logic entry","data":{"creds_file":creds_file,"sheet_name":sheet_name,"auth_accounts_count":len(st.session_state.get('authenticated_accounts',{}))},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
    except: pass
    # #endregion
    
    log_msg = f"[{datetime.datetime.now()}] Starting automated data fetch..."
    st.session_state['last_run_log'].insert(0, log_msg)
    
    if not st.session_state['authenticated_accounts']:
        # #region agent log
        try:
            with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"dash.py:234","message":"No authenticated accounts","data":{},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
        except: pass
        # #endregion
        st.session_state['last_run_log'].insert(0, "No authenticated accounts to sync")
        return
    
    # Fetch data for all authenticated accounts
    accounts_data = get_all_accounts_data()
    
    # #region agent log
    try:
        with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
            accounts_summary = {k: {"has_error":bool(v.get('error')),"holdings_shape":list(v.get('holdings',pd.DataFrame()).shape) if not v.get('holdings',pd.DataFrame()).empty else [0,0],"trades_shape":list(v.get('trades',pd.DataFrame()).shape) if not v.get('trades',pd.DataFrame()).empty else [0,0]} for k,v in accounts_data.items()}
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"C","location":"dash.py:238","message":"accounts_data fetched","data":{"accounts_count":len(accounts_data),"accounts_summary":accounts_summary},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
    except: pass
    # #endregion
    
    # Connect to GSheets
    sheet_obj = load_google_sheet(creds_file, sheet_name)
    
    # #region agent log
    try:
        with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"dash.py:241","message":"Google Sheets connection result","data":{"is_error":isinstance(sheet_obj,str),"error_msg":sheet_obj if isinstance(sheet_obj,str) else "success","sheet_obj_type":str(type(sheet_obj))},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
    except: pass
    # #endregion
    
    if isinstance(sheet_obj, str):  # Error happened
        st.session_state['last_run_log'].insert(0, f"Error GSheets: {sheet_obj}")
        return
    
    try:
        # Process each account's data
        for account_id, data in accounts_data.items():
            # #region agent log
            try:
                with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"C","location":"dash.py:248","message":"Processing account","data":{"account_id":account_id,"has_error":bool(data.get('error'))},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
            except: pass
            # #endregion
            
            if data.get('error'):
                st.session_state['last_run_log'].insert(0, f"⚠️ Skipping {account_id}: {data['error']}")
                continue
            
            df_holdings = data.get('holdings', pd.DataFrame())
            df_trades = data.get('trades', pd.DataFrame())
            df_mf_holdings = data.get('mf_holdings', pd.DataFrame())
            
            # #region agent log
            try:
                with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"C","location":"dash.py:255","message":"DataFrames before update","data":{"account_id":account_id,"holdings_empty":df_holdings.empty,"holdings_shape":list(df_holdings.shape),"trades_empty":df_trades.empty,"trades_shape":list(df_trades.shape)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
            except: pass
            # #endregion
            
            # Create account-specific worksheet names
            holdings_ws_name = f"Holdings_{account_id}"
            trades_ws_name = f"Trades_{account_id}"
            
            # Update Holdings Sheet for this account
            try:
                ws_holdings = sheet_obj.worksheet(holdings_ws_name)
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"dash.py:262","message":"Holdings worksheet found","data":{"account_id":account_id,"worksheet_name":holdings_ws_name},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
            except Exception as e:
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"dash.py:264","message":"Creating holdings worksheet","data":{"account_id":account_id,"worksheet_name":holdings_ws_name,"error":str(e)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                ws_holdings = sheet_obj.add_worksheet(
                    title=holdings_ws_name, rows="100", cols="20"
                )
            
            if not df_holdings.empty:
                # Filter Holdings to only include specified columns
                # First, calculate combined quantity (quantity + collateral_quantity)
                df_holdings_filtered = df_holdings.copy()
                
                # Calculate combined quantity, handling NaN values
                if 'quantity' in df_holdings_filtered.columns and 'collateral_quantity' in df_holdings_filtered.columns:
                    df_holdings_filtered['quantity'] = (
                        df_holdings_filtered['quantity'].fillna(0) + 
                        df_holdings_filtered['collateral_quantity'].fillna(0)
                    )
                elif 'quantity' in df_holdings_filtered.columns:
                    df_holdings_filtered['quantity'] = df_holdings_filtered['quantity'].fillna(0)
                elif 'collateral_quantity' in df_holdings_filtered.columns:
                    df_holdings_filtered['quantity'] = df_holdings_filtered['collateral_quantity'].fillna(0)
                
                # Calculate additional columns
                if 'quantity' in df_holdings_filtered.columns and 'average_price' in df_holdings_filtered.columns:
                    df_holdings_filtered['invested_amount'] = (
                        df_holdings_filtered['quantity'] * df_holdings_filtered['average_price'].fillna(0)
                    )
                
                if 'quantity' in df_holdings_filtered.columns and 'close_price' in df_holdings_filtered.columns:
                    df_holdings_filtered['current_amount'] = (
                        df_holdings_filtered['quantity'] * df_holdings_filtered['close_price'].fillna(0)
                    )
                
                # Select only required columns
                required_columns = ['tradingsymbol', 'quantity', 'average_price', 'close_price', 'pnl', 'day_change', 'invested_amount', 'current_amount']
                available_columns = [col for col in required_columns if col in df_holdings_filtered.columns]
                
                if available_columns:
                    df_holdings_filtered = df_holdings_filtered[available_columns]
                else:
                    st.session_state['last_run_log'].insert(
                        0, f"⚠️ No required columns found for {account_id} holdings"
                    )
                    continue
                
                # Fetch margins data and add cash_balance and opening_balance rows
                kite_obj = st.session_state['authenticated_accounts'].get(account_id)
                if kite_obj:
                    margins_data = get_account_margins(kite_obj)
                    if margins_data:
                        # Create balance rows with same columns as filtered holdings
                        balance_rows = []
                        
                        # Cash balance row
                        cash_row = {}
                        for col in available_columns:
                            if col == 'tradingsymbol':
                                cash_row[col] = 'cash_balance'
                            elif col == 'current_amount':
                                cash_row[col] = margins_data['cash_balance']
                            else:
                                cash_row[col] = 0
                        balance_rows.append(cash_row)
                        
                        # Opening balance row
                        opening_row = {}
                        for col in available_columns:
                            if col == 'tradingsymbol':
                                opening_row[col] = 'opening_balance'
                            elif col == 'current_amount':
                                opening_row[col] = margins_data['opening_balance']
                            else:
                                opening_row[col] = 0
                        balance_rows.append(opening_row)
                        
                        # Convert to DataFrame and append to holdings
                        df_balance_rows = pd.DataFrame(balance_rows)
                        df_holdings_filtered = pd.concat([df_holdings_filtered, df_balance_rows], ignore_index=True)
                
                # Convert complex objects to strings for Google Sheets compatibility
                df_holdings_clean = df_holdings_filtered.copy()
                for col in df_holdings_clean.columns:
                    # Convert datetime objects to strings
                    if df_holdings_clean[col].dtype == 'datetime64[ns]':
                        df_holdings_clean[col] = df_holdings_clean[col].astype(str)
                    # Convert object columns (may contain dicts, None, etc.) to strings
                    elif df_holdings_clean[col].dtype == 'object':
                        df_holdings_clean[col] = df_holdings_clean[col].apply(
                            lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else ''
                        )
                    # Convert NaN values to empty strings
                    df_holdings_clean[col] = df_holdings_clean[col].fillna('')
                
                # Prepare MF holdings for export (exclude specified columns)
                df_all_holdings = df_holdings_clean.copy()
                if not df_mf_holdings.empty:
                    # Filter out columns that should not be exported
                    columns_to_exclude = ['folio', 'pnl', 'xirr', 'tradingsymbol', 'pledged_quantity', 'las_quantity', 'last_price_date', 'discrepancy', 'account_id']
                    df_mf_holdings_filtered = df_mf_holdings.copy()
                    # Remove excluded columns if they exist
                    columns_to_keep = [col for col in df_mf_holdings_filtered.columns if col not in columns_to_exclude]
                    df_mf_holdings_filtered = df_mf_holdings_filtered[columns_to_keep]
                    
                    # Rename 'fund' column to 'tradingsymbol' if it exists
                    if 'fund' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered = df_mf_holdings_filtered.rename(columns={'fund': 'tradingsymbol'})
                    
                    # Rename 'last_price' column to 'close_price' if it exists
                    if 'last_price' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered = df_mf_holdings_filtered.rename(columns={'last_price': 'close_price'})
                    
                    # Calculate additional columns
                    if 'quantity' in df_mf_holdings_filtered.columns and 'average_price' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered['invested_amount'] = (
                            df_mf_holdings_filtered['quantity'] * df_mf_holdings_filtered['average_price'].fillna(0)
                        )
                    
                    if 'quantity' in df_mf_holdings_filtered.columns and 'close_price' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered['current_amount'] = (
                            df_mf_holdings_filtered['quantity'] * df_mf_holdings_filtered['close_price'].fillna(0)
                        )
                    
                    # Calculate P&L
                    if 'current_amount' in df_mf_holdings_filtered.columns and 'invested_amount' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered['pnl'] = (
                            df_mf_holdings_filtered['current_amount'] - df_mf_holdings_filtered['invested_amount']
                        )
                    
                    # Convert complex objects to strings for Google Sheets compatibility
                    df_mf_holdings_clean = df_mf_holdings_filtered.copy()
                    for col in df_mf_holdings_clean.columns:
                        # Convert datetime objects to strings
                        if df_mf_holdings_clean[col].dtype == 'datetime64[ns]':
                            df_mf_holdings_clean[col] = df_mf_holdings_clean[col].astype(str)
                        # Convert object columns (may contain dicts, None, etc.) to strings
                        elif df_mf_holdings_clean[col].dtype == 'object':
                            df_mf_holdings_clean[col] = df_mf_holdings_clean[col].apply(
                                lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else ''
                            )
                        # Convert NaN values to empty strings
                        df_mf_holdings_clean[col] = df_mf_holdings_clean[col].fillna('')
                    
                    # Combine equity holdings and MF holdings
                    # Align columns - add missing columns to each DataFrame with empty values
                    all_columns = list(set(df_holdings_clean.columns.tolist() + df_mf_holdings_clean.columns.tolist()))
                    
                    # Reindex both DataFrames to have all columns
                    df_holdings_aligned = df_holdings_clean.reindex(columns=all_columns, fill_value='')
                    df_mf_aligned = df_mf_holdings_clean.reindex(columns=all_columns, fill_value='')
                    
                    # Concatenate equity holdings and MF holdings
                    df_all_holdings = pd.concat([df_holdings_aligned, df_mf_aligned], ignore_index=True)
                
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run2","hypothesisId":"D","location":"dash.py:273","message":"Before holdings update (cleaned)","data":{"account_id":account_id,"rows":len(df_all_holdings),"cols":len(df_all_holdings.columns),"columns":list(df_all_holdings.columns)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                
                ws_holdings.clear()
                update_result = ws_holdings.update(
                    [df_all_holdings.columns.values.tolist()] + df_all_holdings.values.tolist()
                )
                
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"dash.py:276","message":"Holdings update result","data":{"account_id":account_id,"update_result":str(update_result)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                
                if not df_mf_holdings.empty:
                    st.session_state['last_run_log'].insert(
                        0, f"✅ Holdings and MF Holdings updated for {account_id}"
                    )
                else:
                    st.session_state['last_run_log'].insert(
                        0, f"✅ Holdings updated for {account_id}"
                    )
            else:
                # If no equity holdings, export only MF holdings if available
                if not df_mf_holdings.empty:
                    # Filter out columns that should not be exported
                    columns_to_exclude = ['folio', 'pnl', 'xirr', 'tradingsymbol', 'pledged_quantity', 'last_quantity', 'discrepancy', 'account_id']
                    df_mf_holdings_filtered = df_mf_holdings.copy()
                    # Remove excluded columns if they exist
                    columns_to_keep = [col for col in df_mf_holdings_filtered.columns if col not in columns_to_exclude]
                    df_mf_holdings_filtered = df_mf_holdings_filtered[columns_to_keep]
                    
                    # Rename 'fund' column to 'tradingsymbol' if it exists
                    if 'fund' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered = df_mf_holdings_filtered.rename(columns={'fund': 'tradingsymbol'})
                    
                    # Rename 'last_price' column to 'close_price' if it exists
                    if 'last_price' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered = df_mf_holdings_filtered.rename(columns={'last_price': 'close_price'})
                    
                    # Calculate additional columns
                    if 'quantity' in df_mf_holdings_filtered.columns and 'average_price' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered['invested_amount'] = (
                            df_mf_holdings_filtered['quantity'] * df_mf_holdings_filtered['average_price'].fillna(0)
                        )
                    
                    if 'quantity' in df_mf_holdings_filtered.columns and 'close_price' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered['current_amount'] = (
                            df_mf_holdings_filtered['quantity'] * df_mf_holdings_filtered['close_price'].fillna(0)
                        )
                    
                    # Calculate P&L
                    if 'current_amount' in df_mf_holdings_filtered.columns and 'invested_amount' in df_mf_holdings_filtered.columns:
                        df_mf_holdings_filtered['pnl'] = (
                            df_mf_holdings_filtered['current_amount'] - df_mf_holdings_filtered['invested_amount']
                        )
                    
                    # Convert complex objects to strings for Google Sheets compatibility
                    df_mf_holdings_clean = df_mf_holdings_filtered.copy()
                    for col in df_mf_holdings_clean.columns:
                        # Convert datetime objects to strings
                        if df_mf_holdings_clean[col].dtype == 'datetime64[ns]':
                            df_mf_holdings_clean[col] = df_mf_holdings_clean[col].astype(str)
                        # Convert object columns (may contain dicts, None, etc.) to strings
                        elif df_mf_holdings_clean[col].dtype == 'object':
                            df_mf_holdings_clean[col] = df_mf_holdings_clean[col].apply(
                                lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else ''
                            )
                        # Convert NaN values to empty strings
                        df_mf_holdings_clean[col] = df_mf_holdings_clean[col].fillna('')
                    
                    ws_holdings.clear()
                    ws_holdings.update(
                        [df_mf_holdings_clean.columns.values.tolist()] + df_mf_holdings_clean.values.tolist()
                    )
                    
                    st.session_state['last_run_log'].insert(
                        0, f"✅ MF Holdings updated for {account_id}"
                    )
                else:
                    st.session_state['last_run_log'].insert(
                        0, f"ℹ️ No holdings data for {account_id}"
                    )

            # Update Trades Sheet for this account
            try:
                ws_trades = sheet_obj.worksheet(trades_ws_name)
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"dash.py:286","message":"Trades worksheet found","data":{"account_id":account_id,"worksheet_name":trades_ws_name},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
            except Exception as e:
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"dash.py:288","message":"Creating trades worksheet","data":{"account_id":account_id,"worksheet_name":trades_ws_name,"error":str(e)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                ws_trades = sheet_obj.add_worksheet(
                    title=trades_ws_name, rows="100", cols="20"
                )

            if not df_trades.empty:
                # Ensure account_id column exists
                if 'account_id' not in df_trades.columns:
                    df_trades['account_id'] = account_id
                
                # Convert complex objects to strings for Google Sheets compatibility
                df_trades_clean = df_trades.copy()
                for col in df_trades_clean.columns:
                    # Convert datetime objects to strings
                    if df_trades_clean[col].dtype == 'datetime64[ns]':
                        df_trades_clean[col] = df_trades_clean[col].astype(str)
                    # Convert object columns (may contain dicts, None, etc.) to strings
                    elif df_trades_clean[col].dtype == 'object':
                        df_trades_clean[col] = df_trades_clean[col].apply(
                            lambda x: str(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else ''
                        )
                    # Convert NaN values to empty strings
                    df_trades_clean[col] = df_trades_clean[col].fillna('')
                
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run2","hypothesisId":"D","location":"dash.py:293","message":"Before trades update (cleaned)","data":{"account_id":account_id,"rows":len(df_trades_clean),"cols":len(df_trades_clean.columns),"columns":list(df_trades_clean.columns)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                
                ws_trades.clear()
                update_result = ws_trades.update(
                    [df_trades_clean.columns.values.tolist()] + df_trades_clean.values.tolist()
                )
                
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"D","location":"dash.py:298","message":"Trades update result","data":{"account_id":account_id,"update_result":str(update_result)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                
                st.session_state['last_run_log'].insert(
                    0, f"✅ Trades updated for {account_id}"
                )
            else:
                st.session_state['last_run_log'].insert(
                    0, f"ℹ️ No trades data for {account_id}"
                )
        
        # #region agent log
        try:
            with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"dash.py:310","message":"All accounts processed successfully","data":{},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
        except: pass
        # #endregion
        
        st.session_state['last_run_log'].insert(0, "✅ All accounts synced successfully")
            
    except Exception as e:
        # #region agent log
        try:
            with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"dash.py:315","message":"Exception caught","data":{"error_type":type(e).__name__,"error_msg":str(e)},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
        except: pass
        # #endregion
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

# Auto-authenticate accounts with valid access_tokens on startup (only once)
if not st.session_state['auto_auth_attempted'] and st.session_state['accounts_config']:
    st.session_state['auto_auth_attempted'] = True
    for account in st.session_state['accounts_config']:
        account_id = account.get('account_id')
        access_token = account.get('access_token', '')
        
        # Try to authenticate with existing access_token
        if access_token and access_token.strip():
            kite_obj, status_msg = authenticate_account(
                account, 
                try_access_token_first=True
            )
            if kite_obj:
                st.session_state['authenticated_accounts'][account_id] = kite_obj
                st.session_state['last_run_log'].insert(
                    0, f"[{datetime.datetime.now()}] Auto-connected: {account_id}"
                )
            else:
                # Access token invalid, log but don't show error (user can reconnect)
                st.session_state['last_run_log'].insert(
                    0, f"[{datetime.datetime.now()}] Auto-connect failed for {account_id}: {status_msg}"
                )

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
                
                # Always try access_token first; if it fails, use request_token
                kite_obj, status_msg = authenticate_account(
                    account, 
                    user_req_token if user_req_token else None,
                    try_access_token_first=True
                )
                
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
    holdings, trades, mf_holdings = aggregate_accounts_data(accounts_data)
    
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
                
                st.subheader("Mutual Fund Holdings (All Accounts)")
                if not mf_holdings.empty:
                    st.dataframe(mf_holdings, width='stretch')
                else:
                    st.info("No mutual fund holdings data available")
                
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
                    account_mf_holdings = account_data.get('mf_holdings', pd.DataFrame())
                    
                    st.subheader(f"Holdings - {account_id}")
                    if not account_holdings.empty:
                        st.dataframe(account_holdings, width='stretch')
                    else:
                        st.info(f"No holdings data for {account_id}")
                    
                    st.subheader(f"Mutual Fund Holdings - {account_id}")
                    if not account_mf_holdings.empty:
                        st.dataframe(account_mf_holdings, width='stretch')
                    else:
                        st.info(f"No mutual fund holdings data for {account_id}")
                    
                    st.subheader(f"Trades - {account_id}")
                    if not account_trades.empty:
                        st.dataframe(account_trades, width='stretch')
                    else:
                        st.info(f"No trades data for {account_id}")
                    
                    st.divider()
            
            if st.button("Force Sync to Google Sheet Now"):
                # #region agent log
                import json
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"F","location":"dash.py:590","message":"Force Sync button clicked","data":{"gsheet_json":gsheet_json,"gsheet_name":gsheet_name},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
                with st.spinner("Uploading to Drive..."):
                    update_spreadsheet_logic(gsheet_json, gsheet_name)
                # #region agent log
                try:
                    with open(r'c:\Users\Ananth\Documents\GitHub\zerodha_dashboard\.cursor\debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"F","location":"dash.py:593","message":"Force Sync completed","data":{},"timestamp":int(datetime.datetime.now().timestamp()*1000)}) + '\n')
                except: pass
                # #endregion
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
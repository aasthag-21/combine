
import pandas as pd
import logging
import os
import sys
import json
import datetime

if __name__ == "__main__":
    logging.basicConfig(
        format="%(lineno)d %(levelname)s: %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    LOGGER = logging.getLogger(__name__)

    rename_cols = {
        "Company": "Company",
        "Side": "Order Type",
        "Order Type": "NPM Order Type",
        "Size Range": "Range",
        "Price": "Price",
        "Implied Val(bn)+": "Valuation",
        "Share Class": "Share Class",
        "Notes": "Comments"
    }
    selected_cols = [k for k in rename_cols]

    convert_cols = {
        "Company": str,
        "Side": str,
        "NPM Order Type": str,
        "Range": str,
        "Price": str,
        "Implied Val(bn)+": str,
        "Share Class": str,
        "Notes": str
    }

    new_cols = {
        "Size": "NULL",
        "Date Added": datetime.datetime(year=2023, month=8, day=21).strftime("%Y-%m-%d"),
        "Source ID": 5,
        "ROFR": "NULL",
        "Mgt": "NULL",
        "Carry": "NULL",
        "Acct Manager": "13377",
        "Investment Format": "NULL"
    }

    size_range_mapper = {
        "<$1mm": "<$1 mn",
        "$1-$5mm": "$1 mn - $5 mn",
        "$1-$10mm": "$5 mn - $10 mn",
        "$5-10mm": "$5 mn - $10 mn",
        "$5-$10mm": "$5 mn - $10 mn",
        "$10-$25mm": "$10 mn - $25 mn",
        "$35-$50mm": "$25 mn - $100 mn",
        "$25-$50mm":  "$25 mn - $100 mn",
        "$25-$100mm": "$25 mn - $100 mn"
    }

    side_order_type_mapper = {
        "Seller": "Sell",
        "Buyer": "Buy"
    }
  
  
    df = pd.read_excel("2023-08-29.xlsx", sheet_name="raw", converters=convert_cols, na_filter=False,
                       na_values=['#N/A', 'N/A', '#NA', '<NA>'])
    


    df = df.loc[:, selected_cols]
    df = df.rename(columns=rename_cols)
    df = df.fillna("NULL")
    for new in new_cols:
        df[new] = new_cols[new]
    for index, row in df.iterrows():
      
        if row["Order Type"].strip():
            df.loc[index, "Order Type"] = side_order_type_mapper[row["Order Type"]]
            df.loc[index, "Range"] = size_range_mapper[row["Range"]]

    company_list=[]
    for index, row in df.iterrows():
        company_list.append(row["Company"])
    

    import psycopg2

    #establishing the connection
    conn = psycopg2.connect(database="gatewayproductiondb", user='postgres_ro', password='Gatewaytech123$%^', host='prod-rds-gateway-01.cnygjz9bfs5k.ap-southeast-1.rds.amazonaws.com', port= '5432')
    cursor = conn.cursor()

    #Executing an MYSQL function using the execute() method
    cursor.execute("select version()")

    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
  
    try:
        with conn.cursor() as cursor:
            # Read data from database
            sql = "SELECT id, name FROM investments_company"
            cursor.execute(sql)

            # Fetch all rows
            x_rows = cursor.fetchall()

            # Print results
            source_list=[]
            for x_row in x_rows:
                source_list.append(x_row)
            
        
    finally:
        conn.close()


    mapped_list= []
    for company in company_list:
      
        for source in source_list: 
            
            if company==source[-1]:
                
                mapped_list.append([source[0], company])
               

   
    mapped_df= pd.DataFrame(mapped_list, columns=["source_id","Company"])
 
    merged_df = df.merge(mapped_df, on='Company', how="outer").fillna("NA")

    merged_df.to_excel(f"NPM Orderflow SQL {datetime.date.today().strftime('%Y-%m-%d')}.xlsx", sheet_name="new")
    
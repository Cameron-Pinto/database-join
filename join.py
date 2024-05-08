import pandas as pd
import pyodbc as odbc
from pathlib import Path


# Connection to first SQL Database with credentials
conn1 = odbc.connect(
    '''
    Driver={SQL Server Native Client 11.0};
    Server=SSQL;
    Database=****;
    UID=****;
    PWD=******;
    '''
)

# Connection to second SQL Database with credentials
conn2 = odbc.connect(
    '''
    Driver={SQL Server Native Client 11.0};
    Description=****;
    Server=*****;
    Database=****;
    trusted_connection=Yes
    '''
)

# Quering the first database using SQL
Query1 = pd.read_sql_query(
    '''
    SELECT
    TRIM I.FMTITEMNO as FMTITEMNO,
    I.DESC,
    I.STOCKUNIT,
    PACK_OF =
    CASE
    WHERE LOWER(I.DESC) LIKE LOWER('%2 per box%') THEN '2'
    ELSE '1'
    END,
    CUSTOMER_PRICE =
    CASE
    WHEN I.CATEGORY ='10' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='11' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='12' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='14' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='15' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='16' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='17' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='18' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='19' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='20' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='21' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='23' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='25' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='31' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='37' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)
    WHEN I.CATEGORY ='22' THEN P.UNITPRICE - (P.UNITPRICE * 58/100)
    WHEN I.CATEGORY ='57' THEN P.UNITPRICE - (P.UNITPRICE * 55/100)

    ELSE P.UNITPRICE
    END

    FROM dbo.ICITEM AS I
    JOIN dbo.ICPRICP AS P ON I.ITEMNO=P.ITEMNO
    WHERE

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='14' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='16'  OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='20'OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='22' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='10' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='11' OR

    I.ALLOWONWEB=1 AMD P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='12' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='21' OR

    I. ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='23' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='57' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='15' OR

    I.ALLOWONWEB=1 AND P.DPRICETYPE=1
    AND P.PRICELIST='PL01' AND P.UNITPRICE<>0
    AND I.CATEGORY='19'

    ORDER BY I.FMTITEMNO

    ''',
    conn1,
)

# Quering second database using SQL
Query2 = pd.read_sql_query(
    '''
    SELECT
    V.Warehouse,
    V.Product,
    SUM(V.Unalloc_Quantity) as Total,
    Stock =
    CASE
    WHEN SUM(V.Unalloc_Quantity) > 10 THEN '10'
    WHEN SUM(V.Unalloc_Quantity) < 3 THEN '0'
    ELSE SUM(V.Unalloc_Quantity)
    END

    FROM dbo.EdgeInventoryView AS V

    WHERE V.Warehouse='CA'

    GROUP BY V.Warehouse, V.Product

    ''',
    conn2,
)

# Converting to datframes and merging on column named stock
df1 = pd.DataFrame(Query1)
df2 = pd.DataFrame(Query2)
merged_df = pd.merge(df1, df2, left_on="FMTITEMNO", right_on="Product")["Stock"]


# Converting file to .csv and setting the export of file path
mereged_file= merged_df.to_csv("Promax.csv", index=False)
file_path = ''

# Uploading file to Filezilla Automatically
# file_path = Path('Promax.csv')

# with FTP('paftp.ca', 'maxpro@paftp.ca', 'PWD') as ftp, open(file_path, 'rb') as file:
# ftp.storbinary(f'STOR {file_path.name}', file)

# csv-to-sql-convertor
A fast simple csv to SQL Table convertor
how to run the script
```
python3 csvsql.py sampleSheet1.csv  --HOST 'localhost' --USER first --PASS user --DATABASE mydatabase --TABLE_NAME Order_report
```
Do Not forget to install dependencies

### Result
```
Name                 DataType
-------------------  -------------
Date                 DATE
Month                varchar(1000)
Month_num            FLOAT
Order_Number         FLOAT
User_ID              varchar(1000)
Locality             varchar(1000)
Product_ID           varchar(1000)
XXXX                 varchar(1000)
XXXXXXX              varchar(1000)
Brand                varchar(1000)
XXXXXXXXXXX          varchar(1000)
Qty                  FLOAT
Unit                 varchar(1000)
SP                   FLOAT
MRP                  FLOAT
Order_Qty            FLOAT
Ordered_Value_SP     FLOAT
Ordered_Value_MRP    FLOAT
Status               varchar(1000)
Delivered_Qty        FLOAT
Purchase_Price       FLOAT
Channel              varchar(1000)
City                 varchar(1000)
3it [00:11,  3.73s/it]
Number of Rows iterrated 27392
```

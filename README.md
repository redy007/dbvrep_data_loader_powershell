# dbvrep_data_loader_powershell

Script will make initial load on MS SQL Server within DBVisit replicate software - https://dbvisit.atlassian.net/wiki/spaces/ugd9/pages/128741717/Target+Database+SQL+Server. You can use it to refresh your replication or add new tables.

The script will create tables on SQL Servers and add indexes. Then will load a data.

Before you'll use the script for first time you must have created replication and executed *.all.bat script.

  The script uses:
  - sqlplus binary
  - tns names
  - sqlcmd binary
  - odbc driver for the dbvisit replication



RUN The script within MINE or APPLY process example:<br />
C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh<br /> 
C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -external_import<br />
C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh -prepare_tabs prepare_tabs.txt<br />

sql_server_id = SQL Server admin<br />
sql_server_passwd = SQL Server admin password<br />
system_password = SYSTEM's password<br />

refresh - Do you want start from scratch<br />
external_import - Do you want use 3rd party tool to load data to SQL Server<br />
prepare_tabs - Where are stored table to use by DBVisit Replicate<br />

prepare_tabs.txt content example:
=================================
SOE.CUSTOMERS<br />
SOE.ADDRESSES<br />
SOE.CARD_DETAILS<br />
SOE.WAREHOUSES<br />
SOE.ORDER_ITEMS<br />
SOE.ORDERS<br />
SOE.INVENTORIES<br />
SOE.PRODUCT_INFORMATION<br />
SOE.LOGON<br />
SOE.PRODUCT_DESCRIPTIONS<br />
SOE.ORDERENTRY_METADATA<br />
SOE.DBVISITTEST<br />
SOE.DBVISITTEST2<br />

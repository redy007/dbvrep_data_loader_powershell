# dbvrep_data_loader_powershell

Script will make initial load on MS SQL Server within DBVisit replicate software - https://dbvisit.atlassian.net/wiki/spaces/ugd9/pages/128741717/Target+Database+SQL+Server. You can use it to refresh your replication or add new tables.

The script will create tables on SQL Servers and add indexes. Then will load a data.

Before you'll use the script for first time you must have created replication and executed *.all.bat script.

  The script uses:
  - sqlplus binary
  - tns names
  - sqlcmd binary
  - odbc driver for the dbvisit replication



RUN The script within MINE or APPLY process:
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh 
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -external_import
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh -prepare_tabs prepare_tabs.txt




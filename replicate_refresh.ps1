<#
.Synopsis
   Refresh DBVREP enviromnemt for SQL Server.

.DESCRIPTION
   I created this script for easy recovery of dbvisit replicate replication after 
   changing the data to the source database. Dbvisit replicate does not support 
   DDL replication on a non-oracle database. You can use this script when you 
   change the structure of the tables or refresh the database.

   The script uses pre-replication set values. Modified parameters I recommend to 
   have stored in DDC files. If your own settings are stored in repositories, 
   you will lose them.

   The script initially requires the most critical parameters. The rest is looking 
   for himself. First stops the original replication. Delete and re-create 
   repostors using the * -all.bat script. Launches mine and apply service. 
   Then wait until the apply process is at the same level as mine. In this section, 
   the script proceeds to prepare the tables. From the target database, it discovers 
   the SCN number and passes it to the user for the instance of the data. The script 
   waits for the user and his instance of data, and then goes through replication 
   and excludes not supported columns.

 .NOTES
  version 1.0

  The script uses:
  - sqlplus binary
  - tns names
  - sqlcmd binary
  - odbc driver for the dbvisit replication
  - prepare_tabs.txt file where is defined what should tables will be prepared. 
  	The content of the file eg:
  		msi.first_table
  		msi.second_table
  		...

  version 1.1

  You can run the script from destionat server too. 

  version 2.0
  
  In version 2.0 you can add new tables only to you replication. The tool still use
  SSMA to inistatiate data. The prepare is default settings. To use refresh you need
  to set parameter -refresh.

  version 3.0

  Version 3.0 supports DDL creation on destination database and import a data to the table. Unsupported columns 
  are excluded automatically and these data aren't loaded either. You can choose to use SSMA or any different 
  tool by using variable external_import. Indexes and constraints are created too.


.EXAMPLE
RUN The script from MINE process:
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -rename_schema msi.dbo -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh 
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -rename_schema msi.dbo -sql_server_id sa -sql_server_passwd sa -system_password oracle -external_import
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -rename_schema msi.dbo -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh -prepare_tabs prepare_tabs.txt


#>

param (
    [string]$rename_schema = $( Read-Host "Rename SCHEMA name: [w420g.dbo]" ),
#    [string]$ComputerName = $env:computername,    
	[string]$sql_server_id = $( Read-Host "User ID for SQL Server connection [dbvrep variable APPLY.APPLY_USER (default SA user)]" ),
    [string]$sql_server_passwd = $(Read-Host -asSecureString "Password for SQL Server connection [dbvrep variable APPLY.APPLY_USER (default SA user)]"),
    [string]$system_password = $( Read-Host -asSecureString "Input password for System user on the source database" ),
    [string]$prepare_tabs = "prepare_tabs.txt",
    [switch]$refresh = $false,
    [switch]$external_import = $false
)


Start-Transcript

function stopDbvrepServices($s, $adress)    
{
	cd $adress  
	$tempvar= wmic service $s.name get PathName
	$dbvrepexe = ($tempvar[2] -split "(?!^)(?=--ddcfile)")[0].Trim()
	$dbvrepexe = $dbvrepexe.Replace("`"","")
	$ddcFile = Get-ChildItem -Filter *ddc
	$ddcFile = $ddcFile[0].Name
	# risk the second process is running
	# user will find it out when all.bat script fail.
	if (Get-Service -Name $s.Name| Where-Object {$_.Status -eq "Running"}) {
		& $dbvrepexe --ddcfile $ddcFile shutdown all
		
		Start-Sleep -Seconds 10
		if (Get-Service -Name $s.Name| Where-Object {$_.Status -eq "Running"}) {
			Write-Host "Check the output from the log file for errors. You need to fix it before you can continue."
			cmd /c pause | out-null
			exit 1
		} 
	}

	$vysledek=Get-ChildItem -Filter *batch.bat
	$v=$adress + $vysledek.Name
	$last_line = Get-Content $v -Tail 1
	if ( $last_line.equals("pause")) {
		# Read all lines
		$LinesInFile = [System.IO.File]::ReadAllLines($v)
		# Write all lines, except for the last one, back to the file
		[System.IO.File]::WriteAllLines($v,$LinesInFile[0..($LinesInFile.Count - 2)])
		# Clean up
		Remove-Variable -Name LinesInFile
	}


	$vysledek=Get-ChildItem -Filter *-all.bat
	$v=$adress + $vysledek.Name
	& $v

	$allLog=Get-ChildItem -Filter *-all.log
	if (Get-Content $allLog -Tail 1 |  Where-Object {$_ -match "Error encountered, not starting Dbvisit Replicate."}) { 
		cat $allLog
		Write-Host "Check the output from the log file for errors. Perhaps there is running process on background. Kill them and repeate the script"
		cmd /c pause | out-null
		exit 1
	}


	if ($s.Name.split("_")[1].ToUpper() -eq "MINE") {
		# it's the mine server where script is started
		$target= & $dbvrepexe --ddcfile $ddcFile show APPLY_REMOTE_INTERFACE| Select-String -Pattern ^APPLY
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')).start()
		(Get-Service -Name $s.Name).start()
	} else {
		# it's the apply server where script is started
		$target= & $dbvrepexe --ddcfile $ddcFile show MINE_REMOTE_INTERFACE| Select-String -Pattern ^MINE
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('APPLY','MINE')).start()
		(Get-Service -Name $s.Name).start()
	}

	Write-Host
	Write-Host
	Write-Host
	Write-Host wait 2m for sync both processes
	Start-Sleep -Seconds 120

	$xPLOG= & $dbvrepexe --ddcfile $ddcFile list status| Select-String -Pattern ^MINE
	$MPLOG= $xPLOG -split ' '
	$numCycle = 0;

	Do {
		Write-Host wait 1m for sync both processes
		Start-Sleep -Seconds 60
		$xPLOG= & $dbvrepexe --ddcfile $ddcFile list status| Select-String -Pattern ^APPLY
		$APLOG= $xPLOG -split ' '
		$numCycle++
		if ($numCycle.equals(5)) {
			Write-Host "APPLY process stuck. Check apply log file or SQL Server for locks."
			exit 1 
		}

	}
	Until ($MPLOG[6] -le $APLOG[6])
	Write-Host APPLY process is up to date.
}

function loadTheTable($username, $password, $data_source, $oracle_schema, $table, $database, $schema, $dbvrep_db_apply, $FLASHBACK_SCN, $dbvrep_user_apply, $sql_server_passwd) 
{
	add-type -AssemblyName System.Data.OracleClient
	$result = New-Object System.Collections.ArrayList
	$connection_string = "User Id=$username;Password=$password;Data Source=$data_source"
	#tabulka
	#$table="tblTest"
	#potrebuji databazi // $database
	#$database = 'MSI'
	#potrebuji schema (dbo) // $schema
	#$schema = 'dbo'
	#pro create table radek

	#$COLUMN_NAME = New-Object System.Collections.ArrayList
	#$DATA_TYPE = New-Object System.Collections.ArrayList
	#$NULLABLE = New-Object System.Collections.ArrayList

	# oracle list of data types arraylist
	$oracleDataType = 'NUMBER','FLOAT','NVARCHAR2','VARCHAR2','VARCHAR','CHAR','NCHAR','DATE','RAW','LONG','LONG RAW','BFILE','CLOB','BLOB','LOB','NCLOB','INTERVAL','TIMESTAMP','BINARY_FLOAT','BINARY_DOUBLE','ROWID','XML'
	# sql server list of data types arraylist
	$sqlDataType = 'DECIMAL','DECIMAL','VARCHAR','VARCHAR','VARCHAR','CHAR','CHAR','DATETIME','VARBINARY','TEXT','VARBINARY(MAX)','x','x','x','x','x','x','DATETIME2','x','x','x','x'

	$statement = "select COLUMN_NAME, DATA_TYPE, DECODE(NULLABLE,'N', 'NOT NULL', 'Y', ' '), NVL(DATA_PRECISION, 0), NVL(DATA_SCALE, 0), NVL(DATA_LENGTH, 0) from ALL_TAB_COLS where TABLE_NAME = upper('$table') and OWNER = upper('$oracle_schema') order by COLUMN_ID"
	$col_name = $null
	$dtype = $null
	$null_able = $null
	$DATA_PRECISION = $null
	$DATA_SCALE = $null
	$DATA_LENGTH = $null
	#$DDL = [System.Text.StringBuilder]::new()
	$DDL = New-Object System.Collections.Generic.List[System.Object]

	$col_name = @()
	$dtype = @()
	$null_able = @()
	$DATA_PRECISION = @()
	$DATA_SCALE = @()
	$DATA_LENGTH = @()

	# slouzi array pro select v sqlbukcopy casti
	$SELECT_COLUMNS_NAME = @()


	try {
	    $con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
	    $con.Open()
	    $cmd = $con.CreateCommand()
	    $cmd.CommandText = $statement
	    $result = $cmd.ExecuteReader()
	    #print do souboru
	    while ($result.Read()) {
	    	#$result.FieldCount
	    	$col_name += $result.GetString(0)
	    	$dtype += $result.GetString(1)
	    	$null_able += $result.GetString(2)
	    	$DATA_PRECISION += 	$result.GetValue(3)
	    	$DATA_SCALE += $result.GetValue(4)
	    	$DATA_LENGTH += $result.GetValue(5)
	    }
	} catch {
	    Write-Error ("Database Exception: {0}`n{1}" -f `
	        $con.ConnectionString, $_.Exception.ToString())
	}

	$SELECT_COLUMNS_NAME = ""
	"create table $database.$schema.$table (" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	$DDL.Add("create table $database.$schema.$table (")
	For ($I=0;$I -lt $col_name.count;$I++) {
		$mapNumber = $oracleDataType.indexof($dtype[$I])
		$DATA_TYPE_SQL = $sqlDataType[$mapNumber]
		if ($DATA_TYPE_SQL -eq "DECIMAL" -AND $DATA_SCALE[$I] -eq '0') {
			if ($DATA_PRECISION[$I] -ne '0') {
				$CURR_DATA_PRECISION = $DATA_PRECISION[$I]
				$DATA_TYPE_SQL = "BIGINT" + "($CURR_DATA_PRECISION)"
			}
			else {
				$DATA_TYPE_SQL = "BIGINT"
			}
		}
		# stanovani precision a scale pro numbers
		elseif ($DATA_TYPE_SQL -eq "DECIMAL" -AND ($DATA_SCALE[$I] -ne '0' -OR $DATA_PRECISION[$I] -ne '0')) {
			$CURR_DATA_SCALE = $DATA_SCALE[$I]
			$CURR_DATA_PRECISION = $DATA_PRECISION[$I]
		    $DATA_TYPE_SQL = $DATA_TYPE_SQL+"($CURR_DATA_PRECISION,$CURR_DATA_SCALE)"
		} 
		# stanovani precision pro string (budu muset vsechny vypsat)
		elseif (($DATA_TYPE_SQL -eq "VARCHAR" -OR $DATA_TYPE_SQL -eq "CHAR" -OR $DATA_TYPE_SQL -eq "RAW") -AND ($DATA_LENGTH[$I] -ne '0')) {
			$CURR_DATA_LENGTH = $DATA_LENGTH[$I];
			$DATA_TYPE_SQL = $DATA_TYPE_SQL+"($CURR_DATA_LENGTH)"
		}
		elseif ($DATA_TYPE_SQL -eq "x" -OR $DATA_TYPE_SQL -eq "-1") {
		    continue
		}
		$COLUMN_NAME = $col_name[$I]
		$SELECT_COLUMNS_NAME += ", " + $COLUMN_NAME
		$nullable = $null_able[$I].trim()
		#"$COLUMN_NAME $DATA_TYPE_SQL $nullable,"| Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
		$DDL.Add("$COLUMN_NAME $DATA_TYPE_SQL $nullable,")
	}
	#');' | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
	$DDL.Add(");")
	$SELECT_COLUMNS_NAME = $SELECT_COLUMNS_NAME.trim(",", " ")

	#"" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
	$rows_pk = ""
	$statement = "select COLUMN_NAME from ALL_CONS_COLUMNS where CONSTRAINT_NAME = (select CONSTRAINT_NAME from ALL_CONSTRAINTS where CONSTRAINT_TYPE = 'P' and OWNER = upper('$oracle_schema') and TABLE_NAME= upper('$table')) order by POSITION"

	$con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
	$con.Open()
	$cmd = $con.CreateCommand()
	$cmd.CommandText = $statement
	$result = $cmd.ExecuteReader()
	while ($result.Read()) {
		$rows_pk += ", " + $result.GetString(0)
	}
	if ($result.HasRows) {
	$rows_pk = $rows_pk.trim(",", " ")
	#"ALTER TABLE $database.$schema.$table ADD PRIMARY KEY ($rows_pk);"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	$DDL.Add("ALTER TABLE $database.$schema.$table ADD PRIMARY KEY ($rows_pk);")
	}

	#jit cestou konkretniho jmena nebo nechat na sql serveru?
	#pro ted radeji nechat na sql serveru
	#do budoucna resit na zaklade stringu Oracle (syscol...), tak sql server vytvorit jmeno
	#kdyz unikatni, pak prenesu
	#ALTER TABLE $table ADD CONSTRAINT $statement_pk_CONSTRAINT_NAME PRIMARY KEY ($con_column_name );

	$rows_uk = ""
	$constraint_name = $null
	$statement = "select COLUMN_NAME,CONSTRAINT_NAME from ALL_CONS_COLUMNS where CONSTRAINT_NAME = (select CONSTRAINT_NAME from ALL_CONSTRAINTS where CONSTRAINT_TYPE = 'U' and OWNER = 'MSI' and TABLE_NAME= upper('$table')) order by CONSTRAINT_NAME, POSITION"

	$con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
	$con.Open()
	$cmd = $con.CreateCommand()
	$cmd.CommandText = $statement
	$result = $cmd.ExecuteReader()
	if ($result.HasRows) {
		while ($result.Read()) {
			if ($constraint_name -ne $result.GetString(1) -AND ($constraint_name -ne $null)) {	
				$rows_uk = $rows_uk.trim(",", " ")
				$DDL.Add("ALTER TABLE $database.$schema.$table ADD UNIQUE ($rows_uk);")
				$constraint_name = $result.GetString(1)	
				$rows_uk = ""
				$rows_uk += ", " + $result.GetString(0)
			}
			elseif (($constraint_name -ge $result.GetString(1)) -OR ($constraint_name -eq $null)) {
			    $rows_uk += ", " + $result.GetString(0)
			}
			$constraint_name = $result.GetString(1)
		}
		$rows_uk = $rows_uk.trim(",", " ")
	#"ALTER TABLE $database.$schema.$table ADD UNIQUE ($rows_uk);"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	$DDL.Add("ALTER TABLE $database.$schema.$table ADD UNIQUE ($rows_uk);")
	}

	$statement = "SELECT ui.COLUMN_NAME, ui.index_name FROM ALL_IND_COLUMNS ui, all_constraints uc WHERE ui.table_name = uc.table_name AND ui.table_name = '$table' AND ui.index_name NOT IN (SELECT uc.index_name FROM all_constraints uc WHERE constraint_type IN ('P','U') AND uc.table_name = '$table' ) order by ui.index_name, ui.COLUMN_POSITION"
	$index_name = $null
	$rows_idx = ""

	$con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
	$con.Open()
	$cmd = $con.CreateCommand()
	$cmd.CommandText = $statement
	$result = $cmd.ExecuteReader()
	if ($result.HasRows) {
		while ($result.Read()) {
			if ($index_name -ne $result.GetString(1) -AND ($index_name -ne $null)) {	
				$rows_idx = $rows_idx.trim(",", " ")
				$DDL.Add("CREATE INDEX $index_name ON $table ($rows_idx);")
				$index_name = $result.GetString(1)	
				$rows_idx = ""
				$rows_idx += ", " + $result.GetString(0)
			}
			elseif (($index_name -ge $result.GetString(1)) -OR ($index_name -eq $null)) {
			    $rows_idx += ", " + $result.GetString(0)
			}
			$index_name = $result.GetString(1)
		}
		$rows_idx = $rows_idx.trim(",", " ")
		$DDL.Add("CREATE INDEX $index_name ON $database.$schema.$table ($rows_idx);")
	}


	$SQL_SERVER_DB = (Get-OdbcDsn -Name $dbvrep_db_apply).Attribute["Server"]
	$sqlconn = "server=$SQL_SERVER_DB;database=$database;uid=$dbvrep_user_apply;pwd=$sql_server_passwd;"
	$sqlconn = new-object system.data.sqlclient.SqlConnection($sqlconn);
	$sqlconn.Open();
	$cmd = New-object System.Data.SqlClient.SqlCommand;
	$cmd.Connection = $sqlconn;
	$cmd.CommandText = $DDL
	$rows = $cmd.ExecuteNonQuery();
	$FLASHBACK_SCN = $FLASHBACK_SCN.trim(" ")
	$statement = "select $SELECT_COLUMNS_NAME from $oracle_schema.$table as of SCN $FLASHBACK_SCN"

	try{
	    $con = New-Object System.Data.OracleClient.OracleConnection($connection_string)

	    $con.Open()
	    #$adapter = new-object Oracle.ManagedDataAccess.Client.OracleDataAdapter($statement, $connection_string);
	    #$dtbl = new-object System.Data.DataTable('tblTest');
	    #$adapter.Fill($dtbl);
	    $cmd = $con.CreateCommand()
	    $cmd.CommandText = $statement

	    $result = $cmd.ExecuteReader()
	    # Do something with the results...
	    # Write-Host $result

	} catch {
	    Write-Error ("Database Exception: {0}`n{1}" -f `
	        $con.ConnectionString, $_.Exception.ToString())
	} 

	$sqlbc = new-object system.data.sqlclient.Sqlbulkcopy($sqlconn);
	$sqlbc.DestinationTableName="$schema.$table";
	$sqlbc.WriteToServer($result); 
	$sqlbc.close()
}



$service=Get-Service -name DBvisit*
foreach ( $x in $service) { Write-Host $service.indexof($x) ... $x.name.remove(0,16) ... $x.status}
#Write-Host Your choice? 
$user_choice = Read-Host -Prompt 'What environment do you want to refresh?'
$s=Get-Service -name Dbvisit*|select -Index $user_choice


$tempvar= wmic service $s.name get PathName


$dbvrepexe = ($tempvar[2] -split "(?!^)(?=--ddcfile)")[0].Trim()
$dbvrepexe = $dbvrepexe.Replace("`"","")
$service_path=$tempvar[2] -split ' '
$path_lenght=$service_path[4].length
$path_end=$service_path[4].lastindexof('\')+1
$cut=$path_lenght-$path_end
$adress=$service_path[4]| %{$_.Substring(0, $_.length - $cut) }
cd $adress



if (!(Test-Path $adress\$prepare_tabs)) {
	$checkArray = $false
	$prepareArray= & $dbvrepexe --ddcfile $ddcFile --silent list prepare
	foreach ($element in $prepareArray) {
	    if ($checkArray) {
	    	Write-Host $element.split(' ')[0]
	    	$element.split(' ')[0] | Out-File -append -Encoding ASCII -FilePath $prepare_tabs
	    }
	    if ($element.contains("DBRSUSER")) {
	    	$checkArray = $true
	    }
	}

	$prepare_tabs=$adress + $prepare_tabs
	$last_line = Get-Content $prepare_tabs -Tail 1
	$LinesInFile = [System.IO.File]::ReadAllLines($prepare_tabs)
	[System.IO.File]::WriteAllLines($prepare_tabs,$LinesInFile[0..($LinesInFile.Count - 2)])
	Remove-Variable -Name LinesInFile
}

if ($refresh) {
	stopDbvrepServices $s $adress
}

$ddcFile = Get-ChildItem -Filter *ddc
$ddcFile = $ddcFile[0].Name
& $dbvrepexe --ddcfile $ddcFile  pause MINE
& $dbvrepexe --ddcfile $ddcFile  pause APPLY


if (Test-Path $adress\prepare_script.txt) {
	Clear-Content prepare_script.txt
} 
else {
    New-Item -Name prepare_script.txt -ItemType File
}

if (!$rename_schema) {
	Do {
		$rename_schema = Read-Host "Rename SCHEMA name: [example w420g.dbo]"
		} Until ($rename_schema.contains("."))
}
else {
	if ($rename_schema.contains(".")) { }
		### overeni jestli sedi promena obsahuje tecku
	
	else {
		Write-Host 'SQL Server has database name and object owner inside The name. Add correct name.'
		Do {
			$rename_schema = Read-Host "Rename SCHEMA name: [example w420g.dbo]"
			} Until ($rename_schema.contains("."))
		    
	}

}

foreach($line in Get-Content .\$prepare_tabs) {
	"engine lock tables $line" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 

}



foreach($line in Get-Content .\$prepare_tabs) {
	$renameTo=$line.split('.')[1]
	"prepare table $line rename to $rename_schema.$renameTo"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	#"prepare table $line" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
}

"engine lock release all" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 


& $dbvrepexe --ddcfile $ddcFile read prepare_script.txt |Out-File -Encoding ASCII -FilePath prepare_script_output.txt 
# WARN-9015: Table already prepared on apply. To force the prepare again,
# ERR-9219: Errors detected, will not prepare the table MSI.TEST_1.
# musim precist log a reagovat na tuhle chybu
# kdyby nahodou byly exclude columns v tabulkach, co nebyly prepared, tak se musi vyradit taky
# pri pridavani tabulek do sql serveru musim tuhle kontrolu provest taky 

$dbvrep_db_apply = & $dbvrepexe --ddcfile $ddcFile show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_DATABASE
$dbvrep_db_apply_tmp=$dbvrep_db_apply[0]
$dbvrep_db_apply=($dbvrep_db_apply_tmp -split(' '))[2]

$dbvrep_user_apply = & $dbvrepexe --ddcfile $ddcFile show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_USER
$dbvrep_user_apply=($dbvrep_user_apply -split(' '))[2]
if ($dbvrep_user_apply -ne $sql_server_id) {
	$dbvrep_user_apply = $sql_server_id
}


$dbvrep_user = Get-Content $service_path[4] | Select-String -Pattern DDC_USER
$dbvrep_user_tmp = $dbvrep_user -split ' '
$dbvrep_user = $dbvrep_user_tmp[2]

$source_tns = Get-Content $service_path[4] | Select-String -Pattern DDC_DATABASE 
$source_tns_tmp = $source_tns -split ' '
$source_tns = $source_tns_tmp[2]

$table=(Get-Content .\$prepare_tabs -head 1).split(".")[1].ToUpper()
$SCHEMA=(Get-Content .\$prepare_tabs -head 1).split(".")[0].ToUpper()

$dbvrep_schema_apply = & $dbvrepexe --ddcfile $ddcFile show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_SCHEMA
$dbvrep_schema_apply_tmp=$dbvrep_schema_apply[0]
$dbvrep_schema_apply=($dbvrep_schema_apply_tmp -split(' '))[2]

$dbvrep_schema2_apply = & $dbvrepexe --ddcfile $ddcFile show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_SCHEMA2
$dbvrep_schema2_apply=($dbvrep_schema2_apply -split(' '))[2]

$sqlQuery = @"
SET NOCOUNT ON
select instantiation_scn from $dbvrep_schema_apply.$dbvrep_schema2_apply.DBRSAPPLY_DICT_TABLES where SOURCE_OBJECT_ID = 
( select obj_ from $dbvrep_schema_apply.$dbvrep_schema2_apply.dbrsobj$ where name='$table' and owner_ in 
(select user_ from $dbvrep_schema_apply.$dbvrep_schema2_apply.dbrsuser$ where name = '$SCHEMA'));
go
"@
$FLASHBACK_SCN = sqlcmd -U $dbvrep_user_apply -P $sql_server_passwd -S (Get-OdbcDsn -Name $dbvrep_db_apply).Attribute["Server"] -b -h-1 -Q $sqlQuery

if ($external_import) {

Write-Host
Write-Host
Write-Host
Write-Host *****************************************************
Write-Host "Use this SCN for data instantiation: $FLASHBACK_SCN"
Write-Host *****************************************************
Write-Host
Write-Host
Write-Host
Write-Host "Program is paused until you finish the instantiation"
cmd /c pause | out-null
} else {
	foreach($line in Get-Content .\$prepare_tabs) {
	$renameTo = $line.split('.')[1]
	$oracle_schema = $line.split('.')[0]
	# "prepare table $line rename to $rename_schema.$renameTo"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	# function loadTheTable($username, $password, $data_source, $table, $database, $schema, $dbvrep_db_apply, $FLASHBACK_SCN) 
	loadTheTable "SYSTEM" $system_password $source_tns $oracle_schema $renameTo $rename_schema.split('.')[0] $rename_schema.split('.')[1] $dbvrep_db_apply $FLASHBACK_SCN $dbvrep_user_apply $sql_server_passwd 
	# $line je blbost, protoze to je cela radka -> tudiz predelat

	# $username, $password, $data_source, $table, $database, $schema, $dbvrep_db_apply, $FLASHBACK_SCN) 
	# potrebuji
	#oracle: jmeno_systemu heslo tns_names ok
	#oracle: schema a jmeno tabulky
	#sql_server: jmeno_db jmeno_schematu jmeno_tabulky
	#DSN_name
	#FLASHBACK_SCN
	}
}

$sqlQueryExclude = @"
set verify off
set feedback off
set linesize 100
set pagesize 40000
set heading off

spool exclude_cols.txt

select 'EXCLUDE COLUMN '||d.owner||'.'||d.table_name||'.'||c.column_name thestring
from dba_tab_columns c, dba_tables d
where d.owner = UPPER('$SCHEMA')
and d.owner = c.owner
and d.table_name = c.table_name
and c.DATA_TYPE not in ( 
'NUMBER',
'FLOAT',
'VARCHAR2',
'VARCHAR',
'CHAR',
'NVARCHAR2',
'NCHAR2',
'NCHAR',
'DATE',
'RAW',
'LONG',
'LONG RAW')
and data_type not like 'TIMESTAMP%'
order by d.owner,d.table_name,c.column_name;
spool off
exit
"@
$FLASHBACK_SCN = $sqlQueryExclude | sqlplus -silent system/oracle@$source_tns 
(gc exclude_cols.txt) | ? {$_.trim() -ne "" } | set-content exclude_cols.txt
$excludeFile = Get-Item ".\exclude_cols.txt"
$content = [System.IO.File]::ReadAllText($excludeFile.FullName)
$content = $content.Trim()
[System.IO.File]::WriteAllText("exclude_cols.txt", $content)
if (!((gc .\exclude_cols.txt) -eq $null))
{
	& $dbvrepexe --ddcfile $ddcFile read exclude_cols.txt
	& $dbvrepexe --ddcfile $ddcFile shutdown MINE
	& $dbvrepexe --ddcfile $ddcFile shutdown APPLY
	Start-Sleep -Seconds 10

	if ($s.Name.split("_")[1].ToUpper() -eq "MINE") {
		# it's the mine server where script is started
		$target= & $dbvrepexe --ddcfile $ddcFile show APPLY_REMOTE_INTERFACE| Select-String -Pattern ^APPLY
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')).start()
		(Get-Service -Name $s.Name).start()
	} else {
		# it's the apply server where script is started
		$target= & $dbvrepexe --ddcfile $ddcFile show MINE_REMOTE_INTERFACE| Select-String -Pattern ^MINE
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('APPLY','MINE')).start()
		(Get-Service -Name $s.Name).start()
	}
}

if ($refresh) {

	Write-Host
	Write-Host *****************************************************
	Write-Host The refresh is done. Dbvisit replication is back. 
	Write-Host *****************************************************
}
else {
	Write-Host
	Write-Host *****************************************************
	Write-Host Tables were added to replication 
	Write-Host *****************************************************   
}

exit
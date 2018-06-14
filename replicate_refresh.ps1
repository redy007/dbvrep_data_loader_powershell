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

  version 3.1

  Version 3.1 supports suport Linux as source. However, user must start services manually.


.EXAMPLE
RUN The script within MINE or APPLY process:
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh 
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -external_import
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -sql_server_id sa -sql_server_passwd sa -system_password oracle -refresh -prepare_tabs prepare_tabs.txt


#>

param (
#    [string]$rename_schema = $( Read-Host "Rename SCHEMA name: [w420g.dbo]" ),
#    [string]$ComputerName = $env:computername,    
	[string]$sql_server_id = $( Read-Host "User ID for SQL Server connection [dbvrep variable APPLY.APPLY_USER (default SA user)]" ),
    [string]$sql_server_passwd = $(Read-Host -asSecureString "Password for SQL Server connection [dbvrep variable APPLY.APPLY_USER (default SA user)]"),
    [string]$system_password = $( Read-Host -asSecureString "Input password for System user on the source database" ),
    [string]$prepare_tabs = "prepare_tabs.txt",
    [switch]$refresh = $false,
    [switch]$external_import = $false
)


Start-Transcript

function stopDbvrepServices($s, $adress, $os)    
{
	cd $adress  
	$tempvar= wmic service $s.name get PathName
	$dbvrepexe = ($tempvar[2] -split "(?!^)(?=--ddcfile)")[0].Trim()
	$dbvrepexe = $dbvrepexe.Replace("`"","")
	$ddcFile = Get-ChildItem -Filter *ddc
	$ddcFile = $ddcFile[0].Name

	& $dbvrepexe --ddcfile $ddcFile shutdown all
	
	Start-Sleep -Seconds 10
	if (Get-Service -Name $s.Name| Where-Object {$_.Status -eq "Running"}) {
		Write-Warning "After shutdown one of The dbvisit service is still running. Stop it manully and enter to continue this script."
		cmd /c pause | out-null
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

		Start-Sleep -Seconds 10
		if (Get-Service -Name $s.Name| Where-Object {$_.Status -ne "Running"}) {
			Write-Host "start the MINE service manually on this server."
			cmd /c pause | out-null
		}
		if (Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')| Where-Object {$_.Status -ne "Running"}) {
			Write-Host "start the APPLY service manually on server $t_servername."
			cmd /c pause | out-null
		}

	} else {
		# it's the apply server where script is started
		$target= & $dbvrepexe --ddcfile $ddcFile show MINE_REMOTE_INTERFACE| Select-String -Pattern ^MINE
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		if ($os.equals('Windows')) {
			(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('APPLY','MINE')).start()
		}		
		(Get-Service -Name $s.Name).start()

		Start-Sleep -Seconds 10
			if (Get-Service -Name $s.Name| Where-Object {$_.Status -ne "Running"}) {
				Write-Warning "start the APPLY service manually on this server."
				cmd /c pause | out-null
			}
			if ($os.equals('Linux')) {
				Write-Warning "start the MINE service manually on server $t_servername."
				cmd /c pause | out-null
			} 
			else {
				if (Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')| Where-Object {$_.Status -ne "Running"}) {
					Write-Warning "start the MINE service manually on server $t_servername."
					cmd /c pause | out-null
				}
			}
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
			Write-Error "APPLY process stuck. Check apply log file or SQL Server for locks."
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

	# oracle list of data types arraylist
	$oracleDataType = 'NUMBER','FLOAT','NVARCHAR2','VARCHAR2','VARCHAR','CHAR','NCHAR','DATE','RAW','LONG','LONG RAW','BFILE','CLOB','BLOB','LOB','NCLOB','INTERVAL','TIMESTAMP','BINARY_FLOAT','BINARY_DOUBLE','ROWID','XML'
	# sql server list of data types arraylist
	$sqlDataType = 'DECIMAL','DECIMAL','NVARCHAR','VARCHAR','VARCHAR','CHAR','NCHAR','DATETIME','VARBINARY','TEXT','VARBINARY(MAX)','x','x','x','x','x','x','DATETIME2','x','x','x','x'

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
			if ($dtype[$I] -eq "FLOAT") {
				$DATA_TYPE_SQL = "DECIMAL(38,18)"
			}
			elseif ($DATA_PRECISION[$I] -ne '0') {
			   	$CURR_DATA_PRECISION = $DATA_PRECISION[$I]
				$DATA_TYPE_SQL = "DECIMAL" + "($CURR_DATA_PRECISION)" 
			}
			else {
				$DATA_TYPE_SQL = "BIGINT"
			}
		}
		# stanovani precision a scale pro numbers
		elseif ($DATA_TYPE_SQL -eq "DECIMAL" -AND ($DATA_SCALE[$I] -ne '0' -OR $DATA_PRECISION[$I] -ne '0')) {
		    $DATA_TYPE_SQL = "DECIMAL(38,18)"
		} 
		# stanovani precision pro string (budu muset vsechny vypsat)
		elseif (($DATA_TYPE_SQL -eq "VARCHAR" -OR $DATA_TYPE_SQL -eq "CHAR" -OR $DATA_TYPE_SQL -eq "NCHAR" -OR $DATA_TYPE_SQL -eq "NVARCHAR" -OR $DATA_TYPE_SQL -eq "VARBINARY") -AND ($DATA_LENGTH[$I] -ne '0')) {
			$CURR_DATA_LENGTH = $DATA_LENGTH[$I];
			$DATA_TYPE_SQL = $DATA_TYPE_SQL+"($CURR_DATA_LENGTH)"
		}
		elseif ($dtype[$I] -like 'TIMESTAMP*') {
			$DATA_TYPE_SQL = "DATETIME2"
		}
		elseif ($DATA_TYPE_SQL -eq "x" -OR $DATA_TYPE_SQL -eq "-1") {
		    continue
		}
		$COLUMN_NAME = $col_name[$I]
		$SELECT_COLUMNS_NAME += ", " + $COLUMN_NAME
		$nullable = $null_able[$I].trim()
		"$COLUMN_NAME $DATA_TYPE_SQL $nullable,"| Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
		$DDL.Add("$COLUMN_NAME $DATA_TYPE_SQL $nullable,")
	}
	');' | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
	$DDL.Add(");")
	$SELECT_COLUMNS_NAME = $SELECT_COLUMNS_NAME.trim(",", " ")

	#"" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
	$rows_pk = ""
	$statement = "select COLUMN_NAME from ALL_CONS_COLUMNS cols, ALL_CONSTRAINTS cons where cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME and cols.OWNER = cons.INDEX_OWNER and cons.CONSTRAINT_TYPE = 'P' and cons.OWNER = upper('$oracle_schema') and cons.TABLE_NAME = upper('$table') order by cols.POSITION"

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
	"ALTER TABLE $database.$schema.$table ADD PRIMARY KEY ($rows_pk);"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	$DDL.Add("ALTER TABLE $database.$schema.$table ADD PRIMARY KEY ($rows_pk);")
	}
	#"" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 
	#ALTER TABLE $table ADD CONSTRAINT $statement_pk_CONSTRAINT_NAME PRIMARY KEY ($con_column_name );

	$rows_uk = ""
	$constraint_name = $null
	$statement = "select COLUMN_NAME from ALL_CONS_COLUMNS cols, ALL_CONSTRAINTS cons where cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME and cols.OWNER = cons.INDEX_OWNER and cons.CONSTRAINT_TYPE = 'U' and cons.OWNER = upper('$oracle_schema') and cons.TABLE_NAME = upper('$table') order by cols.POSITION"

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
	"ALTER TABLE $database.$schema.$table ADD UNIQUE ($rows_uk);"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	$DDL.Add("ALTER TABLE $database.$schema.$table ADD UNIQUE ($rows_uk);")
	}
	#"" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 

	$statement = "SELECT i.index_name, c.column_name FROM all_indexes i, all_ind_columns c WHERE i.table_name = upper('table') AND i.owner = upper('oracle_schema') AND i.uniqueness != 'UNIQUE' AND i.index_name = c.index_name AND i.table_owner = c.table_owner AND i.table_name = c.table_name AND i.owner = c.index_owner"
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
				$DDL.Add("CREATE INDEX $index_name ON $database.$schema.$table ($rows_idx);")
				"CREATE INDEX $index_name ON $database.$schema.$table ($rows_idx);" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
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
		"CREATE INDEX $index_name ON $database.$schema.$table ($rows_idx);" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
		$DDL.Add("CREATE INDEX $index_name ON $database.$schema.$table ($rows_idx);")
	}
	#"" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 


	$SQL_SERVER_DB = (Get-OdbcDsn -Name $dbvrep_db_apply).Attribute["Server"]
	$sqlTns = "server=$SQL_SERVER_DB;database=$database;uid=$dbvrep_user_apply;pwd=$sql_server_passwd;"
	$sqlconn = new-object system.data.sqlclient.SqlConnection($sqlTns);
	$sqlconn.Open();
	$cmd = New-object System.Data.SqlClient.SqlCommand;
	$cmd.Connection = $sqlconn;
	$cmd.CommandText = $DDL
	try {
		$rows = $cmd.ExecuteNonQuery();
	} catch [System.Data.SqlClient.SqlException] {
		if ( $_.Exception.Number -eq "2714" ) {
			$cmd.CommandText = "TRUNCATE TABLE $database.$schema.$table"
			Write-Host "TRUNCATE TABLE: " $database "." $schema "." $table
			$cmd.ExecuteNonQuery();
		}
		if ( $_.Exception.Number -eq "8111" ) {
			$rows_pk = ""
			$statement = "select COLUMN_NAME from ALL_CONS_COLUMNS cols, ALL_CONSTRAINTS cons where cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME and cols.OWNER = cons.INDEX_OWNER and cons.CONSTRAINT_TYPE = 'P' and cons.OWNER = upper('$oracle_schema') and cons.TABLE_NAME = upper('$table') order by cols.POSITION"

			$conORCL = New-Object System.Data.OracleClient.OracleConnection($connection_string)
			$conORCL.Open()
			$orcl = $conORCL.CreateCommand()
			$orcl.CommandText = $statement
			$orclResult = $orcl.ExecuteReader()
			while ($orclResult.Read()) {
				$rows_pk = $orclResult.GetString(0)
				$sqlDataType = new-object system.data.sqlclient.SqlConnection($sqlTns);
				$sqlDataType.Open();
				$cmdDataType = New-object System.Data.SqlClient.SqlCommand;
				$cmdDataType.Connection = $sqlDataType;
				$cmdDataType.CommandText = "SELECT DATA_TYPE, ISNULL(character_maximum_length, 9999999), ISNULL(NUMERIC_PRECISION, 255), ISNULL(NUMERIC_SCALE, 255) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$table' AND COLUMN_NAME = '$rows_pk'"
				$rdr = $cmdDataType.ExecuteReader();
				while ($rdr.Read()) {
                        $sqlServerDataType = $rdr.GetString(0)
                        $characterLength = $rdr.GetValue(1)
                        $NumericPrecision = $rdr.GetValue(2)
                        $NumericScale = $rdr.GetValue(3)
                        $sqlAlter = new-object system.data.sqlclient.SqlConnection($sqlTns);
						$sqlAlter.Open();
						$cmdAlter = New-object System.Data.SqlClient.SqlCommand;
						$cmdAlter.Connection = $sqlAlter;
						if ($characterLength -eq 9999999) {
							$cmdAlter.CommandText = "ALTER TABLE $database.$schema.$table ALTER COLUMN $rows_pk $sqlServerDataType($NumericPrecision, $NumericScale) NOT NULL"
						}
						else {
							$cmdAlter.CommandText = "ALTER TABLE $database.$schema.$table ALTER COLUMN $rows_pk $sqlServerDataType($characterLength) NOT NULL"
						}
                        
                        $rows = $cmdAlter.ExecuteNonQuery();
                }
                $rowsPkMulti += ", " + $orclResult.GetString(0)	
                $sqlDataType.close();		
			}
			$rowsPkMulti = $rowsPkMulti.trim(",", " ")
			$sqlconn.Close();
			$sqlconn.Open();
			$cmd = New-object System.Data.SqlClient.SqlCommand;
			$cmd.Connection = $sqlconn;
			Write-Host "ALTER TABLE $database.$schema.$table ADD PRIMARY KEY ($rowsPkMulti)"
			$cmd.CommandText = "ALTER TABLE $database.$schema.$table ADD PRIMARY KEY ($rowsPkMulti)"
			$rows = $cmd.ExecuteNonQuery();
		}
		else {
			Write-Host "different error: " $_.Exception.ToString()
		}
	} catch {
	    Write-Error ("Database Exception: {0}`n{1}" -f `
	        $con.ConnectionString, $_.Exception.ToString())
	} 
	#} #Finally{
    #if ($con.State -eq ‘Open’) { $con.close() }
   	#}


	$newType = 'namespace System.Data.SqlClient
		{    
		 using Reflection;
		 
		 public static class SqlBulkCopyExtension
		 {
		 const String _rowsCopiedFieldName = "_rowsCopied";
		 static FieldInfo _rowsCopiedField = null;
		 
		 public static int RowsCopiedCount(this SqlBulkCopy bulkCopy)
		 {
		 if (_rowsCopiedField == null) _rowsCopiedField = typeof(SqlBulkCopy).GetField(_rowsCopiedFieldName, BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);            
		 return (int)_rowsCopiedField.GetValue(bulkCopy);
		 }
		 }
		}
		'

    Add-Type -ReferencedAssemblies 'System.Data.dll' -TypeDefinition $newType
	$null = [Reflection.Assembly]::LoadWithPartialName("System.Data")



	$FLASHBACK_SCN = $FLASHBACK_SCN.trim(" ")
	$statement = "select $SELECT_COLUMNS_NAME from $oracle_schema.$table as of SCN $FLASHBACK_SCN"

	try{
	    $con = New-Object System.Data.OracleClient.OracleConnection($connection_string)

	    $con.Open()
	    $cmd = $con.CreateCommand()
	    $cmd.CommandText = $statement

	    $result = $cmd.ExecuteReader()

	} catch [System.Data.SqlClient.SqlException] {
		Write-Host $_.Exception.Number

	} catch {
	    Write-Error ("Database Exception: {0}`n{1}" -f `
	        $con.ConnectionString, $_.Exception.ToString())
	} 

	try {
		$sqlbc = new-object system.data.sqlclient.Sqlbulkcopy($sqlconn);
		$sqlbc.DestinationTableName="$schema.$table";
		#$sqlbc.bulkcopyTimeout = 0 
		$sqlbc.BatchSize = 50000
		$sqlbc.NotifyAfter = 50000
		$sqlbc.Add_SqlRowscopied({Write-Host "$($args[1].RowsCopied) rows copied" })
		$sqlbc.WriteToServer($result);
		$sqlbc.close()
		Write-Host "WriteToServer"
		Write-Host	$oracle_schema "." $table

		# "Note: This count does not take into consideration the number of rows actually inserted when Ignore Duplicates is set to ON."
		$total = [System.Data.SqlClient.SqlBulkCopyExtension]::RowsCopiedCount($sqlbc)
		Write-Host "$total total rows written"
		
	} catch [System.Data.OracleClient.OracleException] {
		$oraErrorNumber = ($_.Exception.GetBaseException()[0] -split ':')[1]
		$oraErrorName = ($_.Exception.GetBaseException()[0] -split ' ')[3]
		#Write-Host $_.Exception.GetBaseException()[0]
		Write-Host $oraErrorNumber ":" $oraErrorName "-> table "$oracle_schema"."$table



		"Table $oracle_schema"."$table added to replication"| Out-File -append -Encoding ASCII -FilePath $prepare_tabs

		#unprepare the table with error
		$tempvar= wmic service $s.name get PathName
		$dbvrepexe = ($tempvar[2] -split "(?!^)(?=--ddcfile)")[0].Trim()
		$dbvrepexe = $dbvrepexe.Replace("`"","")
		$ddcFile = Get-ChildItem -Filter *ddc
		$ddcFile = $ddcFile[0].Name
		& $dbvrepexe --ddcfile $ddcFile unprepare table $oracle_schema"."$table  | Out-Null

	} catch {
	    Write-Error ("Database Exception: {0}`n{1}" -f `
	        $con.ConnectionString, $_.Exception.ToString())
		    Write-Host "WriteToServer"
		    Write-Host ($_.Exception.GetBaseException()[0] -split ':')[1]
		    Write-Host	$oracle_schema "." $table
		    Write-Host $_.Exception.GetType().FullName 
	}
	#finally {
	#} 
}

function createForeignKeys($username, $password, $data_source, $oracle_schema, $table, $database, $schema, $dbvrep_db_apply, $FLASHBACK_SCN, $dbvrep_user_apply, $sql_server_passwd)    
{

	$DDL = New-Object System.Collections.Generic.List[System.Object]

	add-type -AssemblyName System.Data.OracleClient
	$result = New-Object System.Collections.ArrayList
	$connection_string = "User Id=$username;Password=$password;Data Source=$source_tns"

	$statement = "SELECT a.constraint_name, a.column_name, c_pk.table_name, b.column_name FROM all_cons_columns a JOIN all_constraints c ON a.owner = c.owner AND 
	a.constraint_name = c.constraint_name JOIN all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name JOIN all_cons_columns b ON C_PK.owner = b.owner 
	AND  C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION WHERE c.constraint_type = 'R' AND c.table_name = upper('$table') AND c.owner=upper('$oracle_schema')"
	$con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
	$con.Open()
	$cmd = $con.CreateCommand()
	$cmd.CommandText = $statement
	$result = $cmd.ExecuteReader()
	while ($result.Read()) {
		$constraint_name = $result.GetString(0)
		$columnName = $result.GetString(1)
		$rTableName = $result.GetString(2)		
		$rColName = $result.GetString(3)
		$DDL.Add("ALTER TABLE $database.$schema.$table ADD CONSTRAINT $constraint_name FOREIGN KEY ($columnName) REFERENCES $database.$schema.$rTableName ($rColName)")		
	}

	$SQL_SERVER_DB = (Get-OdbcDsn -Name $dbvrep_db_apply).Attribute["Server"]
	$sqlconn = "server=$SQL_SERVER_DB;database=$database;uid=$dbvrep_user_apply;pwd=$sql_server_passwd;"
	$sqlconn = new-object system.data.sqlclient.SqlConnection($sqlconn);
	$sqlconn.Open();
	$cmd = New-object System.Data.SqlClient.SqlCommand;
	$cmd.Connection = $sqlconn;
	$cmd.CommandText = $DDL
	try {
		$rows = $cmd.ExecuteNonQuery();
		"ALTER TABLE $database.$schema.$table ADD CONSTRAINT $constraint_name FOREIGN KEY ($columnName) REFERENCES $database.$schema.$rTableName ($rColName)" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	} catch {
	    #Write-Error ("Database Exception: {0}`n{1}" -f `
	    $cmd.CommandText = $DDL
	} 

}


$service=Get-Service -name DBvisit*
foreach ( $x in $service) { Write-Host $service.indexof($x) ... $x.name.remove(0,16) ... $x.status}
#Write-Host Your choice? 
$user_choice = Read-Host -Prompt 'What environment do you want to use?'
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

$ddcFile = Get-ChildItem -Filter *ddc
$ddcFile = $ddcFile[0].Name

$os = & $dbvrepexe --ddcfile $ddcFile show SETUP_SCRIPT_PATH| Select-String -Pattern ^MINE.SETUP_SCRIPT_PATH
$os=($os -split(' '))[2]
if ($os.contains("/")) {
    $os = "Linux"
}
elseif ($os.contains("\")) {
    $os = "Windows"
}
else {
    write-host "Unkown OS."
}


if ($refresh) {
	stopDbvrepServices $s $adress $os
}


& $dbvrepexe --ddcfile $ddcFile  pause MINE | Out-Null
& $dbvrepexe --ddcfile $ddcFile  pause APPLY | Out-Null


if (Test-Path $adress\prepare_script.txt) {
	Clear-Content prepare_script.txt
} 
else {
    New-Item -Name prepare_script.txt -ItemType File | Out-Null
}

$APPLY_MSSQL_USER_DB = & $dbvrepexe --ddcfile $ddcFile show APPLY_MSSQL_USER_DB| Select-String -Pattern ^APPLY.APPLY_MSSQL_USER_DB
$APPLY_MSSQL_USER_DB = ($APPLY_MSSQL_USER_DB -split(' '))[2]

$dbvrep_schema2_apply = & $dbvrepexe --ddcfile $ddcFile show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_SCHEMA2
$dbvrep_schema2_apply = ($dbvrep_schema2_apply -split(' '))[2]

foreach($line in Get-Content .\$prepare_tabs) {
	"engine lock tables $line" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 

}



foreach($line in Get-Content .\$prepare_tabs) {
	$renameTo=$line.split('.')[1]
	"prepare table $line rename to $APPLY_MSSQL_USER_DB.$dbvrep_schema2_apply.$renameTo"  | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
	#"prepare table $line" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt
}

"engine lock release all" | Out-File -append -Encoding ASCII -FilePath prepare_script.txt 


& $dbvrepexe --ddcfile $ddcFile read prepare_script.txt 2>&1|Out-File -Encoding ASCII -FilePath prepare_script_output.txt 

if ($refresh) {
	$tabsToPrepare = @()
	Get-Content prepare_tabs.txt |
	    ForEach-Object {
	            $tabsToPrepare += $_
	    }

	$tabsPrepared = @()
	$checkArray = $false
	$prepareArray= & $dbvrepexe --ddcfile $ddcFile --silent list prepare 
	$prepareArray |
	    ForEach-Object {
		if ($checkArray) {
	            $tabsPrepared += $_.split(' ')[0]
		}
		if ($_.contains("DBRSUSER")) {
		    $checkArray = $true
	        }

	}
	$tabsPrepared = $tabsPrepared[0..($tabsPrepared.Length-2)]
	$excludeTabsArray = $tabsPrepared + $tabsToPrepare | select -uniq    #union

} else {
	$notPreparedTabs = @()
	$tabsToPrepare = @()
	$excludeTabsFilter = ""
	Get-Content prepare_script_output.txt |
	    ForEach-Object {
	        if($_ -match ('^' + [regex]::Escape('ERR-9219')))
	        {
	            $notPreparedTabs += ($_.split(' ')[8]) -Replace ".$"
	        }
	    }

	Get-Content prepare_tabs.txt |
	    ForEach-Object {
	            $tabsToPrepare += $_
	    }

	$excludeTabsArray = $tabsToPrepare| ? {!($notPreparedTabs -contains $_)}
	""|Out-File -Encoding ASCII -FilePath prepare_script_output.txt 
	$excludeTabsArray |Out-File -Encoding ASCII -FilePath prepare_script_output.txt 
	foreach ($foo in $excludeTabsArray) {
		$excludeTabsFilter += ", " + "`'" + $foo + "`'"
	}
	$excludeTabsFilter = $excludeTabsFilter.trim(",", " ")
}

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

	$foreignKeyTab = New-Object System.Collections.Generic.List[System.Object]
	$foreignKeySchema = New-Object System.Collections.Generic.List[System.Object]
	add-type -AssemblyName System.Data.OracleClient
	$result = New-Object System.Collections.ArrayList
	$list = [System.Collections.Generic.List[System.Object]]
	$systemName = "SYSTEM"
	$connection_string = "User Id=$systemName;Password=$system_password;Data Source=$source_tns"

	$preparedTabsArray = & $dbvrepexe --ddcfile $ddcFile --silent list prepare 

	Write-Host "Creating tables and loading data to SQL Server"
	
	foreach($line in $excludeTabsArray) {
		$renameTo = $line.split('.')[1]
		$oracle_schema = $line.split('.')[0]

		loadTheTable "SYSTEM" $system_password $source_tns $oracle_schema $renameTo $APPLY_MSSQL_USER_DB $dbvrep_schema2_apply $dbvrep_db_apply $FLASHBACK_SCN $dbvrep_user_apply $sql_server_passwd 
		
		$statement = "select d.owner, d.table_name from all_constraints d, all_constraints b where d.constraint_name=b.r_constraint_name and b.table_name=upper('$renameTo') and b.owner=upper('$oracle_schema') and b.constraint_type='R'"
		$con = New-Object System.Data.OracleClient.OracleConnection($connection_string)
		$con.Open()
		$cmd = $con.CreateCommand()
		$cmd.CommandText = $statement
		$result = $cmd.ExecuteReader()
		while ($result.Read()) {
			$owner = $result.GetString(0)
			$tableName = $result.GetString(1)
			$outputTable = "*$owner.$tableName*"
			if (($preparedTabsArray -like $outputTable).Count -gt 0) {
				$foreignKeyTab.Add($renameTo)
				$foreignKeySchema.Add($oracle_schema)
			}
		}
	}

	Write-Host "Creating FOREIGN constraints"
	For ($M=0;$M -lt $foreignKeyTab.count;$M++) {
		createForeignKeys "SYSTEM" $system_password $source_tns $foreignKeySchema[$M] $foreignKeyTab[$M] $APPLY_MSSQL_USER_DB $dbvrep_schema2_apply $dbvrep_db_apply $FLASHBACK_SCN $dbvrep_user_apply $sql_server_passwd
	}
}

$lastStart = $false
Write-Host "Excluding non supported columns"

foreach($line in $excludeTabsArray) {
	$renameTo = $line.split('.')[1]
	$oracle_schema = $line.split('.')[0]

	$sqlQueryExclude = @"
	set verify off
	set feedback off
	set linesize 100
	set pagesize 40000
	set heading off

	spool exclude_cols.txt

	select 'EXCLUDE COLUMN '||d.owner||'.'||d.table_name||'.'||c.column_name thestring
	from dba_tab_columns c, dba_tables d
	where d.owner = UPPER('$oracle_schema')
	and d.table_name = upper('$renameTo')
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

	if (!((gc .\exclude_cols.txt) -eq $null)) {
		& $dbvrepexe --ddcfile $ddcFile read exclude_cols.txt
		$lastStart = $true
	}

}

if ($lastStart)
{
	Write-Host "Restarting MINE & APPLY process"
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

		Start-Sleep -Seconds 10
		if (Get-Service -Name $s.Name| Where-Object {$_.Status -ne "Running"}) {
			Write-Host "start the MINE service manually on this server."
			cmd /c pause | out-null
		}
		if (Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')| Where-Object {$_.Status -ne "Running"}) {
			Write-Host "start the APPLY service manually on server $t_servername."
			cmd /c pause | out-null
		}

	} else {
		# it's the apply server where script is started
		$target= & $dbvrepexe --ddcfile $ddcFile show MINE_REMOTE_INTERFACE| Select-String -Pattern ^MINE
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		if ($os.equals('Windows')) {
			(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('APPLY','MINE')).start()
		}		
		(Get-Service -Name $s.Name).start()

		Start-Sleep -Seconds 10
			if (Get-Service -Name $s.Name| Where-Object {$_.Status -ne "Running"}) {
				Write-Warning "start the APPLY service manually on this server."
				cmd /c pause | out-null
			}
			if ($os.equals('Linux')) {
				Write-Warning "start the MINE service manually on server $t_servername."
				cmd /c pause | out-null
			} 
			else {
				if (Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')| Where-Object {$_.Status -ne "Running"}) {
					Write-Warning "start the MINE service manually on server $t_servername."
					cmd /c pause | out-null
				}
			}
	}
}


if ($refresh) {

	Write-Host
	Write-Host *****************************************************
	Write-Host The refresh is done. Dbvisit replication is back. 
	Write-Host *****************************************************
	Write-Host
	Write-Host
}
else {
	Write-Host
	Write-Host *****************************************************
	Write-Host Tables were added to replication 
	Write-Host *****************************************************   
	Write-Host
	Write-Host
}

exit
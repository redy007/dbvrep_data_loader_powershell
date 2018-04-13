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


.EXAMPLE
RUN The script from MINE process:
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -rename_schema msi.dbo -sa_passwd sa -system_password oracle -refresh 
   . C:\Users\Administrator\Desktop\replicate_refresh.ps1 -rename_schema msi.dbo -sa_passwd sa -system_password oracle -refresh -prepare_tabs prepare_tabs.txt


#>

param (
    [string]$rename_schema = $( Read-Host "Rename SCHEMA name: [w420g.dbo]" ),
#    [string]$ComputerName = $env:computername,    
    [string]$sa_passwd = $(Read-Host -asSecureString "Password for SQL Server connection [dbvrep variable APPLY.APPLY_USER (default SA user)]"),
    [string]$system_password = $( Read-Host -asSecureString "Input password for System user on the source database" ),
    [string]$prepare_tabs = "prepare_tabs.txt"
    [switch]$refresh = $false
)


Start-Transcript

function stopDbvrepServices    
{  
	# risk the second process is running
	# user will find it out when all.bat script fail.
	if (Get-Service -Name $s.Name| Where-Object {$_.Status -eq "Running"}) {
		.\start-console.bat shutdown all
		
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
		$target= .\start-console.bat show APPLY_REMOTE_INTERFACE| Select-String -Pattern ^APPLY
		$t= $target -split ' '
		$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

		(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')).start()
		(Get-Service -Name $s.Name).start()
	} else {
		# it's the apply server where script is started
		$target= .\start-console.bat show MINE_REMOTE_INTERFACE| Select-String -Pattern ^MINE
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

	$xPLOG=.\start-console.bat list status| Select-String -Pattern ^MINE
	$MPLOG= $xPLOG -split ' '
	$numCycle = 0;

	Do {
		Write-Host wait 1m for sync both processes
		Start-Sleep -Seconds 60
		$xPLOG=.\start-console.bat list status| Select-String -Pattern ^APPLY
		$APLOG= $xPLOG -split ' '
		$numCycle++
		if ($numCycle.equals(5)) {
			Write-Host "APPLY process stuck. Check apply log file or SQL Server for locks."
		}

	}
	Until ($MPLOG[6] -le $APLOG[6])
	Write-Host APPLY process is up to date.
}

$service=Get-Service -name DBvisit*
foreach ( $x in $service) { Write-Host $service.indexof($x) ... $x.name.remove(0,16) ... $x.status}
#Write-Host Your choice? 
$user_choice = Read-Host -Prompt 'What environment do you want to refresh?'
$s=Get-Service -name Dbvisit*|select -Index $user_choice


$tempvar= wmic service $s.name get PathName

$service_path=$tempvar[2] -split ' '
$path_lenght=$service_path[4].length
$path_end=$service_path[4].lastindexof('\')+1
$cut=$path_lenght-$path_end
$adress=$service_path[4]| %{$_.Substring(0, $_.length - $cut) }
cd $adress



if (!(Test-Path $adress\$prepare_tabs)) {
	$checkArray = $false
	$prepareArray= .\start-console.bat --silent list prepare
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
	stopDbvrepServices()
}

.\start-console.bat pause MINE
.\start-console.bat pause APPLY


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


.\start-console.bat  read prepare_script.txt |Out-File -Encoding ASCII -FilePath prepare_script_output.txt 


$dbvrep_db_apply = .\start-console.bat show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_DATABASE
$dbvrep_db_apply_tmp=$dbvrep_db_apply[0]
$dbvrep_db_apply=($dbvrep_db_apply_tmp -split(' '))[2]

$dbvrep_user_apply = .\start-console.bat show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_USER
$dbvrep_user_apply=($dbvrep_user_apply -split(' '))[2]



$dbvrep_user = Get-Content $service_path[4] | Select-String -Pattern DDC_USER
$dbvrep_user_tmp = $dbvrep_user -split ' '
$dbvrep_user = $dbvrep_user_tmp[2]

$source_tns = Get-Content $service_path[4] | Select-String -Pattern DDC_DATABASE 
$source_tns_tmp = $source_tns -split ' '
$source_tns = $source_tns_tmp[2]

$table=(Get-Content .\$prepare_tabs -head 1).split(".")[1].ToUpper()
$SCHEMA=(Get-Content .\$prepare_tabs -head 1).split(".")[0].ToUpper()

$dbvrep_schema_apply = .\start-console.bat show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_SCHEMA
$dbvrep_schema_apply_tmp=$dbvrep_schema_apply[0]
$dbvrep_schema_apply=($dbvrep_schema_apply_tmp -split(' '))[2]

$dbvrep_schema2_apply = .\start-console.bat show APPLY.APPLY_DATABASE| Select-String -Pattern ^APPLY.APPLY_SCHEMA2
$dbvrep_schema2_apply=($dbvrep_schema2_apply -split(' '))[2]

$sqlQuery = @"
SET NOCOUNT ON
select instantiation_scn from $dbvrep_schema_apply.$dbvrep_schema2_apply.DBRSAPPLY_DICT_TABLES where SOURCE_OBJECT_ID = 
( select obj_ from $dbvrep_schema_apply.$dbvrep_schema2_apply.dbrsobj$ where name='$table' and owner_ in 
(select user_ from $dbvrep_schema_apply.$dbvrep_schema2_apply.dbrsuser$ where name = '$SCHEMA'));
go
"@
$FLASHBACK_SCN = sqlcmd -U $dbvrep_user_apply -P "sa" -S (Get-OdbcDsn -Name $dbvrep_db_apply).Attribute["Server"] -b -h-1 -Q $sqlQuery

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
.\start-console.bat read exclude_cols.txt
.\start-console.bat shutdown MINE
.\start-console.bat shutdown APPLY
Start-Sleep -Seconds 10


if ($s.Name.split("_")[1].ToUpper() -eq "MINE") {
	# it's the mine server where script is started
	$target= .\start-console.bat show APPLY_REMOTE_INTERFACE| Select-String -Pattern ^APPLY
	$t= $target -split ' '
	$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

	(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('MINE','APPLY')).start()
	(Get-Service -Name $s.Name).start()
} else {
	# it's the apply server where script is started
	$target= .\start-console.bat show MINE_REMOTE_INTERFACE| Select-String -Pattern ^MINE
	$t= $target -split ' '
	$t_servername=$t[2] | %{$_.Substring(0, $_.length - 5) }

	(Get-Service -ComputerName $t_servername -Name $s.Name.Replace('APPLY','MINE')).start()
	(Get-Service -Name $s.Name).start()
}

Write-Host
Write-Host *****************************************************
Write-Host The dbvisit replication is back. 
Write-Host *****************************************************

exit
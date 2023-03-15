#!/bin/ksh

###############################################################################
# MODIFICATIONS LOG:                                                          #
#-----------------------------------------------------------------------------#
# Date:    DBA:       Description:                                            #
#-----------------------------------------------------------------------------#
#                                                                             #
###############################################################################


print_usage ()
{
   echo "Usage: `basename $1`"
   echo ""
}

##############################################################################
#
#   $1 - Table name
#   $2 - Array size
#   $3 - Table handling flag, 
#        - = skip table
#        y = truncate table before load
#        n = do not truncate before load
#   $4 - Flag to perform direct load
#        y = perform direct load
#   $5 - Specifies the to schema name for the table
#   $6 - Specifies the from schema
#   $7 - Specifies SQL for table delete
#
#   ENVIRONMENT VARIABLES:
#		TO_PSWD - connection string to use for database connection
#		LOGSDIR - directory to place load log files
#		DATADIR - directory to find the table data files (*.dat)
#		OUTSDIR - directory to find the table description files (*.out)
#
##############################################################################
start_table()
{
	export data=${DATADIR}
	export logs=${LOGSDIR}
	export outs=${OUTSDIR}

	# make table name 32 characters (space padded)
	typeset -L32 tblname=$1
	
	# perform the load into the TO_PSWD database
	if [ $3 = "-" ]; then
		echo "SUCCESS: cnvbcp ${tblname}   skipped"
	elif [ ! -f ${data}/$1.dat ]; then
		echo "ERROR:   cnvbcp $1 ${logs}/$1.log"
		echo "ERROR:   cnvbcp ${data}/${1}.dat does not exist" >> ${logs}/$1.log
		return 1
	elif [ ${cmd_genctl_only} -eq 0 -a `/bin/ls -s ${data}/$1.dat 2>/dev/null|awk '{print $1}'` -eq 0 ]; then
		typeset -R9 loaded="*0"
		echo "SUCCESS: cnvbcp ${tblname} ${loaded} Rows loaded"
       
		# truncate if desired
		if [ $3 = "y" ]; then
			trunc_table $1 $5 $7 >> ${logs}/$1.dataload.log
			trunc_table $1 $5 $7
			if [ $? -ne 0 ]; then
				echo "ERROR:   trunc_table ${5}.${1} ${7}, ${logs}/$1.dataload.log"
				return 1
			fi
		fi
	else
		table="$1"
		outfile="${outs}/$1.out"
		datfile="${data}/$1.dat"
		logfile="${logs}/$1.log"
		badfile="${data}/$1.bad"
		array_size="$2"
		if [ $5 = "-" ]; then
			schema=""
		else
			schema="-s ${5}"
		fi

		if [[ $3 == "y" ]]; then
			if [[ $7 == "-" ]]; then
				truncate="-x"
			else
				truncate=""
				trunc_table $1 $5 $7
				if [ $? -ne 0 ]; then
					echo "ERROR:   trunc_table ${5}.${1} ${7}, ${logs}/$1.dataload.log"
					return 1
				fi
			fi
		else
			truncate=""
		fi
      
		loadstart=`date +%s`
		echo cnvbcp -t $table -d $datfile -o $outfile -l $logfile -b $badfile -B 20 -P -a $array_size $schema $truncate >> ${logs}/$1.dataload.log
		cnvbcp -t $table -d $datfile -o $outfile -l $logfile -b $badfile -B 20 -P -a $array_size $schema $truncate >> ${logs}/$1.dataload.log 2>&1
		if [ $? -ne 0 ]; then
			echo "ERROR:   cnvbcp $1 ${logs}/$1.log"
			return 1
		fi
		loadend=`date +%s`

		typeset -R9 loaded=`grep 'rows sent to database' ${logfile} | awk '{print $1}'`
#		typeset -R9 notloaded=`grep 'not loaded due to errors' ${logfile} | awk '{print $1}'`

#		if [ ${notloaded} -ne 0 ]; then
#			echo "ERROR:   cnvbcp $1 ${logs}/$1.log"
#			return 1
#		fi

		difftime $loadstart $loadend
		echo "SUCCESS: cnvbcp ${tblname} ${loaded} Rows loaded ($tdiff)"
	fi

	if [ ${cmd_leave_tmp_files} -eq 0 ]; then
		/bin/rm -f ${logs}/$1.dataload.log
		/bin/rm -f ${logs}/$1.trunc
	fi

	return 0
}

##############################################################################
#
#	$1 - Table Name
#	$2 - Table owner to be truncated
#	$3 - SQL to delete table
#
##############################################################################
trunc_table ()
{
	# process delete table sql if it exists
	if [[ $3 != "-" ]]; then
		sql_file=${SYNPATH}/$3
		if [[ -s $sql_file ]]; then
			cnvsqlcmd -i $sql_file
		else
			echo "ERROR:  sql file [$sql_file] doesn't exist or is empty"
			return 1
		fi
	else
		if [ $2 = "-" ]; then
			schema=""
		else
			schema=$2.
		fi

		#This is ok because schema return dbo.
		cnvsqlcmd -Q "truncate table ${schema}${1};"
	fi

	trunc_table_status=$?
	if [ $trunc_table_status -ne 0 ]; then
	   echo ERROR:   error running SQL statement 
	   return $trunc_table_status
	fi

	return $?
}

##############################################################################
#  run_control_files
#
#  $1 - base name of the control files to process
##############################################################################
run_control_files()
{
	ctlfile=$1;
	base_ctl=`basename ${ctlfile}`;

	let num_child=1
	
	# start a child for each of the control files
	for i in `/bin/ls ${workdir}/${base_ctl}.????`; do
		#set -m
		process_control_file $i &
		child_pid[${num_child}]=$!
		let num_child=${num_child}+1
	done

	# now wait for each child to finish
	let rc=0
	let i=${num_child}-1
	while [ ${i} -gt 0 ]; do
		wait ${child_pid[${i}]}
		let rc=${rc}+$?
		let i=${i}-1
	done

	# display an error if one occurred
	if [ ${rc} -ne 0 ]; then
		echo "ERROR:   ${rc} errors found processing the control file"
	fi

	return ${rc}
}

##############################################################################
#  split_control_file
#
#  $1 - control file to process
##############################################################################
split_control_file()
{
	ctlfile=$1;
	base_ctl=`basename ${ctlfile}`;

	let ctl_num=0;

	# run through the control file
	egrep -v '^#|^$' ${ctlfile} |
		while read i ; do
			# put the line into an array
			set -A tbl_desc $i

			# make sure it is a valid line
			if [ "${tbl_desc[0]}" != "#" -a ${#tbl_desc[*]} -ge 9 ]; then
				# put the line into the split control file
				typeset -Z4 num=${tbl_desc[5]}
				cat >> ${workdir}/${base_ctl}.${num} <<!
$i
!
				split_ctl_status=$?
				if [ ${split_ctl_status} -ne 0 ]; then
					echo "Error writing to ${workdir}/${base_ctl}.${num}"
					echo "   in split_control_file()"
					return ${split_ctl_status}
				fi
			fi
		done
	
	# start all of the control files in parallel
	run_control_files $ctlfile
	rc=$?

	return ${rc}
}

##############################################################################
#  process_control_file
#
#  $1 - control file to process
##############################################################################
process_control_file()
{
	ctlfile=$1;
	let num_errs=0;

	egrep -v '^#|^$' ${ctlfile} |
		while read i ; do
			# put the line into an array
			set -A tbl_desc $i

			# make sure it is a valid line
			if [ "${tbl_desc[0]}" != "#" -a ${#tbl_desc[*]} -ge 9 ]; then
				# set up all of the variables from the control file
				table_name=${tbl_desc[0]}
				array_size=${tbl_desc[1]}
				delete_ctl=${tbl_desc[6]}
				from_owner=${tbl_desc[7]}
				to_owner=${tbl_desc[8]}

				echo $delete_ctl

				# If the user entered a dash, this means that
				# the table SHOULD NOT be imported.  By leaving
				# this a -, the start_table routine will skip
				# the table completely
				if [ ${tbl_desc[2]} = "-" ]; then
					trunc_before="-"
				else
					if [ -z "$cmd_trunc_before_load" ]; then
						trunc_before=${tbl_desc[2]}
					else
						trunc_before="y"
					fi
				fi

				direct_load=${tbl_desc[4]}

				start_table $table_name $array_size $trunc_before $direct_load $to_owner $from_owner $delete_ctl

				# check the status
				if [ $? -ne 0 ]; then
					let num_errs=${num_errs}+1
				fi
			fi
		done

	return ${num_errs}
}


difftime()
{
	start_time=$1
	end_time=$2

	duration=`expr $end_time - $start_time`
	hr=`expr $duration / 3600`
	mn=`expr $duration / 60 % 60`
	sc=`expr $duration % 60`

	sf=""
	if [ $sc -lt 10 ] ; then
		sf="0"
	fi

	mf=""
	if [ $mn -lt 10 ] ; then
		mf="0"
	fi

	hf=""
	if [ $hr -lt 10 ] ; then
		hf="0"
	fi

	tdiff="$hf$hr:$mf$mn:$sf$sc"
}


##############################################################################
#  MAIN PROGRAM STARTS HERE
##############################################################################

# default all fo the variables
unset cmd_array_size
unset control_file
unset cmd_cnvbcp_ctl_file
unset cmd_owner_schema
unset cmd_trunc_before_load
unset cmd_genctl_only
unset exit_error_count

export cmd_skip_index="FALSE"
export cmd_max_procs=0
export cmd_leave_tmp_files=0
export cmd_genctl_only=0
export exit_error_count=2000000000
export recoverable_opt=""

# set up the working directory
workdir=/tmp/`basename $0`.$$; export workdir
mkdir -p $workdir

# get the command line options
while getopts e:f:ims:tuxz c
do
	case $c in 
		e)
			export exit_error_count=${OPTARG}
			;;
		f)
			export cmd_cnvbcp_ctl_file=${OPTARG}
			;;
		i)
			export cmd_skip_index="TRUE"
			;;
		m)
			export cmd_max_procs=1
			;;
		s)
			export control_file=${OPTARG}
			;;
		t)
			export cmd_trunc_before_load="y"
			;;
		u)
			export recoverable_opt="UNRECOVERABLE";
			;;
		x)
			export cmd_leave_tmp_files=1
			;;
		z)
			export cmd_genctl_only=1
			;;
		*)
			print_usage $0
			;;
	esac
done

error=0

# now make sure we have access to the files given on the command line
if [ ! -z "${control_file}" ]; then
	if [ ! -f ${control_file} ]; then
		echo "ERROR:   control file does not exist: ${control_file}"
		error=1
	elif [ ! -r ${control_file} ]; then
		echo "ERROR:   control file is not readable: ${control_file}"
		error=1
	fi
fi

# make sure the environment variables have been set
if [ -z "${TO_PSWD}" ]; then
	echo "ERROR:   environment variable TO_PSWD must be set"
	echo "  i.e. TO_PSWD=username/password@tnsname"
	error=1
fi

if [ -z "${DATADIR}" ]; then
	echo "ERROR:   environment variable DATADIR must be set to the location"
	echo "       of the data files to be loaded"
	error=1
fi

if [ -z "${LOGSDIR}" ]; then
	echo "ERROR:   environment variable LOGSDIR must be set"
	error=1
fi

if [ -z "${OUTSDIR}" ]; then
	echo "ERROR:   environment variable OUTSDIR must be set to the location"
	echo "       of the table description files"
	error=1
fi

if [ $error -ne 0 ]; then
	echo ""
	print_usage $0
	exit ${error}
fi

# make sure the CNVBCP Credentials have been set
export CNVBCP_PSWD=$TO_PSWD

cat ${control_file}

if [ $cmd_max_procs -gt 0 ]; then
	split_control_file ${control_file}
	rc=$?
else
	process_control_file ${control_file}
	rc=$?
fi

echo "Finished:"
# clean up the work directory
if [ ${cmd_leave_tmp_files} -ne 0 ]; then
	echo "\ttemporary files left in ${workdir}"
else
	/bin/rm -rf ${workdir}
fi

if [ ${cmd_genctl_only} -eq 1 -o ${cmd_leave_tmp_files} -ne 0 ]; then
	echo "logs and control files left in ${LOGSDIR}"
fi

exit ${rc}


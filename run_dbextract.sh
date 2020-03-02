#! /bin/bash

MYSQLQUERYFILE=templ_extract_pat.mysql
MYSQLOUTFILE=extraction.csv
INSTMYSQLFILE=inst_extract_pat.sql
INSTMYSQLCOLLATEFILE=inst_collate_extract_pat.sql
TEMPTIMEFILE=time.out
LOGFILE=lg_dbextract.log
SEPARATOR=';'
REPLACEMENTSEPARATOR='|'
PATIENTIDSFILE=testpatids
RM='/bin/rm -f'
CREDENTIALS_FILE=./cred.sh
REGEX_FUNC_FILE=./regex_func.sh
USE_COLLATED_MYSQL_FILE=./use_collated.sh
REGEX_FUNC_DEFAULT=PREG_REPLACE

source $CREDENTIALS_FILE
source $REGEX_FUNC_FILE
source $USE_COLLATED_MYSQL_FILE

reset_file=1
if [ $reset_file -eq 1 ] ; then
   eval $RM $MYSQLOUTFILE
   eval $RM $LOGFILE
fi

timestamp() { date -d "$1" +"%s"; }

convertsecs() {
 h=$(bc <<< "${1}/3600")
 m=$(bc <<< "(${1}%3600)/60")
 s=$(bc <<< "${1}%60")
 printf "%02d:%02d:%02d\n" $h $m $s
}

PATIENTNO=0
PATIENTTOTAL=$(wc -l $PATIENTIDSFILE | awk '{print $1}')
DATETIMECMD='date +"%D %T"'
STARTTIME=$(eval $DATETIMECMD)
STARTTIMESTAMP=$(timestamp "$STARTTIME")
PREVTIMESTAMP=$STARTTIMESTAMP
SEDTRANSFORMATIONS="s/\t/\\$SEPARATOR/g; s/\\s*([Pp]r[^)]*)//g; s/\\s*:\\s*\\$SEPARATOR/\\$SEPARATOR/g"
OLDERSEDTRANSFORMATIONS="s/|;/;/g; s/|$//g; s/;|/;/g; s/:\\s*\[/\[/g;"
TOTALTIMETAKEN=0
SED="LANG=fr_CA.iso88591 sed"
VALIDPIDREGEX="[0-9]{6}"
CONVERTFROMUTF8CMD="iconv -f UTF-8 -t ISO-8859-1//TRANSLIT"
HEADING_ID_PLACEHOLDER='@headingid;'
INTERMED_OUT_FILE=intermed.csv
MYSQL_CMD="mysql --default-character-set=utf8 -B -u $DBUSR -p$DBPWD $DBNAM"
UTF8_COLLATE="COLLATE utf8_unicode_ci"

echo "START    -------- {0.00%, $PATIENTNO/$PATIENTTOTAL, $STARTTIME}"  >> $LOGFILE #Write start time to log file
while read -r -n 6 PATIENTID
do
   if [[ $PATIENTID =~ $VALIDPIDREGEX ]] ; then
   	echo
   	echo "=============="
	echo "PATIENT $PATIENTID"
   	echo "=============="
	echo
   	((PATIENTNO=PATIENTNO+1))
	
	SED_PAT_ID_SUBSTS="s/$HEADING_ID_PLACEHOLDER/\\\"$PATIENTID\\\";/g; s/';'/'\\$SEPARATOR'/g; s/\(@sepreplacement := \)[^;]*;/\1\\'$REPLACEMENTSEPARATOR';/g; s/IF[(]@answered/IF(fr.id IS NOT NULL/g"
	if [ $REGEX_FUNC != $REGEX_FUNC_DEFAULT ] ; then
		SED_PAT_ID_SUBSTS="$SED_PAT_ID_SUBSTS ; s/\(PREG_REPLACE\)[(]\([^,]\+\),\([^,]\+\), \([^)]\+\)[)]/$REGEX_FUNC(\4,\2,\3)/g"
	fi
	SED_SUBST_PRE_CMD="$SED \"$SED_PAT_ID_SUBSTS\" $MYSQLQUERYFILE > $INSTMYSQLFILE"
	echo $SED_SUBST_PRE_CMD
	eval $SED_SUBST_PRE_CMD
	
	SED_SUBST_W_COLLATE="$SED_PAT_ID_SUBSTS ; s/[^_]REPLACE[(][a-z0-9.@_]\+/\0 $UTF8_COLLATE/g; s/;;/ $UTF8_COLLATE;/g; s/WHERE [a-z_]\+ = @[a-z]\+/\0 $UTF8_COLLATE/g"
   	SED_SUBST_CMD="$SED \"$SED_SUBST_W_COLLATE\" $MYSQLQUERYFILE > $INSTMYSQLCOLLATEFILE" #Copy MYSQL query template and substitute in patient id
	echo $SED_SUBST_CMD
	eval $SED_SUBST_CMD
   	
	if [ $USE_COLLATED -eq 1 ] ; then
		INSTMYSQLFILE=$INSTMYSQLCOLLATEFILE
	fi

	CMD1="$MYSQL_CMD -e 'source $INSTMYSQLFILE' > $INTERMED_OUT_FILE"
	CMD2="cat $INTERMED_OUT_FILE | while read; do $SED '$SEDTRANSFORMATIONS' | $CONVERTFROMUTF8CMD; done >> $MYSQLOUTFILE" #Shell command to run MYSQL query and clean up output
   	echo "($PATIENTID) $CMD1"
   	eval $CMD1
	echo $CMD2
	eval $CMD2
   	
	CALCPROGRESSCMD="awk 'BEGIN{printf \"%.2f\", "$PATIENTNO*"100 / $PATIENTTOTAL}'"
   	PROGRESS=$(eval $CALCPROGRESSCMD)
   	CURRENTTIME=$(eval $DATETIMECMD)
   	if [ ! -z "$CURRENTTIMESTAMP" ] ; then
     	   PREVTIMESTAMP=$CURRENTTIMESTAMP
   	fi
   	CURRENTTIMESTAMP=$(timestamp "$CURRENTTIME")
   #ELAPSED=$(awk '/^real/{print $2}' $TEMPTIMEFILE) #Extract duration information from temp file
   	ITERATIONTIMETAKEN=$(($CURRENTTIMESTAMP-$PREVTIMESTAMP))
	TIMETAKEN=$(($TIMETAKEN+$ITERATIONTIMETAKEN))
   	AVERAGEITERATIONTIMETAKEN=$(eval "awk 'BEGIN{printf \"%.2f\", $TIMETAKEN / $PATIENTNO}'")
   	PREDICTEDTOTALTIMETAKEN=$(eval "awk 'BEGIN{printf \"%.2f\", $AVERAGEITERATIONTIMETAKEN * $PATIENTTOTAL}'")
   	#CALCPREDICTEDENDTIMECMD="$DATETIMECMD --date=\"$STARTTIME EDT + $PREDICTEDTOTALTIMETAKEN seconds\""
	#PREDICTEDENDTIME=$(eval $CALCPREDICTEDENDTIMECMD)
   	#PREDICTEDENDTIMESTAMP=$(timestamp "$PREDICTEDENDTIME")
   	#PREDICTEDTIMELEFT=$(($PREDICTEDENDTIMESTAMP-$CURRENTTIMESTAMP))
   	#ITERATIONTIMETAKENSTR=$(convertsecs $ITERATIONTIMETAKEN)
   	LOGFILEENTRY="($PATIENTID) $ITERATIONTIMETAKENSTR {$PROGRESS%, $PATIENTNO/$PATIENTTOTAL, $CURRENTTIME"
   	#if [ $PATIENTNO -lt $PATIENTTOTAL ]; then
      	#   LOGFILEENTRY="$LOGFILEENTRY, END: $PREDICTEDENDTIME [+$(convertsecs $PREDICTEDTIMELEFT)]"
   	#fi
   	LOGFILEENTRY="$LOGFILEENTRY}"
   #else
      	#echo "*** Ignoring entry \"$PATIENTID\" ***"
      	#LOGFILEENTRY="($PATIENTID) 00:00:00 === IGNORED ==="
   fi
   echo $LOGFILEENTRY >> $LOGFILE
done < "$PATIENTIDSFILE"
printf "============================\nTOTAL ELAPSED TIME: %s\n============================" $(convertsecs $TIMETAKEN) >> $LOGFILE

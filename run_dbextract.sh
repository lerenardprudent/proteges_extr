#! /bin/sh

MYSQLQUERYFILE=templ_extract_pat.sql
MYSQLOUTFILE=extracted_pat.csv
INSTMYSQLFILE=inst_extract_pat.sql
TEMPTIMEFILE=time.out
LOGFILE=lg_dbextract.log
SEPARATOR=';'
REPLACEMENTSEPARATOR='|';
PATIENTIDSFILE=testpatids
RM='/bin/rm -f'
CREDENTIALS_FILE=./cred.sh

source $CREDENTIALS_FILE

reset_file=true
options=$(getopt -o c --long cont -- "$@")
eval set -- "$options"

#Handle the single option -c
[ $? -ne 0 ] || {
while true; do
    case "$1" in
    -c)
        reset_file=false
        ;;
    --cont)
        reset_file=false
        ;;
    --)
        shift
        break
        ;;
    esac
    shift
done
}

if [ $reset_file = true ] ; then
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
HEADINGIDPLACEHOLDER="@headingid";
SEDPATIDSUBSTS="s/$HEADINGIDPLACEHOLDER;/\\\"$PATIENTID\\\";/g; s/';'/'\\$SEPARATOR'/g; s/\(@sepreplacement := \)[^;]*;/\1\\'$REPLACEMENTSEPARATOR';/g"

echo "START    -------- {0.00%, $PATIENTNO/$PATIENTTOTAL, $STARTTIME}"  >> $LOGFILE #Write start time to log file
while read -r PATIENTID
do
   if [[ $PATIENTID =~ $VALIDPIDREGEX ]] ; then
   	((PATIENTNO=PATIENTNO+1))
   	SUBSTPATIENTIDSCMD="$SED \"$SEDPATIDSUBSTS\" $MYSQLQUERYFILE > $INSTMYSQLFILE" #Copy MYSQL query template and substitute in patient id
   	echo $SUBSTPATIENTIDSCMD
	eval $SUBSTPATIENTIDSCMD
   	CMD="mysql -B -u $DBUSR -p$DBPWD $DBNAM -e 'source $INSTMYSQLFILE' | while read; do $SED '$SEDTRANSFORMATIONS' | $CONVERTFROMUTF8CMD; done >> $MYSQLOUTFILE" #Shell command to run MYSQL query and clean up output
   #TIMECMD="{ time $CMD; } 2>$TEMPTIMEFILE" #Command to get timing information of previous command and write it to temp file
   	echo "($PATIENTID) $CMD"
   	eval $CMD
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
   	CALCPREDICTEDENDTIMECMD="$DATETIMECMD --date=\"$STARTTIME EDT + $PREDICTEDTOTALTIMETAKEN seconds\""
	PREDICTEDENDTIME=$(eval $CALCPREDICTEDENDTIMECMD)
   	PREDICTEDENDTIMESTAMP=$(timestamp "$PREDICTEDENDTIME")
   	PREDICTEDTIMELEFT=$(($PREDICTEDENDTIMESTAMP-$CURRENTTIMESTAMP))
   	ITERATIONTIMETAKENSTR=$(convertsecs $ITERATIONTIMETAKEN)
   	LOGFILEENTRY="($PATIENTID) $ITERATIONTIMETAKENSTR {$PROGRESS%, $PATIENTNO/$PATIENTTOTAL, $CURRENTTIME"
   	if [ $PATIENTNO -lt $PATIENTTOTAL ]; then
      	   LOGFILEENTRY="$LOGFILEENTRY, END: $PREDICTEDENDTIME [+$(convertsecs $PREDICTEDTIMELEFT)]"
   	fi
   	LOGFILEENTRY="$LOGFILEENTRY}"
   else
      	echo "*** Ignoring entry \"$PATIENTID\" ***"
      	LOGFILEENTRY="($PATIENTID) 00:00:00 === IGNORED ==="
   fi
   echo $LOGFILEENTRY >> $LOGFILE
done < $PATIENTIDSFILE
printf "============================\nTOTAL ELAPSED TIME: %s\n============================" $(convertsecs $TIMETAKEN) >> $LOGFILE

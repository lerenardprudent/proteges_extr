#! /bin/bash

sourceFile() {
    [[ -f "$1" ]] && source "$1"
}

generateExtractionOutfileName() {
	CMD="printf \"extr_%s_%s_%s.csv\" `$MYSQL_CMD -N -e \"select code from form_types where id = $FORM_TYPE_ID\"` `date '+%Y-%m-%d'` `date '+%H~%M~%S'`"
	eval $CMD
}

CREDENTIALS_FILE=./cred.sh
REGEX_FUNC_FILE=./regex_func.sh
REGEX_FUNC=PREG_REPLACE
REGEX_FUNC_DEFAULT=$REGEX_FUNC
USE_COLLATED_MYSQL_FILE=./use_collated.sh
USE_COLLATED=0
DO_SELECT_STD_VAR_EXTRACTION_FILE=./do_std.sh
DO_SELECT_STD_VAR_EXTRACTION_INSTEAD=0
DO__STD_HIST_EXTRACTION_FILE=./do_std_hist.sh
DO_STD_HIST_EXTRACTION_INSTEAD=0

FORM_TYPE_ID=1

sourceFile $CREDENTIALS_FILE
sourceFile $REGEX_FUNC_FILE
sourceFile $USE_COLLATED_MYSQL_FILE
sourceFile $DO_SELECT_STD_VAR_EXTRACTION_FILE
sourceFile $DO__STD_HIST_EXTRACTION_FILE

MYSQL_CMD="mysql -B -u $DBUSR -p$DBPWD $DBNAM"
MYSQL_CMD_W_UTF_CHARSET="$MYSQL_CMD --default-character-set=utf8"

MYSQLQUERYFILE=templ_extract_pat.mysql
INSTMYSQLFILE=inst_extract_pat.sql
INSTMYSQLCOLLATEFILE=inst_collate_extract_pat.sql
TEMPTIMEFILE=time.out
LOGFILE=lg_dbextract.log
SEPARATOR=';'
REPLACEMENTSEPARATOR='|'
PATIENTNO=0
PATIENTIDSFILE=testpatids
PATIENTTOTAL=$(wc -l $PATIENTIDSFILE | awk '{print $1}')
RM='/bin/rm -f'

formatLogFileEntry() {
	PRINTF_CMD="LANG=C printf '(%6s) %10s {%6.2f%%, %3d/%3d, %16s}' $1 $2 $3 $4 $5 $6"
	eval $PRINTF_CMD
}

reset_file=1
if [ $reset_file -eq 1 ] ; then
   eval $RM $LOGFILE
fi

timestamp() { date -d "$1" +"%s"; }

convertsecs() {
 h=$(bc <<< "${1}/3600")
 m=$(bc <<< "(${1}%3600)/60")
 s=$(bc <<< "${1}%60")
 printf "%02d:%02d:%02d\n" $h $m $s
}

printGenerateSpssFileCmd() {
	OUTFILE=`basename $1 sql`
	OUTFILE="${OUTFILE}sps"
	 printf "** TO GENERATE '$OUTFILE': $MYSQL_CMD -N -e 'source $1' | sed 's/\./.\\\n/g' > $OUTFILE\n" 
}
	
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
UTF8_COLLATE="COLLATE utf8_unicode_ci"
RECODE_VARS_SQL_FRAG_FILE=frag_recode_vars.mysql
RECODE_VARS_FRAG=$(cat $RECODE_VARS_SQL_FRAG_FILE | tr '\r\n' ' ' | tr '\r' ' ' | sed 's/\//\\\//g; s/"/\\"/g; s/\@/\\@/g')
RECODE_VARS_SQL_FILE=recode_vars.sql
ADD_VAR_LABELS_SQL_FILE=label_short_vars.sql
ORDER_MYSQL_FRAG=$(sed -n "/ORDER BY.*$/p" $MYSQLQUERYFILE)
PERL_SUBST_1="s/SELECT.*?FROM/$RECODE_VARS_FRAG/s; s/GROUP BY.*$/$ORDER_MYSQL_FRAG/g"
GEN_SPSS_RECODE_VARS_FILE_CMD="perl -i.orig -p0e '$PERL_SUBST_1' $RECODE_VARS_SQL_FILE"
ADD_VAR_LABELS_SQL_FRAG_FILE=frag_add_var_labels.mysql
ADD_VAR_LABELS_FRAG=$(cat $ADD_VAR_LABELS_SQL_FRAG_FILE | sed 's/\//\\\//g; s/"/\\"/g')
PERL_SUBST_2="s/SELECT.*?FROM/$ADD_VAR_LABELS_FRAG/s; s/GROUP BY.*$/$ORDER_MYSQL_FRAG/g"
GEN_SPSS_ADD_VAR_LABELS_FILE_CMD="perl -i.orig -p0e \"$PERL_SUBST_2\" $ADD_VAR_LABELS_SQL_FILE"
STD_SQL_FILE=std_baseline.sql
STD_HIST_SQL_FILE=std_hist.sql
SELECT_STD_HIST_VARS_SQL_FRAG_FILE=frag_std_hist.mysql
SELECT_STD_HIST_VARS_SQL_FRAG=$(cat $SELECT_STD_HIST_VARS_SQL_FRAG_FILE | tr '\r\n' ' ' | tr '\r' ' ' | sed 's/\//\\\//g; s/"/\\"/g; s/\@/\\@/g')

echo "$SELECT_STD_HIST_VARS_SQL_FRAG"


if [ $DO_SELECT_STD_VAR_EXTRACTION_INSTEAD -eq 1 ]; then
	FORM_TYPE_ID=3
elif [ $DO_STD_HIST_EXTRACTION_INSTEAD -eq 1 ]; then
	FORM_TYPE_ID=11
fi
MYSQLOUTFILE=$(generateExtractionOutfileName)

LOG_FIRST_LINE_CMD="formatLogFileEntry 'START' '00:00:00' 0 $PATIENTNO $PATIENTTOTAL $STARTTIME"
eval $LOG_FIRST_LINE_CMD >> $LOGFILE #Write start time to log file
while read -r -n 6 PATIENTID
do
   LOGFILEENTRY=""
   if [[ $PATIENTID =~ $VALIDPIDREGEX ]] ; then
   	echo
   	echo "=============="
	echo "PATIENT $PATIENTID"
   	echo "=============="
	echo
   	((PATIENTNO=PATIENTNO+1))
	
	SED_PAT_ID_SUBSTS="s/$HEADING_ID_PLACEHOLDER/\\\"$PATIENTID\\\";/g; s/';'/'\\$SEPARATOR'/g; s/\(@sepreplacement := \)[^;]*;/\1\\'$REPLACEMENTSEPARATOR';/g; s/IF[(]@answered/IF(fr.id IS NOT NULL/g"
	if [ $REGEX_FUNC != $REGEX_FUNC_DEFAULT ] ; then
		SED_PAT_ID_SUBSTS="$SED_PAT_ID_SUBSTS ; s/\($REGEX_FUNC_DEFAULT\)[(]\([^,]\+\),\([^,]\+\), \([^)]\+\)[)]/$REGEX_FUNC(\4,\2,\3)/g"
	fi
	SED_SUBST_PRE_CMD="$SED \"$SED_PAT_ID_SUBSTS\" $MYSQLQUERYFILE > $INSTMYSQLFILE"
	echo $SED_SUBST_PRE_CMD
	eval $SED_SUBST_PRE_CMD
	
	if [ $USE_COLLATED -eq 1 ] ; then
		SED_SUBST_W_COLLATE="$SED_PAT_ID_SUBSTS ; s/[^_]REPLACE[(][a-z0-9.@_]\+/\0 $UTF8_COLLATE/g; s/;;/ $UTF8_COLLATE;/g; s/WHERE [a-z_]\+ = @[a-z]\+/\0 $UTF8_COLLATE/g"
		SED_SUBST_CMD="$SED \"$SED_SUBST_W_COLLATE\" $MYSQLQUERYFILE > $INSTMYSQLCOLLATEFILE" #Copy MYSQL query template and substitute in patient id
		echo $SED_SUBST_CMD
		eval $SED_SUBST_CMD
		INSTMYSQLFILE=$INSTMYSQLCOLLATEFILE
		MYSQL_CMD=$MYSQL_CMD_W_UTF_CHARSET
	fi
	
	MYSQLINFILE=$INSTMYSQLFILE
		
	if [ $DO_SELECT_STD_VAR_EXTRACTION_INSTEAD -eq 1 ]; then
		GEN_STD_SQL="sed \"s/^JOIN form_part_elem_inputs.*$/\0 JOIN std_vars sv ON fpei.name LIKE CONCAT(sv.varname, '%')/g; s/\(formtype :=\)\s[0-9]\+/\1 $FORM_TYPE_ID/g\" $INSTMYSQLFILE > $STD_SQL_FILE"
		echo $GEN_STD_SQL
		eval $GEN_STD_SQL
		MYSQLINFILE=$STD_SQL_FILE
	elif [ $DO_STD_HIST_EXTRACTION_INSTEAD -eq 1 ]; then
		CONC="CONCAT(fpei.name, '_', idx)"
		CONC2="CONCAT(fpei.name, '_', idx, '_', idy)"
		GEN_STD_HIST_SQL="sed \"s/^LEFT JOIN form_responses.*$/LEFT JOIN ( SELECT form_id, var_name, group_concat(val separator ';') val, 'dummy' id from form_responses group by form_id, var_name) fr ON fr.form_id = f.id and fr.var_name = concat(iz.var_name, '_', iz.idx)/g; s/\(formtype :=\)\s[0-9]\+/\1 $FORM_TYPE_ID/g; s/^JOIN form_part_elem_inputs.*$/\0 JOIN idxs iz ON fpei.name = iz.var_name/g; s/^\(\s\+ORDER BY\).*$/\1 idx, pos/g\" $INSTMYSQLFILE > $STD_HIST_SQL_FILE"
		
		echo $GEN_STD_HIST_SQL
		eval $GEN_STD_HIST_SQL
		
		PERL_SUBST3_CMD="perl -i.orig -p0e \"s/\@vn := fpei.*?AS rpnse/$SELECT_STD_HIST_VARS_SQL_FRAG/s; s/GROUP_CONCAT.*?ORDER/GROUP_CONCAT(IF(heading_row, clust_name, clust_val) ORDER/s\" $STD_HIST_SQL_FILE"
		echo $PERL_SUBST3_CMD
		eval $PERL_SUBST3_CMD
		
		MYSQLINFILE=$STD_HIST_SQL_FILE
	fi	
	
	CMD1="$MYSQL_CMD -e 'source $MYSQLINFILE' > $INTERMED_OUT_FILE"
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
   	ITERATIONTIMETAKENSTR=$(convertsecs $ITERATIONTIMETAKEN)
	
   	#if [ $PATIENTNO -lt $PATIENTTOTAL ]; then
      	#   LOGFILEENTRY="$LOGFILEENTRY, END: $PREDICTEDENDTIME [+$(convertsecs $PREDICTEDTIMELEFT)]"
   	#fi
   	
	LOGFILEENTRY=$(formatLogFileEntry $PATIENTID $ITERATIONTIMETAKENSTR $PROGRESS $PATIENTNO $PATIENTTOTAL $CURRENTTIME)
   
   #else
      	#echo "*** Ignoring entry \"$PATIENTID\" ***"
      	#LOGFILEENTRY="($PATIENTID) 00:00:00 === IGNORED ==="
   fi
   
   if [ ! -z "$LOGFILEENTRY" ]; then
		echo $LOGFILEENTRY >> $LOGFILE
   fi
done < "$PATIENTIDSFILE"
printf "============================\nTOTAL ELAPSED TIME: %s\n============================" $(convertsecs $TIMETAKEN) >> $LOGFILE

cp $INSTMYSQLFILE $RECODE_VARS_SQL_FILE
echo $GEN_SPSS_RECODE_VARS_FILE_CMD
eval $GEN_SPSS_RECODE_VARS_FILE_CMD

cp $INSTMYSQLFILE $ADD_VAR_LABELS_SQL_FILE
echo $GEN_SPSS_ADD_VAR_LABELS_FILE_CMD
eval $GEN_SPSS_ADD_VAR_LABELS_FILE_CMD

printGenerateSpssFileCmd $RECODE_VARS_SQL_FILE
printGenerateSpssFileCmd $ADD_VAR_LABELS_SQL_FILE
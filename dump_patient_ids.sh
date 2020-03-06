#! /bin/sh

source ./cred.sh
CMD="mysql -u$DBUSR -p\"$DBPWD\" $DBNAM -N -e 'select idsubj from patients'"
#echo $CMD
eval $CMD

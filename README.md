# proteges_extr
To perform extraction:
1) Run run_dbextract.sh. An output CSV file will be created and the commands to generate SPSS syntax files will be output to the screen.
2) Run the commands displayed to create the syntax files.
3) Load the CSV file in Excel and save it in XLSX format (which will convert all date strings in such a way that SPSS will understand them)
4) Load the XLS file in SPSS.
5) Load and execute the syntax files created at step 2.
6) Delete the endmost 'FIN' column.
7) Save to extraction.sav.

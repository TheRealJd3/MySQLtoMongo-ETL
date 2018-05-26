# MySQLtoMongo-ETL
ETL of data from MySQL database to a NOSQL database(MongoDB)
Steps to run
1-      Start XAMPP and  run MySQL(https://www.apachefriends.org/download.html)
2-      Go to XAMPP Control panel and click “Shell”
3-      In Shell, type the following : 
           mysql –u root 
4-      At the  prompt, type :create database flights;
5-      At the new prompt, type : use flights;
6-      At the new prompt, type source together with the path to the file on the desktop (dragging it to the command line will work) – no semi-colon!
7-      At the new prompt type show tables; and you should find flights there.
8-      To complete the transfer to NoSQL, download and run MongoDB
9-      Run ETLscript.py to complete the ETL process 


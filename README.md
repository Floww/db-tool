# db-tool
Simple database tool, nothing really amazing.

Database supported : 
- Oracle 11G only.

Make a build
--------------
Execute "package" maven lifecycle to make a build to "bin" directory.

db.properties
--------------
Next to the JAR file in "bin" directory, this file references database connection informations.

 	db.type=ORACLE11G
 	db.jdbc.driverClassName=oracle.jdbc.OracleDriver
 	db.jdbc.url=jdbc:oracle:thin:@//host:port/instance
 	db.jdbc.username=user
 	db.jdbc.password=pwd

Using tool
--------------
Usage : java -jar db-tool.jar -schema &amp;lt;schema> [OPTIONS] [TABLE_1] [TABLE_2] ...

Export SQL dump to file.

 -ddl            Generate DDL SQL
 
 -schema &lt;arg>   Database schema
 
 -syncdml        Generate Sync DML SQL

Usage example (2 tables to synchronize)
--------------

In command line :
- cd bin
- java -jar db-tool.jar -schema APA1 -syncdml APA_SERVER_TYPE APA_SERVER_STATE

Output :
- File SYNC_DML_APA_SERVER_TYPE.sql
- File SYNC_DML_APA_SERVER_STATE.sql

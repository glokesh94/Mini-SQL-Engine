Mini-Sql-Engine
===============

A mini SQL engine capable of handling the basic SQL queries using system calls only.

The program reads from the schema table and accesses individual table for answering the queries.

The engine supports the following queries :
- select
- project
- distinct
- join

If  "sql qyery | file name"  is typed, the results are written in the file. 

 **How to Use**
- Create one file which contains schema for both relations and one file for tuples of each table.
- Schema file should be name *'schema'* and files containing tuples of each table should have .csv extension.
- Compile the code using command:

		g++ engine.cpp
- Run the executable.
 
		./a.out
- Then a prompt should appear. Now the user can start typing the Sql queries.
 	
		<megatron_sql_engine> select * from table where attribute value = 'abc';


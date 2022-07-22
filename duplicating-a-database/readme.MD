# Duplicating-a-database
<ul>
<li>DuplicatingDatabasePython.sql: This program uses a pythong stored procedure to recreate the tables of a share into another database.  'Describe share' cannot run in a stored procedure and must be executed beforehand.  The code has three components:
<ol>
<li>Setting the context and describe the share to create a list of tables to be recreated. A user can manually create this table if they prefer</li>
<li>Creating the stored function that iterates through our list of tables and runs a CTAS statement </li>
<li>Calling the function </li>
</ol>
</li>
</ul>

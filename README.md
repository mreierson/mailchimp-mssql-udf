# mailchimp-mssql-udf - Interact directly with MailChimp's API from MS SQL Server  

This project maps MailChimp's API as a set of MS SQL Server user-defined functions.  These UDFs allow MailChimp data to be queried alongside relational database tables.

## Usage

* Return all defined lists  
`SELECT * FROM dbo.mcsql_clr_lists_list(@apikey)`  

* Return all members of a list  
`SELECT `  
`members.* `  
`FROM  `  
`(SELECT * FROM dbo.mcsql_clr_lists_list(@apikey) WHERE name=@name) lists `  
`CROSS APPLY dbo.mcsql_clr_lists_members(@apikey, lists.id, default) members`  

*  Return all list member merge variables for a list  
`SELECT`  
`vars.*`  
`FROM`  
`(SELECT * FROM dbo.mcsql_clr_lists_list(@apikey) WHERE name=@name) list `  
`CROSS APPLY dbo.mcsql_clr_lists_members(@apikey, list.id, default) members  `  
`CROSS APPLY dbo.mcsql_clr_lists_member_merge_vars(@apikey, list.id, members.email, default,   default) vars`

*  Return all merge variable values for a specific member  
`SELECT * FROM dbo.mcsql_clr_lists_member_merge_vars(@apikey, @listid, 'username@domain.com', default, default)`

*  Add a merge variable to a list  
`EXEC dbo.mcsql_lists_merge_var_add @apikey, @listid, 'FAV_COLOR', 'Favorite Color', @opt_field_type = 'dropdown', @opt_choices='Red,Green,Blue'`

*  Update a merge variable  
`EXEC dbo.mcsql_lists_merge_var_update @apikey, @listid, 'FAV_COLOR', @opt_choices = 'Red,Green,Blue,Purple'`

*  Set a merge variable value for a member  
`EXEC dbo.mcsql_lists_member_merge_var_set @apikey, @listid, @email = 'username@domain.com', @tag = 'FAV_COLOR', @value = 'Purple'`

*  Update merge variable values for all list members  
`BEGIN`  
`  DECLARE @apikey nvarchar(255) = 'API-KEY';`  
`  DECLARE @listname nvarchar(255) = ‘mylist01’;`  
`  DECLARE @listid nvarchar(255);`  
` `  
`  SELECT @listid = id `  
`  FROM dbo.mcsql_clr_lists_list(@apikey) WHERE name=@listname;`  
``  
`  SELECT * INTO #MembersToUpdate`  
`  FROM dbo.mcsql_clr_lists_members(@apikey, @listid, default) members`  
`  JOIN MyDatabaseTable ON members.Email = MyDatabaseTable.Email;`  
` `
`  WHILE EXISTS (SELECT * FROM #MembersToUpdate)`  
`  BEGIN`  
`    DECLARE @member_email nvarchar(255);`  
`    DECLARE @merge_var_value nvarchar(255);`  
` `
`    SELECT TOP 1`  
`    @member_email = email,`  
`    @merge_var_value = my_value_column`  
`    FROM #MembersToUpdate;`  
` `  
`    EXEC dbo.mcsql_lists_member_merge_var_set @apikey, @listid, `  
`    @email = @member_email, @tag = 'FAV_COLOR', `  
`    @value = @merge_var_value;`  
` `  
`    DELETE FROM #MembersToUpdate WHERE email = @member_email;`  
`    END`  
`    DROP TABLE #MembersToUpdate`  
`END`  

## Notes
*  The mcsql_clr_ prefix is used for all functions and stored procedures defined in the CLR Assembly/DLL.  There’s another set of stored procedures with a mcsql_ prefix that wrap the CLR procedures to give better usability with respect to optional parameters as they are not supported by CLR procedures/functions.  For the functions, the ‘default’ keyword should be used if a value isn’t specified.

*  The naming of the functions & procedures correspond closely to MailChimp’s API naming.  The deviations occur when dealing with hierarchical data that needed to be flattened to work in an SQL environment.

*  The table-valued functions are handling paging internally and returning full datasets.  Paging functionality corresponds well to a web client, but can be cumbersome when working with the datasets in SQL.

*  Filtering and sorting parameters are set to defaults internally with the intention to use SQL to provide that functionality.

*  Array parameters should be specified as strings of comma separated values.  Likewise, returned arrays will be joined into a single CSV string.

*  String sizes are currently defined to 255, which isn’t completely arbritary as it’s maximum size for the merge variables, but may need to be increased for some of the other columns.

### Terminating the backup operation {#s3_forget}

To minimize the impact of the backup process on user load indicators, data is sent from table copies. Before sending data from YDB, the backup process creates consistent copies of all the tables being sent to YDB. As they're made using the copy-on-write technique, there is almost no change in the size of the DB at the time of their creation. Once the data is sent, the created copies remain in the DB.

To delete the table copies from the DB and the completed operation from the list of operations, run the command:

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH operation forget 'ydb://export/6?id=283824558378666&kind=s3'
```
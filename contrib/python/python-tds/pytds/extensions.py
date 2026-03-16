#: Transaction can read uncommitted data
ISOLATION_LEVEL_READ_UNCOMMITTED = 1

#: Transaction can read only committed data, will block on attempt
#: to read modified uncommitted data
ISOLATION_LEVEL_READ_COMMITTED = 2

#: Transaction will place lock on read records, other transactions
#: will block trying to modify such records
ISOLATION_LEVEL_REPEATABLE_READ = 3

#: Transaction will lock tables to prevent other transactions
#: from inserting new data that would match selected recordsets
ISOLATION_LEVEL_SERIALIZABLE = 4

#: Allows non-blocking consistent reads on a snapshot for transaction without
#: blocking other transactions changes
ISOLATION_LEVEL_SNAPSHOT = 5

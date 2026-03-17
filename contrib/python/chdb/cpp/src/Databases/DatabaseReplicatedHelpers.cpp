#include <Databases/DatabaseReplicatedHelpers.h>
#include <Databases/DatabaseReplicated.h>

namespace DB_CHDB
{

String getReplicatedDatabaseShardName(const DatabasePtr & database)
{
    return assert_cast<const DatabaseReplicated *>(database.get())->getShardName();
}

String getReplicatedDatabaseReplicaName(const DatabasePtr & database)
{
    return assert_cast<const DatabaseReplicated *>(database.get())->getReplicaName();
}

}

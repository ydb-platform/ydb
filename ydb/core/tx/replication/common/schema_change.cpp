#include "schema_change.h"

#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication {

// Older readers discarded index metadata. Its absence says nothing about
// indexes; when both snapshots provide it, compare it along with the schema.
bool IsSameSchemaChange(const NKikimrReplication::TSchemaChange& lhs,
        const NKikimrReplication::TSchemaChange& rhs)
{
    if (lhs.HasIndexes() == rhs.HasIndexes()) {
        return lhs.SerializeAsString() == rhs.SerializeAsString();
    }

    auto left = lhs;
    auto right = rhs;
    left.ClearIndexes();
    right.ClearIndexes();
    return left.SerializeAsString() == right.SerializeAsString();
}


}

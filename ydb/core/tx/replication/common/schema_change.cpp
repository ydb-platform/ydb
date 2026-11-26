#include "schema_change.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NReplication {

bool HasFulltextIndexMetadata(const NKikimrReplication::TSchemaChange& schema) {
    for (const auto& index : schema.GetIndexes().GetItems()) {
        if (index.HasFulltextSettings()) {
            return true;
        }
    }
    return false;
}

void ClearFulltextIndexMetadata(NKikimrReplication::TSchemaChange& schema) {
    if (schema.HasIndexes()) {
        for (auto& index : *schema.MutableIndexes()->MutableItems()) {
            index.ClearFulltextSettings();
        }
    }
}

// Call only after comparing the two reports. Their index entries then match in order.
bool MergeSchemaIndexMetadata(NKikimrReplication::TSchemaChange& into,
        const NKikimrReplication::TSchemaChange& from)
{
    if (!from.HasIndexes()) {
        return false;
    }
    if (!into.HasIndexes()) {
        into.MutableIndexes()->CopyFrom(from.GetIndexes());
        return true;
    }

    bool changed = false;
    for (ui32 i = 0; i < from.GetIndexes().ItemsSize(); ++i) {
        const auto& incoming = from.GetIndexes().GetItems(i);
        auto& index = *into.MutableIndexes()->MutableItems(i);
        if (incoming.HasFulltextSettings() && !index.HasFulltextSettings()) {
            index.MutableFulltextSettings()->CopyFrom(incoming.GetFulltextSettings());
            changed = true;
        }
    }
    return changed;
}

bool IsSameSchemaChange(const NKikimrReplication::TSchemaChange& lhs,
        const NKikimrReplication::TSchemaChange& rhs)
{
    if (lhs.SerializeAsString() == rhs.SerializeAsString()) {
        return true;
    }

    auto left = lhs;
    auto right = rhs;
    if (left.HasIndexes() != right.HasIndexes()) {
        left.ClearIndexes();
        right.ClearIndexes();
    } else if (left.GetIndexes().ItemsSize() == right.GetIndexes().ItemsSize()) {
        for (ui32 i = 0; i < left.GetIndexes().ItemsSize(); ++i) {
            auto* leftIndex = left.MutableIndexes()->MutableItems(i);
            auto* rightIndex = right.MutableIndexes()->MutableItems(i);
            if (!leftIndex->HasFulltextSettings() || !rightIndex->HasFulltextSettings()) {
                leftIndex->ClearFulltextSettings();
                rightIndex->ClearFulltextSettings();
            }
        }
    }
    return left.SerializeAsString() == right.SerializeAsString();
}

}

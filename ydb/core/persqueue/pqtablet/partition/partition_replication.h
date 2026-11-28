#pragma once

#include <deque>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/common/key.h>

namespace NKikimr::NPQ {

class TSchemaChangesIndex {
public:
    void AddKey(const TPartitionId& partition, ui64 schemaVersion, ui64 endOffset);
    void AddChange(NKikimrClient::TKeyValueRequest& request, const TPartitionId& partition, uint64_t endOffset, uint64_t schemaVersion, const NKikimrPQ::TPQTabletConfig& config);
    std::vector<TSchemaChangesKey> Lookup(ui64 offset) const;

private:
    std::deque<TSchemaChangesKey> Keys;
};

} // namespace NKikimr::NPQ

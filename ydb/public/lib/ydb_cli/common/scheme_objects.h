#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/generic/is_in.h>
#include <util/generic/vector.h>

#include <algorithm>

namespace NYdb::NConsoleClient {

// Scheme objects that `ydb export` and `ydb tools list-objects` walk.
inline bool IsExportableSchemeObject(const NScheme::TSchemeEntry& entry) {
    return IsIn({
        NScheme::ESchemeEntryType::Table,
        NScheme::ESchemeEntryType::ColumnTable,
        NScheme::ESchemeEntryType::View,
        NScheme::ESchemeEntryType::Topic,
    }, entry.Type);
}

// Async replicas are created by replication and are not exported as their own items.
inline bool IsAsyncReplicaTable(const NTable::TTableDescription& description) {
    const auto& attributes = description.GetAttributes();
    const auto it = attributes.find("__async_replica");
    return it != attributes.end() && it->second == "true";
}

// Describes each row table and drops async replicas. A describe error is returned as TStatus
// so the caller can retry it inside RetryOperationSync.
inline TStatus RemoveAsyncReplicaTables(NTable::TSession& session, TVector<NScheme::TSchemeEntry>& entries) {
    try {
        std::erase_if(entries, [&session](const NScheme::TSchemeEntry& entry) {
            if (entry.Type != NScheme::ESchemeEntryType::Table) {
                return false;
            }
            const auto describeResult = session.DescribeTable(entry.Name).ExtractValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(describeResult);
            return IsAsyncReplicaTable(describeResult.GetTableDescription());
        });
    } catch (NStatusHelpers::TYdbErrorException& ex) {
        return ex.ExtractStatus();
    }
    return TStatus(EStatus::SUCCESS, {});
}

} // namespace NYdb::NConsoleClient

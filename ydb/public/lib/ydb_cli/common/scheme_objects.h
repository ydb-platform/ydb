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

// Row and column tables marked __async_replica or __omit_backup are not exported.
// Other objects are filtered by the export service, which can read their scheme
// attributes without extending the public API.
inline bool IsNotExportableTable(const NTable::TTableDescription& description) {
    const auto& attributes = description.GetAttributes();
    for (const auto* name : {"__async_replica", "__omit_backup"}) {
        const auto it = attributes.find(name);
        if (it != attributes.end() && it->second == "true") {
            return true;
        }
    }
    return false;
}

// Describes each row or column table and drops ones that must not be exported.
// A describe error is returned as TStatus so the caller can retry it inside RetryOperationSync.
inline TStatus RemoveNotExportableTables(NTable::TSession& session, TVector<NScheme::TSchemeEntry>& entries) {
    try {
        std::erase_if(entries, [&session](const NScheme::TSchemeEntry& entry) {
            if (entry.Type != NScheme::ESchemeEntryType::Table
                && entry.Type != NScheme::ESchemeEntryType::ColumnTable)
            {
                return false;
            }
            const auto describeResult = session.DescribeTable(entry.Name).ExtractValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(describeResult);
            return IsNotExportableTable(describeResult.GetTableDescription());
        });
    } catch (NStatusHelpers::TYdbErrorException& ex) {
        return ex.ExtractStatus();
    }
    return TStatus(EStatus::SUCCESS, {});
}

} // namespace NYdb::NConsoleClient

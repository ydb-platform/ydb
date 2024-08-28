#pragma once

#include <cstddef>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NJaegerTracing {

enum class ERequestType: size_t {
    UNSPECIFIED,
    
    BSCONFIG_DEFINE,
    BSCONFIG_FETCH,

    KEYVALUE_CREATEVOLUME,
    KEYVALUE_DROPVOLUME,
    KEYVALUE_ALTERVOLUME,
    KEYVALUE_DESCRIBEVOLUME,
    KEYVALUE_LISTLOCALPARTITIONS,
    KEYVALUE_ACQUIRELOCK,
    KEYVALUE_EXECUTETRANSACTION,
    KEYVALUE_READ,
    KEYVALUE_READRANGE,
    KEYVALUE_LISTRANGE,
    KEYVALUE_GETSTORAGECHANNELSTATUS,

    TABLE_CREATESESSION,
    TABLE_KEEPALIVE,
    TABLE_ALTERTABLE,
    TABLE_CREATETABLE,
    TABLE_DROPTABLE,
    TABLE_DESCRIBETABLE,
    TABLE_COPYTABLE,
    TABLE_COPYTABLES,
    TABLE_RENAMETABLES,
    TABLE_EXPLAINDATAQUERY,
    TABLE_EXECUTESCHEMEQUERY,
    TABLE_BEGINTRANSACTION,
    TABLE_DESCRIBETABLEOPTIONS,
    TABLE_DELETESESSION,
    TABLE_COMMITTRANSACTION,
    TABLE_ROLLBACKTRANSACTION,
    TABLE_PREPAREDATAQUERY,
    TABLE_EXECUTEDATAQUERY,
    TABLE_BULKUPSERT,
    TABLE_STREAMEXECUTESCANQUERY,
    TABLE_STREAMREADTABLE,
    TABLE_READROWS,

    QUERY_EXECUTEQUERY,
    QUERY_EXECUTESCRIPT,
    QUERY_FETCHSCRIPTRESULTS,
    QUERY_CREATESESSION,
    QUERY_DELETESESSION,
    QUERY_ATTACHSESSION,
    QUERY_BEGINTRANSACTION,
    QUERY_COMMITTRANSACTION,
    QUERY_ROLLBACKTRANSACTION,

    DISCOVERY_WHOAMI,
    DISCOVERY_NODEREGISTRATION,
    DISCOVERY_LISTENDPOINTS,

    REQUEST_TYPES_CNT, // Add new types above this line
};

static constexpr size_t kRequestTypesCnt = static_cast<size_t>(ERequestType::REQUEST_TYPES_CNT);

static const THashMap<TStringBuf, ERequestType> NameToRequestType = {
    {"KeyValue.CreateVolume", ERequestType::KEYVALUE_CREATEVOLUME},
    {"KeyValue.DropVolume", ERequestType::KEYVALUE_DROPVOLUME},
    {"KeyValue.AlterVolume", ERequestType::KEYVALUE_ALTERVOLUME},
    {"KeyValue.DescribeVolume", ERequestType::KEYVALUE_DESCRIBEVOLUME},
    {"KeyValue.ListLocalPartitions", ERequestType::KEYVALUE_LISTLOCALPARTITIONS},
    {"KeyValue.AcquireLock", ERequestType::KEYVALUE_ACQUIRELOCK},
    {"KeyValue.ExecuteTransaction", ERequestType::KEYVALUE_EXECUTETRANSACTION},
    {"KeyValue.Read", ERequestType::KEYVALUE_READ},
    {"KeyValue.ReadRange", ERequestType::KEYVALUE_READRANGE},
    {"KeyValue.ListRange", ERequestType::KEYVALUE_LISTRANGE},
    {"KeyValue.GetStorageChannelStatus", ERequestType::KEYVALUE_GETSTORAGECHANNELSTATUS},

    {"Table.CreateSession", ERequestType::TABLE_CREATESESSION},
    {"Table.KeepAlive", ERequestType::TABLE_KEEPALIVE},
    {"Table.AlterTable", ERequestType::TABLE_ALTERTABLE},
    {"Table.CreateTable", ERequestType::TABLE_CREATETABLE},
    {"Table.DropTable", ERequestType::TABLE_DROPTABLE},
    {"Table.DescribeTable", ERequestType::TABLE_DESCRIBETABLE},
    {"Table.CopyTable", ERequestType::TABLE_COPYTABLE},
    {"Table.CopyTables", ERequestType::TABLE_COPYTABLES},
    {"Table.RenameTables", ERequestType::TABLE_RENAMETABLES},
    {"Table.ExplainDataQuery", ERequestType::TABLE_EXPLAINDATAQUERY},
    {"Table.ExecuteSchemeQuery", ERequestType::TABLE_EXECUTESCHEMEQUERY},
    {"Table.BeginTransaction", ERequestType::TABLE_BEGINTRANSACTION},
    {"Table.DescribeTableOptions", ERequestType::TABLE_DESCRIBETABLEOPTIONS},
    {"Table.DeleteSession", ERequestType::TABLE_DELETESESSION},
    {"Table.CommitTransaction", ERequestType::TABLE_COMMITTRANSACTION},
    {"Table.RollbackTransaction", ERequestType::TABLE_ROLLBACKTRANSACTION},
    {"Table.PrepareDataQuery", ERequestType::TABLE_PREPAREDATAQUERY},
    {"Table.ExecuteDataQuery", ERequestType::TABLE_EXECUTEDATAQUERY},
    {"Table.BulkUpsert", ERequestType::TABLE_BULKUPSERT},
    {"Table.StreamExecuteScanQuery", ERequestType::TABLE_STREAMEXECUTESCANQUERY},
    {"Table.StreamReadTable", ERequestType::TABLE_STREAMREADTABLE},
    {"Table.ReadRows", ERequestType::TABLE_READROWS},

    {"Query.ExecuteQuery", ERequestType::QUERY_EXECUTEQUERY},
    {"Query.ExecuteScript", ERequestType::QUERY_EXECUTESCRIPT},
    {"Query.FetchScriptResults", ERequestType::QUERY_FETCHSCRIPTRESULTS},
    {"Query.CreateSession", ERequestType::QUERY_CREATESESSION},
    {"Query.DeleteSession", ERequestType::QUERY_DELETESESSION},
    {"Query.AttachSession", ERequestType::QUERY_ATTACHSESSION},
    {"Query.BeginTransaction", ERequestType::QUERY_BEGINTRANSACTION},
    {"Query.CommitTransaction", ERequestType::QUERY_COMMITTRANSACTION},
    {"Query.RollbackTransaction", ERequestType::QUERY_ROLLBACKTRANSACTION},

    {"Discovery.WhoAmI", ERequestType::DISCOVERY_WHOAMI},
    {"Discovery.NodeRegistration", ERequestType::DISCOVERY_NODEREGISTRATION},
    {"Discovery.ListEndpoints", ERequestType::DISCOVERY_LISTENDPOINTS},
};

struct TRequestDiscriminator {
    ERequestType RequestType = ERequestType::UNSPECIFIED;
    TMaybe<TString> Database = NothingObject;

    static const TRequestDiscriminator EMPTY;
};

} // namespace NKikimr::NJaegerTracing

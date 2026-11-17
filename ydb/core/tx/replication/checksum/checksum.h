#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_handle.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

namespace NKikimr::NReplication::NChecksum {

struct TEvResult: TEventLocal<TEvResult, EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER) + 100> {
    Ydb::Replication::StreamChecksumTablesInternalResponse Record;
};

struct TSettings {
    TString Database;
    ui32 MaxShardsInFlight = 0;
    TMaybe<TString> UserToken;
    Ydb::Replication::StreamChecksumTablesInternalRequest Request;
    TInstant Deadline;
    // Destination callers transfer a hold acquired at CoordinatorPlanned.
    TMaybe<TRowVersion> Snapshot;
    NKqp::TSnapshotHandle SnapshotHandle;
    // Verify that a destination path still names the table whose snapshot was retained.
    TVector<TTableId> ExpectedTables;
};

TRowVersion FromProto(const Ydb::VirtualTimestamp& version);
void ToProto(TRowVersion version, Ydb::VirtualTimestamp* proto);
bool Equal(const Ydb::Replication::TableChecksum& lhs, const Ydb::Replication::TableChecksum& rhs);

IActor* CreateChecksumTables(const TActorId& parent, TSettings settings);

} // namespace NKikimr::NReplication::NChecksum

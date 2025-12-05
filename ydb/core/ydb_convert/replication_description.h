#pragma once

#include <ydb/core/protos/replication.pb.h>

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <util/generic/fwd.h>

namespace NKikimrSchemeOp {
    class TReplicationDescription;
    class TDirEntry;
}

namespace NYql {
    class TIssue;
}

namespace NKikimr {

void ConvertConnectionParams(
    const NKikimrReplication::TConnectionParams& from,
    Ydb::Replication::ConnectionParams& to);

void ConvertConsistencySettings(
    const NKikimrReplication::TConsistencySettings& from,
    Ydb::Replication::DescribeReplicationResult& to);

void ConvertItem(
    const NKikimrReplication::TReplicationConfig::TTargetSpecific::TTarget& from,
    Ydb::Replication::DescribeReplicationResult::Item& to);

void ConvertStats(
    const NKikimrReplication::TReplicationState& from,
    Ydb::Replication::DescribeReplicationResult& to);

void ConvertStats(
    const NKikimrReplication::TReplicationState&,
    Ydb::Replication::DescribeTransferResult&);

template<typename T>
void ConvertState(const NKikimrReplication::TReplicationState& from, T& to) {
    switch (from.GetStateCase()) {
    case NKikimrReplication::TReplicationState::kStandBy:
        to.mutable_running();
        ConvertStats(from, to);
        break;
    case NKikimrReplication::TReplicationState::kError:
        *to.mutable_error()->mutable_issues() = from.GetError().GetIssues();
        break;
    case NKikimrReplication::TReplicationState::kDone:
        to.mutable_done();
        break;
    case NKikimrReplication::TReplicationState::kPaused:
        to.mutable_paused();
        break;
    default:
        break;
    }
}

bool FillReplicationDescription(
    Ydb::Replication::DescribeReplicationResult& out,
    const NKikimrSchemeOp::TReplicationDescription inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry,
    Ydb::StatusIds_StatusCode& status,
    TString& error);

} // namespace NKikimr

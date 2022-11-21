#pragma once

#include <ydb/core/kqp/common/kqp_common.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>

#include <library/cpp/actors/core/actor.h>


namespace NKikimr {
namespace NKqp {

struct TEvKqpSnapshot {
    struct TEvCreateSnapshotRequest : public TEventLocal<TEvCreateSnapshotRequest,
        TKqpSnapshotEvents::EvCreateSnapshotRequest>
    {
        explicit TEvCreateSnapshotRequest(const TVector<TString>& tables)
            : Tables(tables)
            , MvccSnapshot(false){}

        explicit TEvCreateSnapshotRequest()
            : Tables({})
            , MvccSnapshot(true){}

        const TVector<TString> Tables;
        const bool MvccSnapshot;
    };

    struct TEvCreateSnapshotResponse : public TEventLocal<TEvCreateSnapshotResponse,
        TKqpSnapshotEvents::EvCreateSnapshotResponse>
    {
        TEvCreateSnapshotResponse(const IKqpGateway::TKqpSnapshot& snapshot,
            NKikimrIssues::TStatusIds::EStatusCode status, NYql::TIssues&& issues)
            : Snapshot(snapshot)
            , Status(status)
            , Issues(std::move(issues)) {}

        const IKqpGateway::TKqpSnapshot Snapshot;
        const NKikimrIssues::TStatusIds::EStatusCode Status;
        const NYql::TIssues Issues;
    };

    struct TEvDiscardSnapshot : public TEventLocal<TEvDiscardSnapshot, TKqpSnapshotEvents::EvDiscardSnapshot> {
        explicit TEvDiscardSnapshot(const IKqpGateway::TKqpSnapshot& snapshot)
            : Snapshot(snapshot)
        {}
        const IKqpGateway::TKqpSnapshot Snapshot;
    };
};

NActors::IActor* CreateKqpSnapshotManager(const TString& database, TDuration queryTimeout);

} // namespace NKqp
} // namespace NKikimr

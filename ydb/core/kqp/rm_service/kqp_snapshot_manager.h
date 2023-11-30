#pragma once

#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr {
namespace NKqp {

struct TEvKqpSnapshot {
    struct TEvCreateSnapshotRequest : public TEventLocal<TEvCreateSnapshotRequest,
        TKqpSnapshotEvents::EvCreateSnapshotRequest>
    {
        explicit TEvCreateSnapshotRequest(const TVector<TString>& tables, ui64 cookie, NLWTrace::TOrbit&& orbit = {})
            : Tables(tables)
            , MvccSnapshot(false)
            , Orbit(std::move(orbit))
            , Cookie(cookie) {}

        explicit TEvCreateSnapshotRequest(ui64 cookie, NLWTrace::TOrbit&& orbit = {})
            : Tables({})
            , MvccSnapshot(true)
            , Orbit(std::move(orbit))
            , Cookie(cookie) {}

        const TVector<TString> Tables;
        const bool MvccSnapshot;
        NLWTrace::TOrbit Orbit;
        ui64 Cookie;
    };

    struct TEvCreateSnapshotResponse : public TEventLocal<TEvCreateSnapshotResponse,
        TKqpSnapshotEvents::EvCreateSnapshotResponse>
    {
        TEvCreateSnapshotResponse(const IKqpGateway::TKqpSnapshot& snapshot,
            NKikimrIssues::TStatusIds::EStatusCode status, NYql::TIssues&& issues, NLWTrace::TOrbit&& orbit)
            : Snapshot(snapshot)
            , Status(status)
            , Issues(std::move(issues))
            , Orbit(std::move(orbit)) {}

        const IKqpGateway::TKqpSnapshot Snapshot;
        const NKikimrIssues::TStatusIds::EStatusCode Status;
        const NYql::TIssues Issues;
        NLWTrace::TOrbit Orbit;
    };

    struct TEvDiscardSnapshot : public TEventLocal<TEvDiscardSnapshot, TKqpSnapshotEvents::EvDiscardSnapshot> {
        TEvDiscardSnapshot() = default;
    };
};

NActors::IActor* CreateKqpSnapshotManager(const TString& database, TDuration queryTimeout);

} // namespace NKqp
} // namespace NKikimr

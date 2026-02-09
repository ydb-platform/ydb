#pragma once

#include <google/protobuf/timestamp.pb.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/common/sensitive_event_pb.h>
#include <ydb/core/tx/replication/common/worker_id.h>

namespace NKikimr::NReplication {

struct TEvService {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_SERVICE),

        EvHandshake,
        EvStatus,
        EvRunWorker,
        EvStopWorker,
        EvWorkerStatus,
        EvWorkerDataEnd,
        EvGetTxId,
        EvTxIdResult,
        EvHeartbeat,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_SERVICE));

    struct TEvHandshake: public TEventPB<TEvHandshake, NKikimrReplication::TEvHandshake, EvHandshake> {
        TEvHandshake() = default;

        explicit TEvHandshake(ui64 tabletId, ui64 generation) {
            Record.MutableController()->SetTabletId(tabletId);
            Record.MutableController()->SetGeneration(generation);
        }
    };

    struct TEvStatus: public TEventPB<TEvStatus, NKikimrReplication::TEvStatus, EvStatus> {
        TEvStatus() = default;
    };

    struct TEvRunWorker: public TSensitiveEventPB<TEvRunWorker, NKikimrReplication::TEvRunWorker, EvRunWorker> {
        TEvRunWorker() = default;
    };

    struct TEvStopWorker: public TEventPB<TEvStopWorker, NKikimrReplication::TEvStopWorker, EvStopWorker> {
        TEvStopWorker() = default;
    };

    struct TEvWorkerStatus: public TEventPB<TEvWorkerStatus, NKikimrReplication::TEvWorkerStatus, EvWorkerStatus> {
        TEvWorkerStatus() = default;

        explicit TEvWorkerStatus(const TWorkerId& id, NKikimrReplication::TEvWorkerStatus::EStatus status) {
            id.Serialize(*Record.MutableWorker());
            Record.SetStatus(status);
            Record.SetReason(NKikimrReplication::TEvWorkerStatus::REASON_ACK);
        }

        explicit TEvWorkerStatus(const TWorkerId& id,
                NKikimrReplication::TEvWorkerStatus::EStatus status,
                NKikimrReplication::TEvWorkerStatus::EReason reason,
                const TString& errorDescription
        ) {
            id.Serialize(*Record.MutableWorker());
            Record.SetStatus(status);
            Record.SetReason(reason);
            Record.SetErrorDescription(errorDescription);
        }

        explicit TEvWorkerStatus(const TWorkerId& id, TDuration lag) {
            id.Serialize(*Record.MutableWorker());
            Record.SetStatus(NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
            Record.SetReason(NKikimrReplication::TEvWorkerStatus::REASON_INFO);
            Record.SetLagMilliSeconds(lag.MilliSeconds());
        }

        explicit TEvWorkerStatus(const TWorkerId& id, TInstant startTime, TVector<std::pair<ui64, i64>>&& statsValues) {
            id.Serialize(*Record.MutableWorker());
            Record.SetStatus(NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
            Record.SetReason(NKikimrReplication::TEvWorkerStatus::REASON_STATS);
            auto& stats = *Record.MutableStats();
            if (startTime) {
                stats.MutableStartTime()->set_seconds(startTime.Seconds());
            }
            for (auto [k, v] : statsValues) {
                auto& val = *stats.AddValues();
                val.SetKey(k);
                val.SetValue(v);
            }
        }
    };

    struct TEvWorkerDataEnd: public TEventPB<TEvWorkerDataEnd, NKikimrReplication::TEvWorkerDataEnd, EvWorkerDataEnd> {
        TEvWorkerDataEnd() = default;
    };

    struct TEvGetTxId: public TEventPB<TEvGetTxId, NKikimrReplication::TEvGetTxId, EvGetTxId> {
        TEvGetTxId() = default;

        template <typename TContainer>
        explicit TEvGetTxId(const TContainer& container) {
            Record.MutableVersions()->Reserve(container.size());
            for (const auto& v : container) {
                v.ToProto(Record.AddVersions());
            }
        }
    };

    struct TEvTxIdResult: public TEventPB<TEvTxIdResult, NKikimrReplication::TEvTxIdResult, EvTxIdResult> {
        TEvTxIdResult() = default;

        explicit TEvTxIdResult(ui64 tabletId, ui64 generation) {
            Record.MutableController()->SetTabletId(tabletId);
            Record.MutableController()->SetGeneration(generation);
        }
    };

    struct TEvHeartbeat: public TEventPB<TEvHeartbeat, NKikimrReplication::TEvHeartbeat, EvHeartbeat> {
        TEvHeartbeat() = default;

        explicit TEvHeartbeat(const TRowVersion& version) {
            version.ToProto(Record.MutableVersion());
        }
    };

};

namespace NService {

inline TString MakeDiscoveryPath(const TString& tenant) {
    return "rs+" + tenant;
}

} // NService

inline TActorId MakeReplicationServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("ReplictnSvc"));
}

IActor* CreateReplicationService();

}

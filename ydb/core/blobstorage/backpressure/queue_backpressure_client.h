#pragma once

#include "defs.h"
#include "queue_backpressure_common.h"

namespace NKikimr {

    namespace NBackpressure {
        class TQueueClientId;
    } // NBackpressure

    struct TEvPruneQueue : public TEventLocal<TEvPruneQueue, TEvBlobStorage::EvPruneQueue>
    {};

    struct TEvProxyQueueState : public TEventLocal<TEvProxyQueueState, TEvBlobStorage::EvProxyQueueState> {
        TVDiskID VDiskId;
        NKikimrBlobStorage::EVDiskQueueId QueueId;
        bool IsConnected;
        bool ExtraBlockChecksSupport;
        std::shared_ptr<const TCostModel> CostModel;

        TEvProxyQueueState(const TVDiskID &vDiskId, NKikimrBlobStorage::EVDiskQueueId queueId, bool isConnected,
                bool extraBlockChecksSupport, std::shared_ptr<const TCostModel> costModel)
            : VDiskId(vDiskId)
            , QueueId(queueId)
            , IsConnected(isConnected)
            , ExtraBlockChecksSupport(extraBlockChecksSupport)
            , CostModel(std::move(costModel))
        {}

        TString ToString() const {
            TStringStream str;
            str << "{VDiskId# " << VDiskId.ToString();
            str << " QueueId# " << static_cast<ui32>(QueueId);
            str << " IsConnected# " << (IsConnected ? "true" : "false");
            str << " ExtraBlockChecksSupport# " << (ExtraBlockChecksSupport ? "true" : "false");
            if (CostModel) {
                str << " CostModel# " << CostModel->ToString();
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvRequestProxyQueueState
        : public TEventLocal<TEvRequestProxyQueueState, TEvBlobStorage::EvRequestProxyQueueState>
    {};

    IActor* CreateVDiskBackpressureClient(const TIntrusivePtr<TBlobStorageGroupInfo>& info, TVDiskIdShort vdiskId,
        NKikimrBlobStorage::EVDiskQueueId queueId,const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TBSProxyContextPtr& bspctx, const NBackpressure::TQueueClientId& clientId, const TString& queueName,
        ui32 interconnectChannel, bool local, TDuration watchdogTimeout,
        TIntrusivePtr<NBackpressure::TFlowRecord> &flowRecord, NMonitoring::TCountableBase::EVisibility visibility,
        bool useActorSystemTime = false);

} // NKikimr

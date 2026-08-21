#pragma once

#include "defs.h"

namespace NKikimr {

class TLoaderActor : public TActorBootstrapped<TLoaderActor> {
    enum {
        EvIssuePutRequest = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    };
    struct TEvIssuePutRequest : TEventLocal<TEvIssuePutRequest, EvIssuePutRequest> {};

    const NBackpressure::TQueueClientId ClientId;
    const TVDiskID VDiskId;
    const TActorId VDiskActorId;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TBSProxyContextPtr BSProxyCtx;
    TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;
    TActorId QueueId;
    ui32 InFlightRemain = 16;
    bool Ready = false;
    ui32 BlobIdx = 1;
    TString Buffer = TString::Uninitialized(100 << 10);
    std::deque<TLogoBlobID> RequestQ;

public:
    TLoaderActor(NBackpressure::TQueueClientId clientId, TVDiskID vdiskId, TActorId vdiskActorId)
        : ClientId(std::move(clientId))
        , VDiskId(std::move(vdiskId))
        , VDiskActorId(std::move(vdiskActorId))
        , Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
        , BSProxyCtx(MakeIntrusive<TBSProxyContext>(Counters))
        , FlowRecord(MakeIntrusive<NBackpressure::TFlowRecord>())
    {
        memset(Buffer.Detach(), '*', Buffer.size());
    }

    void Bootstrap() {
        LOG_DEBUG(*TlsActivationContext, NActorsServices::TEST, "%s Bootstrap", ClientId.ToString().data());
        TVector<TActorId> vdiskIds;
        vdiskIds.push_back(VDiskActorId);
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType(TBlobStorageGroupType::ErasureNone),
            1u, 1u, 1u, &vdiskIds);
        QueueId = Register(CreateVDiskBackpressureClient(info, VDiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog,
            Counters, BSProxyCtx, ClientId, "test", 0, true, TDuration::Seconds(60), FlowRecord,
            NMonitoring::TCountableBase::EVisibility::Public));
        IssuePutRequest();
        Become(&TThis::StateFunc);
    }

    void IssuePutRequest() {
        if (InFlightRemain && Ready) {
            const TLogoBlobID blobId(0x0123456789abcdefUL, 1, BlobIdx++, 0, 1, Buffer.size());
            LOG_DEBUG(*TlsActivationContext, NActorsServices::TEST, "%s %s", ClientId.ToString().data(),
                blobId.ToString().data());
            Send(QueueId, new TEvBlobStorage::TEvVPut(blobId, TRope(Buffer), VDiskId, false, nullptr, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog));
            RequestQ.push_back(blobId);
            --InFlightRemain;
        }

        Schedule(GenerateInactivityTimeout(), new TEvIssuePutRequest);
    }

    TDuration GenerateInactivityTimeout() {
        const ui32 ms = RandomNumber<ui32>(100) + 10;
        return TDuration::MilliSeconds(ms);
    }

    void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
        ++InFlightRemain;
        auto& record = ev->Get()->Record;
        const auto& blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        UNIT_ASSERT(!RequestQ.empty());
        UNIT_ASSERT_VALUES_EQUAL(RequestQ.front(), blobId);
        RequestQ.pop_front();
        LOG_DEBUG(*TlsActivationContext, NActorsServices::TEST, "%s %s %s", ClientId.ToString().data(),
            blobId.ToString().data(), NKikimrProto::EReplyStatus_Name(record.GetStatus()).data());
    }

    void Handle(TEvProxyQueueState::TPtr ev) {
        Ready = ev->Get()->IsConnected;
    }

    void PassAway() override {
        Send(QueueId, new TEvents::TEvPoison);
        TActorBootstrapped::PassAway();
    }

    STRICT_STFUNC(StateFunc, {
        cFunc(TEvents::TSystem::Poison, PassAway);
        cFunc(EvIssuePutRequest, IssuePutRequest);
        hFunc(TEvBlobStorage::TEvVPutResult, Handle);
        hFunc(TEvProxyQueueState, Handle);
    })
};

} // NKikimr

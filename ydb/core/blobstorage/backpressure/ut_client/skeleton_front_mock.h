#pragma once

#include "defs.h"

namespace NKikimr {

class TSkeletonFrontMockActor : public TActorBootstrapped<TSkeletonFrontMockActor> {
    enum {
        EvProcessQueue = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    };
    struct TEvProcessQueue : TEventLocal<TEvProcessQueue, EvProcessQueue> {};

    bool Ready = false;
    std::set<TActorId> Notify;
    std::optional<TCostModel> CostModel;
    NBackpressure::TQueueBackpressure<NBackpressure::TQueueClientId> Server;

    struct TOperation {
        NBackpressure::TQueueClientId ClientId;
        NBackpressure::TMessageId MessageId;
        ui64 Cost;
        std::unique_ptr<IEventHandle> Result;
        TActorId Sender;
    };

    std::deque<TOperation> Operations;

public:
    TSkeletonFrontMockActor()
        : Server(true, 1000000, 100000, 20000, 200000, 100000, 200000, 300000, TDuration::Minutes(1))
    {}

    void Bootstrap() {
        NKikimrBlobStorage::TVDiskCostSettings settings;
        FillInCostSettings(&settings);
        CostModel.emplace(settings, TErasureType::ErasureNone);

        Become(&TThis::StateFunc, TDuration::Seconds(10), new TEvents::TEvWakeup);
    }

    void HandleWakeup() {
        LOG_DEBUG(*TlsActivationContext, NActorsServices::TEST, "HandleWakeup");
        Ready = true;
        for (const TActorId& id : std::exchange(Notify, {})) {
            Send(id, new TEvBlobStorage::TEvVReadyNotify);
        }
    }

    void Reply(IEventHandle& ev, IEventBase *reply) {
        Send(ev.Sender, reply, IEventHandle::MakeFlags(ev.GetChannel(), 0), ev.Cookie);
    }

    void Handle(TEvBlobStorage::TEvVCheckReadiness::TPtr ev) {
        auto& record = ev->Get()->Record;
        if (!Ready && record.GetNotifyIfNotReady()) {
            Notify.insert(ev->Sender);
        }
        Reply(*ev, new TEvBlobStorage::TEvVCheckReadinessResult(Ready ? NKikimrProto::OK : NKikimrProto::NOTREADY));
    }

    void Handle(TEvBlobStorage::TEvVStatus::TPtr ev) {
        auto& record = ev->Get()->Record;

        if (!Ready) {
            Reply(*ev, new TEvBlobStorage::TEvVStatusResult(NKikimrProto::NOTREADY, record.GetVDiskID()));
            if (record.GetNotifyIfNotReady()) {
                Notify.insert(ev->Sender);
            }
        }
    }

    void Handle(TEvBlobStorage::TEvVPut::TPtr ev) {
        auto& record = ev->Get()->Record;
        const TLogoBlobID& blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const ui64 cookie = record.GetCookie();

        if (!Ready) {
            Reply(*ev, new TEvBlobStorage::TEvVPutResult(NKikimrProto::NOTREADY,
                blobId, vdiskId, record.HasCookie() ? &cookie : nullptr,
                TOutOfSpaceStatus(0, 0), TActivationContext::Now(), ev->Get()->GetCachedByteSize(),
                &record, nullptr, nullptr, nullptr, record.GetBuffer().size(), 0, TString()));
            if (record.GetNotifyIfNotReady()) {
                Notify.insert(ev->Sender);
            }
            return;
        }

        UNIT_ASSERT(record.HasMsgQoS());

        std::unique_ptr<TEvBlobStorage::TEvVPutResult> reply(new TEvBlobStorage::TEvVPutResult(NKikimrProto::OK,
            blobId, vdiskId, record.HasCookie() ? &cookie : nullptr,
            TOutOfSpaceStatus(0, 0), TActivationContext::Now(), ev->Get()->GetCachedByteSize(),
            &record, nullptr, nullptr, nullptr, record.GetBuffer().size(), 0, TString()));

        auto *qos = reply->Record.MutableMsgQoS();
        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, "Received " << SingleLineProto(*qos));

        if (qos->GetSendMeCostSettings()) {
            FillInCostSettings(qos->MutableCostSettings());
        }

        const NBackpressure::TQueueClientId clientId(*qos);
        const NBackpressure::TMessageId messageId(qos->GetMsgId());
        const ui64 cost = CostModel->GetCost(*ev->Get());
        auto feedback = Server.Push(clientId, ev->Sender, messageId, cost, TActivationContext::Now());
        feedback.first.Serialize(*qos->MutableWindow());
        NotifyClients(feedback.second);
        if (feedback.Good()) {
            if (Operations.empty()) {
                Schedule(TDuration::MicroSeconds((999 + cost) / 1000), new TEvProcessQueue);
            }
            Operations.push_back(TOperation{clientId, messageId, cost, std::make_unique<IEventHandle>(ev->Sender,
                SelfId(), reply.release(), IEventHandle::MakeFlags(ev->GetChannel(), 0), ev->Cookie), ev->Sender});
        } else {
            reply->Record.SetStatus(feedback.first.Status == NKikimrBlobStorage::TWindowFeedback::IncorrectMsgId
                ? NKikimrProto::TRYLATER : NKikimrProto::TRYLATER_SIZE);

            LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, "Sending bad " << SingleLineProto(reply->Record));

            Reply(*ev, reply.release());
        }
    }

    void HandleProcessQueue() {
        TOperation& op = Operations.front();

        LOG_DEBUG_S(*TlsActivationContext, NActorsServices::TEST, "Sending " << SingleLineProto(op.Result->Get<TEvBlobStorage::TEvVPutResult>()->Record));

        TActivationContext::Send(op.Result.release());

        auto feedback = Server.Processed(op.Sender, op.MessageId, op.Cost, TActivationContext::Now());
        Send(op.Sender, new TEvBlobStorage::TEvVWindowChange(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, feedback.first));
        NotifyClients(feedback.second);
        Operations.pop_front();
        if (!Operations.empty()) {
            Schedule(TDuration::MicroSeconds((999 + Operations.front().Cost) / 1000), new TEvProcessQueue);
        }
    }

    void NotifyClients(const std::vector<NBackpressure::TWindowStatus<NBackpressure::TQueueClientId>>& feedback) {
        for (const auto& item : feedback) {
            Send(item.ActorId, new TEvBlobStorage::TEvVWindowChange(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, item));
        }
    }

    void FillInCostSettings(NKikimrBlobStorage::TVDiskCostSettings *settings) {
        settings->SetSeekTimeUs(60e6 / 7200); // HDD, for instance
        settings->SetReadSpeedBps(60e6);
        settings->SetWriteSpeedBps(60e6);
        settings->SetReadBlockSize(4096);
        settings->SetWriteBlockSize(4096);
        settings->SetMinREALHugeBlobInBytes(512 << 10);
    }

    STRICT_STFUNC(StateFunc, {
        cFunc(TEvents::TSystem::Poison, PassAway);
        cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        cFunc(EvProcessQueue, HandleProcessQueue);
        hFunc(TEvBlobStorage::TEvVStatus, Handle);
        hFunc(TEvBlobStorage::TEvVCheckReadiness, Handle);
        hFunc(TEvBlobStorage::TEvVPut, Handle);
    })
};

} // NKikimr

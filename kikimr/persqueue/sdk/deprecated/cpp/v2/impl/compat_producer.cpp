
#include "compat_producer.h"
#include "persqueue_p.h"

namespace NPersQueue {

    using namespace NYdb::NPersQueue;


    static TWriteSessionSettings ConvertToWriteSessionSettings(const TProducerSettings& ps) {
        TWriteSessionSettings settings;
        settings.Path(ps.Topic)
                .MessageGroupId(ps.SourceId)
                .RetryPolicy(ps.ReconnectOnFailure ? NYdb::NPersQueue::IRetryPolicy::GetExponentialBackoffPolicy(ps.ReconnectionDelay, ps.ReconnectionDelay, ps.MaxReconnectionDelay,
                                                                        ps.MaxAttempts, TDuration::Max(), 2.0, [](NYdb::EStatus){ return ERetryErrorClass::ShortRetry; })
                                                   : NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy())
                .Codec(NYdb::NPersQueue::ECodec(ps.Codec + 1))
                .CompressionLevel(ps.Quality)
                .ValidateSeqNo(false);
        for (auto& attr : ps.ExtraAttrs) {
            settings.AppendSessionMeta(attr.first, attr.second);
        }

        if (ps.PreferredCluster) {
            settings.PreferredCluster(ps.PreferredCluster);
        }
        if (ps.DisableCDS || ps.Server.UseLogbrokerCDS == EClusterDiscoveryUsageMode::DontUse) {
            settings.ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Off);
        } else if (ps.Server.UseLogbrokerCDS == EClusterDiscoveryUsageMode::Use) {
            settings.ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::On);
        }

        return settings;
    }

    TYdbSdkCompatibilityProducer::~TYdbSdkCompatibilityProducer() {
        WriteSession->Close(TDuration::Seconds(0));

        NotifyClient(NErrorCode::OK, "producer object destroyed by client");
    }

    TYdbSdkCompatibilityProducer::TYdbSdkCompatibilityProducer(const TProducerSettings& settings, TPersQueueClient& persQueueClient,
                                     std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib)
    : IProducerImpl(destroyEventRef, pqLib)
    {
        IsDeadPromise = ::NThreading::NewPromise<TError>();
        TWriteSessionSettings sessionSettings = ConvertToWriteSessionSettings(settings);
        Closed = false;
        WriteSession = persQueueClient.CreateWriteSession(sessionSettings);
    }

    void TYdbSdkCompatibilityProducer::SubscribeToNextEvent() {
        NextEvent = WriteSession->WaitEvent();
        std::weak_ptr<TYdbSdkCompatibilityProducer> self = shared_from_this();
        NextEvent.Subscribe([self](const auto& future) {
            auto selfShared = self.lock();
            Y_UNUSED(future);
            if (selfShared) {
                selfShared->DoProcessNextEvent();
            }
        });
    }

    void TYdbSdkCompatibilityProducer::Init() noexcept {
    }

    NThreading::TFuture<TProducerCreateResponse> TYdbSdkCompatibilityProducer::Start(TInstant deadline) noexcept {
        Y_UNUSED(deadline); // TODO: start timeout?
        SubscribeToNextEvent();
        return WriteSession->GetInitSeqNo().Apply([](const auto& future) {
            TWriteResponse response;
            if (future.HasException()) {
                response.MutableError()->SetDescription("session closed");
                response.MutableError()->SetCode(NErrorCode::ERROR);
            } else {
                response.MutableInit()->SetMaxSeqNo(future.GetValue());
            }
            TProducerCreateResponse res(std::move(response));

            return NThreading::MakeFuture<TProducerCreateResponse>(res);
            });
    }

    void TYdbSdkCompatibilityProducer::WriteImpl(NThreading::TPromise<TProducerCommitResponse>& promise, TMaybe<TProducerSeqNo> seqNo, ::NPersQueue::TData data) noexcept {
        TContinuationToken *contToken = nullptr;
        bool write = false;
        with_lock(Lock) {
            if (!Closed) {
                contToken = ContToken.Get();
                if(contToken == nullptr || !ToWrite.empty()) {
                    ToWrite.emplace(promise, seqNo, data);
                    return;
                } else { // contToken here and ToWrite is empty - can write right now
                    ToAck.emplace(promise, seqNo, data);
                    write = true;
                }
            }
        }
        if (write) {
            if (data.IsEncoded()) {
                WriteSession->WriteEncoded(std::move(*contToken), data.GetEncodedData(), NYdb::NPersQueue::ECodec(data.GetCodecType() + 1), data.GetOriginalSize(), seqNo);   // can trigger NextEvent.Subscribe() in the same thread
            } else {
                WriteSession->Write(std::move(*contToken), data.GetSourceData(), seqNo);   // can trigger NextEvent.Subscribe() in the same thread
            }
        } else {
            TWriteResponse wr;
            wr.MutableError()->SetCode(NErrorCode::ERROR);
            wr.MutableError()->SetDescription("session closed");
            TProducerCommitResponse resp(seqNo ? *seqNo : 0, data, std::move(wr));
            promise.SetValue(resp);
        }
    }

    void TYdbSdkCompatibilityProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, ::NPersQueue::TData data) noexcept {
        return WriteImpl(promise, {}, std::move(data));    // similar to TCompressingProducer
    }

    void TYdbSdkCompatibilityProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, ::NPersQueue::TData data) noexcept {
        return WriteImpl(promise, seqNo, std::move(data));    // similar to TCompressingProducer
    }

    void TYdbSdkCompatibilityProducer::Cancel() {
        WriteSession->Close(TDuration::Seconds(0));
    }

    NErrorCode::EErrorCode GetErrorCode(const NYdb::EStatus status) {
        switch(status) {
            case NYdb::EStatus::SUCCESS:
                return NErrorCode::OK;
            case NYdb::EStatus::UNAVAILABLE:
                return NErrorCode::INITIALIZING;
            case NYdb::EStatus::OVERLOADED:
                return NErrorCode::OVERLOAD;
            case NYdb::EStatus::BAD_REQUEST:
                return NErrorCode::BAD_REQUEST;
            case NYdb::EStatus::NOT_FOUND:
            case NYdb::EStatus::SCHEME_ERROR:
                return NErrorCode::UNKNOWN_TOPIC;
            case NYdb::EStatus::UNSUPPORTED:
                return NErrorCode::BAD_REQUEST;
            case NYdb::EStatus::UNAUTHORIZED:
                return NErrorCode::ACCESS_DENIED;
            default:
                return NErrorCode::ERROR;
        }
        return NErrorCode::ERROR;
    }

    struct TToNotify {
        NThreading::TPromise<TProducerCommitResponse> Promise;
        TProducerCommitResponse Resp;

        TToNotify(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerCommitResponse&& resp)
        : Promise(promise)
        , Resp(resp)
        {}
    };


    void TYdbSdkCompatibilityProducer::NotifyClient(NErrorCode::EErrorCode code, const TString& reason) {

        TError err;
        err.SetCode(code);
        err.SetDescription(reason);
        std::queue<TToNotify> toNotify;
        with_lock(Lock) {
            if (Closed) return;
            Closed = true;
            while(!ToAck.empty()) {
                TMsgData& item = ToAck.front();
                TWriteResponse wr;
                wr.MutableError()->CopyFrom(err);
                TProducerCommitResponse resp(item.SeqNo ? *item.SeqNo : 0, item.Data, std::move(wr));
                toNotify.emplace(item.Promise, std::move(resp));
                ToAck.pop();
            }
            while(!ToWrite.empty()) {
                TMsgData& item = ToWrite.front();
                TWriteResponse wr;
                wr.MutableError()->CopyFrom(err);
                TProducerCommitResponse resp(item.SeqNo ? *item.SeqNo : 0, item.Data, std::move(wr));
                toNotify.emplace(item.Promise, std::move(resp));
                ToWrite.pop();
            }
        }
        while(!toNotify.empty()) {
            toNotify.front().Promise.SetValue(toNotify.front().Resp);
            toNotify.pop();
        }
        IsDeadPromise.SetValue(err);
        DestroyPQLibRef();
    }

    void TYdbSdkCompatibilityProducer::DoProcessNextEvent() {

        std::queue<TToNotify> toNotify;

        TVector<TWriteSessionEvent::TEvent> events = WriteSession->GetEvents(false);
        //Y_VERIFY(!events.empty());
        for (auto& event : events) {
            if(std::holds_alternative<TSessionClosedEvent>(event)) {
                NotifyClient(GetErrorCode(std::get<TSessionClosedEvent>(event).GetStatus()), std::get<TSessionClosedEvent>(event).DebugString() );
                return;
            } else if(std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event)) {

                // get seqNos, signal their promises
                TWriteSessionEvent::TAcksEvent& acksEvent = std::get<TWriteSessionEvent::TAcksEvent>(event);
                std::queue<TToNotify> toNotify;
                for(TWriteSessionEvent::TWriteAck& ackEvent : acksEvent.Acks) {
                    TWriteResponse writeResp;
                    auto ackMsg = writeResp.MutableAck();
                    ackMsg->SetSeqNo(ackEvent.SeqNo);
                    ackMsg->SetAlreadyWritten(ackEvent.State == TWriteSessionEvent::TWriteAck::EEventState::EES_ALREADY_WRITTEN);
                    ackMsg->SetOffset(ackEvent.Details ? ackEvent.Details->Offset : 0);
                    with_lock(Lock) {
                        Y_ASSERT(!ToAck.empty());
                        TProducerCommitResponse resp(ackEvent.SeqNo, ToAck.front().Data, std::move(writeResp));
                        toNotify.emplace(ToAck.front().Promise, std::move(resp));
                        ToAck.pop();
                    }
                }
                while(!toNotify.empty()) {
                    toNotify.front().Promise.SetValue(toNotify.front().Resp);
                    toNotify.pop();
                }
            } else if(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
                TWriteSessionEvent::TReadyToAcceptEvent& readyEvent = std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event);
                TContinuationToken contToken = std::move(readyEvent.ContinuationToken);     // IF there is only one only movable contToken THEN this is thread safe
                TString strData;
                TMaybe<TProducerSeqNo> seqNo;
                bool write = false;
                with_lock(Lock) {
                    if(ToWrite.empty()) {
                        ContToken = std::move(contToken);
                    } else {
                        strData = ToWrite.front().Data.GetSourceData();
                        seqNo = ToWrite.front().SeqNo;
                        ToAck.push(ToWrite.front());
                        ToWrite.pop();
                        write = true;
                    }
                }
                if (write) {
                    WriteSession->Write(std::move(contToken), strData, seqNo);
                }
            }
        }

        SubscribeToNextEvent();
    }

    NThreading::TFuture<TError> TYdbSdkCompatibilityProducer::IsDead() noexcept {
        return IsDeadPromise.GetFuture();
    }

}   // namespace NYdb::NPersQueue

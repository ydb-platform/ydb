#include "retrying_producer.h"
#include "persqueue_p.h"

#include <util/generic/strbuf.h>
#include <util/stream/zlib.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/vector.h>
#include <util/string/builder.h>

namespace NPersQueue {

TRetryingProducer::TRetryingProducer(const TProducerSettings& settings, std::shared_ptr<void> destroyEventRef,
                                     TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger)
    : IProducerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Settings(settings)
    , Logger(std::move(logger))
    , IsDeadPromise(NThreading::NewPromise<TError>())
    , NeedRecreation(true)
    , Stopping(false)
    , ToProcess(0)
    , LastReconnectionDelay(TDuration::Zero())
    , ReconnectionAttemptsDone(0)
{
    if (Settings.MaxAttempts == 0) {
        ythrow yexception() << "MaxAttempts setting can't be zero.";
    }

    if (Settings.ReconnectionDelay == TDuration::Zero()) {
        ythrow yexception() << "ReconnectionDelay setting can't be zero.";
    }

    if (Settings.StartSessionTimeout < PQLib->GetSettings().ChannelCreationTimeout) {
        ythrow yexception() << "StartSessionTimeout can't be less than ChannelCreationTimeout.";
    }
}

TRetryingProducer::~TRetryingProducer() noexcept {
    Destroy("Destructor called");
}

NThreading::TFuture<TProducerCreateResponse> TRetryingProducer::Start(TInstant deadline) noexcept {
    StartPromise = NThreading::NewPromise<TProducerCreateResponse>();
    if (deadline != TInstant::Max()) {
        std::weak_ptr<TRetryingProducer> self = shared_from_this();
        auto onDeadline = [self] {
            auto selfShared = self.lock();
            if (selfShared) {
                selfShared->OnStartDeadline();
            }
        };
        PQLib->GetScheduler().Schedule(deadline, this, onDeadline);
    }
    RecreateProducer(deadline);
    return StartPromise.GetFuture();
}

void TRetryingProducer::RecreateProducer(TInstant deadline) noexcept {
    Producer = nullptr;
    if (Stopping)
        return;

    Y_VERIFY(InFlightRequests.size() == Futures.size());
    DEBUG_LOG("Recreating subproducer. Futures size: " << Futures.size(), Settings.SourceId, "");
    if (Futures.empty()) {
        DoRecreate(deadline);
    } // otherwise it will be recreated in ProcessFutures()
}

void TRetryingProducer::DoRecreate(TInstant deadline) noexcept {
    Y_VERIFY(InFlightRequests.size() == Futures.size());
    Y_VERIFY(Futures.empty());
    if (Stopping)
        return;
    Y_VERIFY(NeedRecreation);
    NeedRecreation = false;
    if (ReconnectionAttemptsDone >= Settings.MaxAttempts) {
        Destroy(TStringBuilder() << "Failed " << ReconnectionAttemptsDone << " reconnection attempts");
        return;
    }
    ++ReconnectionAttemptsDone;
    DEBUG_LOG("Creating subproducer. Attempt: " << ReconnectionAttemptsDone, Settings.SourceId, "");
    Producer = PQLib->CreateRawProducer(Settings, DestroyEventRef, Logger);
    StartFuture = Producer->Start(deadline);

    std::weak_ptr<TRetryingProducer> self(shared_from_this());
    PQLib->Subscribe(StartFuture,
                     this,
                     [self](const NThreading::TFuture<TProducerCreateResponse>& f) {
                         auto selfShared = self.lock();
                         if (selfShared) {
                             selfShared->ProcessStart(f);
                         }
                     });
}

TDuration TRetryingProducer::UpdateReconnectionDelay() {
    if (LastReconnectionDelay == TDuration::Zero()) {
        LastReconnectionDelay = Settings.ReconnectionDelay;
    } else {
        LastReconnectionDelay *= 2;
    }
    LastReconnectionDelay = Min(LastReconnectionDelay, Settings.MaxReconnectionDelay);
    return LastReconnectionDelay;
}

void TRetryingProducer::ScheduleRecreation() {
    if (Stopping) {
        return;
    }
    NeedRecreation = true;
    if (!ReconnectionCallback) {
        const TDuration delay = UpdateReconnectionDelay();
        DEBUG_LOG("Schedule subproducer recreation through " << delay, Settings.SourceId, "");
        std::weak_ptr<TRetryingProducer> self(shared_from_this());
        ReconnectionCallback =
            PQLib->GetScheduler().Schedule(delay,
                                           this,
                                           [self, sourceId = Settings.SourceId, logger = Logger] {
                                               auto selfShared = self.lock();
                                               WRITE_LOG("Subproducer recreation callback. self is " << (selfShared ? "OK" : "nullptr") << ", stopping: " << (selfShared ? selfShared->Stopping : false), sourceId, "", TLOG_DEBUG, logger);
                                               if (selfShared) {
                                                   selfShared->ReconnectionCallback = nullptr;
                                                   if (!selfShared->Stopping) {
                                                       selfShared->RecreateProducer(TInstant::Now() + selfShared->Settings.StartSessionTimeout);
                                                   }
                                               }
                                           });
    }
}

void TRetryingProducer::ProcessStart(const NThreading::TFuture<TProducerCreateResponse>& f) noexcept {
    INFO_LOG("Subproducer start response: " << f.GetValue().Response, Settings.SourceId, "");
    if (Stopping)
        return;
    if (NeedRecreation)
        return;
    if (!StartFuture.HasValue())
        return;
    if (StartFuture.GetValue().Response.HasError()) {
        WARN_LOG("Subproducer start error: " << f.GetValue().Response, Settings.SourceId, "");
        ScheduleRecreation();
    } else {
        LastReconnectionDelay = TDuration::Zero();
        ReconnectionAttemptsDone = 0;

        // recreate on dead
        DEBUG_LOG("Subscribe on subproducer death", Settings.SourceId, "");
        std::weak_ptr<TRetryingProducer> self(shared_from_this());
        PQLib->Subscribe(Producer->IsDead(),
                         this,
                         [self](const NThreading::TFuture<TError>& error) {
                             auto selfShared = self.lock();
                             if (selfShared) {
                                 selfShared->OnProducerDead(error.GetValue());
                             }
                         });

        if (!StartPromise.HasValue()) {
            StartPromise.SetValue(StartFuture.GetValue());
        }

        SendData();
    }
}

void TRetryingProducer::OnProducerDead(const TError& error) {
    WARN_LOG("Subproducer is dead: " << error, Settings.SourceId, "");
    ScheduleRecreation();
}

void TRetryingProducer::SendData() noexcept {
    if (Stopping)
        return;
    ui64 maxSeqNo = StartFuture.GetValue().Response.GetInit().GetMaxSeqNo();
    std::deque<NThreading::TPromise<TProducerCommitResponse>> promises;
    std::deque<TWriteData> values;
    ui64 prevSeqNo = 0;
    for (auto& d : ResendRequests) {
        Y_VERIFY(d.SeqNo == 0 || d.SeqNo > prevSeqNo);
        if (d.SeqNo != 0) {
            prevSeqNo = d.SeqNo;
        }
        if (d.SeqNo != 0 && d.SeqNo <= maxSeqNo) {
            promises.push_back(Promises.front());
            Promises.pop_front();
            values.push_back(d);
        } else {
            DelegateWriteAndSubscribe(d.SeqNo, std::move(d.Data));
        }
    }
    //Requests can be not checked - it is up to client
    for (auto& d : Requests) {
        DelegateWriteAndSubscribe(d.SeqNo, std::move(d.Data));
    }
    ResendRequests.clear();
    Requests.clear();
    for (ui32 i = 0; i < promises.size(); ++i) {
        TWriteResponse res;
        res.MutableAck()->SetAlreadyWritten(true);
        res.MutableAck()->SetSeqNo(values[i].SeqNo);
        promises[i].SetValue(TProducerCommitResponse(values[i].SeqNo, std::move(values[i].Data), std::move(res)));
    }
}

void TRetryingProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, const TProducerSeqNo seqNo, TData data) noexcept {
    Y_VERIFY(data.IsEncoded());
    if (!StartFuture.Initialized()) {
        TWriteResponse res;
        res.MutableError()->SetDescription("producer is not ready");
        res.MutableError()->SetCode(NErrorCode::ERROR);
        promise.SetValue(TProducerCommitResponse{seqNo, std::move(data), std::move(res)});
        return;
    }

    Promises.push_back(promise);

    if (!StartFuture.HasValue() || NeedRecreation || Stopping || !Requests.empty() || !ResendRequests.empty()) {
        Requests.emplace_back(seqNo, std::move(data));
    } else {
        DelegateWriteAndSubscribe(seqNo, std::move(data));
    }
}

void TRetryingProducer::DelegateWriteAndSubscribe(TProducerSeqNo seqNo, TData&& data) noexcept {
    Y_VERIFY(InFlightRequests.size() == Futures.size());
    if (seqNo == 0) {
        Futures.push_back(Producer->Write(data));
    } else {
        Futures.push_back(Producer->Write(seqNo, data));
    }
    InFlightRequests.emplace_back(seqNo, std::move(data));

    std::weak_ptr<TRetryingProducer> self(shared_from_this());
    PQLib->Subscribe(Futures.back(),
                     this,
                     [self](const NThreading::TFuture<TProducerCommitResponse>&) {
                         auto selfShared = self.lock();
                         if (selfShared) {
                             selfShared->ProcessFutures();
                         }
                     });
}

NThreading::TFuture<TError> TRetryingProducer::IsDead() noexcept {
    return IsDeadPromise.GetFuture();
}

void TRetryingProducer::ProcessFutures() noexcept {
    NThreading::TPromise<TProducerCommitResponse> promise;
    NThreading::TFuture<TProducerCommitResponse> future;
    {
        if (Stopping) {
            return;
        }
        ++ToProcess;
        while (ToProcess) {
            if (Futures.empty() || !Futures.front().HasValue()) {
                break;
            }

            Y_VERIFY(InFlightRequests.size() == Futures.size());
            --ToProcess;

            const TProducerCommitResponse& response = Futures.front().GetValue();
            const TWriteData& writeData = InFlightRequests.front();
            Y_VERIFY(Promises.size() == Futures.size() + Requests.size() + ResendRequests.size());

            if (NeedRecreation) {
                ResendRequests.emplace_back(response.SeqNo, TData(writeData.Data));
                InFlightRequests.pop_front();
                Futures.pop_front();
                continue;
            }

            if (Futures.front().GetValue().Response.HasError()) {
                Y_VERIFY(!NeedRecreation);
                ScheduleRecreation();
                WARN_LOG("Future response with error: " << response.Response, Settings.SourceId, "");
                ResendRequests.emplace_back(response.SeqNo, TData(writeData.Data));
                InFlightRequests.pop_front();
                Futures.pop_front();
                Producer = nullptr;
                continue;
            }

            promise = Promises.front();
            Promises.pop_front();
            future = Futures.front();
            InFlightRequests.pop_front();
            Futures.pop_front();
            promise.SetValue(future.GetValue());
        }
        if (NeedRecreation && Futures.empty() && !ReconnectionCallback) {
            // We need recreation, but scheduled recreation hasn't start producer because of nonempty future list.
            DEBUG_LOG("Recreating subproducer after all futures were processed", Settings.SourceId, "");
            RecreateProducer(TInstant::Now() + Settings.StartSessionTimeout);
        }
    }
}

void TRetryingProducer::Destroy(const TString& description) {
    TError error;
    error.SetDescription(description);
    error.SetCode(NErrorCode::ERROR);
    Destroy(error);
}

void TRetryingProducer::SubscribeDestroyed() {
    NThreading::TPromise<void> promise = ProducersDestroyed;
    auto handler = [promise](const auto&) mutable {
        promise.SetValue();
    };
    if (Producer) {
        WaitExceptionOrAll(DestroyedPromise.GetFuture(), Producer->Destroyed())
            .Subscribe(handler);
    } else {
        DestroyedPromise.GetFuture()
            .Subscribe(handler);
    }

    DestroyPQLibRef();
}

void TRetryingProducer::Destroy(const TError& error) {
    if (Stopping) {
        return;
    }
    Stopping = true;
    SubscribeDestroyed();
    const bool started = StartFuture.Initialized();
    Producer = nullptr;
    if (started) {
        Y_VERIFY(Promises.size() == ResendRequests.size() + Requests.size() + InFlightRequests.size());
        ResendRequests.insert(ResendRequests.begin(), InFlightRequests.begin(), InFlightRequests.end());
        ResendRequests.insert(ResendRequests.end(), Requests.begin(), Requests.end());
        for (auto& v : ResendRequests) {
            TWriteResponse resp;
            *resp.MutableError() = error;
            Promises.front().SetValue(TProducerCommitResponse(v.SeqNo, std::move(v.Data), std::move(resp)));
            Promises.pop_front();
        }
        Y_VERIFY(Promises.empty());
    }
    if (StartPromise.Initialized() && !StartPromise.HasValue()) {
        TWriteResponse resp;
        *resp.MutableError() = error;
        StartPromise.SetValue(TProducerCreateResponse(std::move(resp)));
    }
    IsDeadPromise.SetValue(error);
}

NThreading::TFuture<void> TRetryingProducer::Destroyed() noexcept {
    return ProducersDestroyed.GetFuture();
}

void TRetryingProducer::OnStartDeadline() {
    if (!StartPromise.HasValue()) {
        TError error;
        error.SetDescription("Start timeout.");
        error.SetCode(NErrorCode::CREATE_TIMEOUT);
        Destroy(error);
    }
}

void TRetryingProducer::Cancel() {
    Destroy(GetCancelReason());
}

}

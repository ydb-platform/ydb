#include "multicluster_producer.h"
#include "persqueue_p.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NPersQueue {

TWeightIndex::TWeightIndex(size_t count)
    : Weights(count)
    , Index(count)
    , IsEnabled(count)
    , EnabledCnt(0)
{
    Y_VERIFY(count > 0);
}

void TWeightIndex::SetWeight(size_t i, unsigned weight) {
    const unsigned oldWeight = Weights[i];
    const int diff = static_cast<int>(weight) - static_cast<int>(oldWeight);
    if (diff != 0) {
        Weights[i] = weight;
        if (IsEnabled[i]) {
            UpdateIndex(i, diff);
        }
    }
}

void TWeightIndex::Enable(size_t i) {
    if (!IsEnabled[i]) {
        ++EnabledCnt;
        IsEnabled[i] = true;
        UpdateIndex(i, static_cast<int>(Weights[i]));
    }
}

void TWeightIndex::Disable(size_t i) {
    if (IsEnabled[i]) {
        --EnabledCnt;
        IsEnabled[i] = false;
        UpdateIndex(i, -static_cast<int>(Weights[i]));
    }
}

size_t TWeightIndex::Choose(unsigned randomNumber) const {
    Y_VERIFY(randomNumber < WeightsSum());
    Y_VERIFY(!Index.empty());
    const auto choice = std::upper_bound(Index.begin(), Index.end(), randomNumber);
    Y_VERIFY(choice != Index.end());
    return choice - Index.begin();
}

void TWeightIndex::UpdateIndex(size_t i, int weightDiff) {
    for (size_t j = i; j < Index.size(); ++j) {
        Index[j] += weightDiff;
    }
}

static TProducerCommitResponse MakeErrorCommitResponse(const TString& description, TProducerSeqNo seqNo, const TData& data) {
    TWriteResponse res;
    res.MutableError()->SetDescription(description);
    res.MutableError()->SetCode(NErrorCode::ERROR);
    return TProducerCommitResponse{seqNo, data, std::move(res)};
}

TMultiClusterProducer::TMultiClusterProducer(const TMultiClusterProducerSettings& settings, std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib, TIntrusivePtr<ILogger> logger)
    : IProducerImpl(std::move(destroyEventRef), std::move(pqLib))
    , Settings(settings)
    , DeadPromise(NThreading::NewPromise<TError>())
    , StartPromise(NThreading::NewPromise<TProducerCreateResponse>())
    , State(EState::Created)
    , Logger(std::move(logger))
    , WeightIndex(Settings.ServerWeights.size())
    , FallbackWeightIndex(Settings.ServerWeights.size())
    , Subproducers(Settings.ServerWeights.size())
    , FallbackSubproducers(Settings.ServerWeights.size())
{
    if (Settings.ServerWeights.size() <= 1) {
        ythrow yexception() << "MultiCluster producer is working with several servers. You'd better to create retrying producer in this case";
    }
    if (Settings.MinimumWorkingDcsCount == 0) {
        ythrow yexception() << "MinimumWorkingDcsCount can't be zero. We want that you have at least something working!";
    }
    if (Settings.SourceIdPrefix.empty()) {
        ythrow yexception() << "SourceIdPrefix must be nonempty";
    }

    bool reconnectWarningPrinted = false;
    for (TMultiClusterProducerSettings::TServerWeight& w : Settings.ServerWeights) {
        if (!w.Weight) {
            ythrow yexception() << "Server weight must be greater than zero";
        }
        if (!w.ProducerSettings.ReconnectOnFailure) {
            w.ProducerSettings.ReconnectOnFailure = true;
            if (!reconnectWarningPrinted) {
                reconnectWarningPrinted = true;
                WARN_LOG("ReconnectOnFailure setting is off. Turning it on for multicluster producer.", Settings.SourceIdPrefix, "");
            }
            ythrow yexception() << "You must enable ReconnectOnFailure setting";
        }
        if (w.ProducerSettings.MaxAttempts == std::numeric_limits<unsigned>::max()) {
            ythrow yexception() << "MaxAttempts must be a finite number";
        }
    }

    for (size_t i = 0; i < Settings.ServerWeights.size(); ++i) {
        WeightIndex.SetWeight(i, Settings.ServerWeights[i].Weight);
        FallbackWeightIndex.SetWeight(i, Settings.ServerWeights[i].Weight);
    }
}

TMultiClusterProducer::~TMultiClusterProducer() {
    Destroy("Destructor called");
}

void TMultiClusterProducer::StartSubproducer(TInstant deadline, size_t i, bool fallback) {
    std::weak_ptr<TMultiClusterProducer> self(shared_from_this());
    auto& sp = GetSubproducers(fallback)[i];
    sp.Producer = CreateSubproducer(Settings.ServerWeights[i].ProducerSettings, i, fallback);
    sp.StartFuture = sp.Producer->Start(deadline);
    sp.DeadFuture = sp.Producer->IsDead();
    PQLib->Subscribe(sp.StartFuture,
                     this,
                     [self, i, fallback](const auto&) {
                         auto selfShared = self.lock();
                         if (selfShared) {
                             selfShared->OnProducerStarted(i, fallback);
                         }
                     });
    PQLib->Subscribe(sp.DeadFuture,
                     this,
                     [self, i, fallback](const auto&) {
                         auto selfShared = self.lock();
                         if (selfShared) {
                             selfShared->OnProducerDead(i, fallback);
                         }
                     });
}

void TMultiClusterProducer::OnNeedProducerRestart(size_t i, bool fallback) {
    if (State == EState::Dead) {
        return;
    }
    const auto deadline = TInstant::Now() + Settings.ServerWeights[i].ProducerSettings.StartSessionTimeout;
    StartSubproducer(deadline, i, fallback);
}

void TMultiClusterProducer::ScheduleProducerRestart(size_t i, bool fallback) {
    std::weak_ptr<TMultiClusterProducer> self(shared_from_this());
    PQLib->GetScheduler().Schedule(Settings.ServerWeights[i].ProducerSettings.ReconnectionDelay,
                                   this,
                                   [self, i, fallback] {
                                       auto selfShared = self.lock();
                                       if (selfShared) {
                                           selfShared->OnNeedProducerRestart(i, fallback);
                                       }
                                   });
}

void TMultiClusterProducer::StartSubproducers(TInstant deadline, bool fallback) {
    for (size_t i = 0; i < Settings.ServerWeights.size(); ++i) {
        StartSubproducer(deadline, i, fallback);
    }
}

NThreading::TFuture<TProducerCreateResponse> TMultiClusterProducer::Start(TInstant deadline) noexcept {
    Y_VERIFY(State == EState::Created);
    State = EState::Starting;
    StartSubproducers(deadline);
    StartSubproducers(deadline, true);
    return StartPromise.GetFuture();
}

void TMultiClusterProducer::OnProducerStarted(size_t i, bool fallback) {
    if (State == EState::Dead) {
        return;
    }
    TSubproducerInfo& info = GetSubproducers(fallback)[i];
    info.StartAnswered = true;
    DEBUG_LOG("Start subproducer response: " << info.StartFuture.GetValue().Response, Settings.SourceIdPrefix, "");
    if (info.StartFuture.GetValue().Response.HasError()) {
        ERR_LOG("Failed to start subproducer[" << i << "]: " << info.StartFuture.GetValue().Response.GetError().GetDescription(), Settings.SourceIdPrefix, "");
        if (State == EState::Starting && !fallback) {
            size_t startedCount = 0;
            for (const auto& p : Subproducers) {
                startedCount += p.StartAnswered;
            }
            if (startedCount == Subproducers.size() && WeightIndex.EnabledCount() < Settings.MinimumWorkingDcsCount) {
                Destroy("Not enough subproducers started successfully");
                return;
            }
        }

        ScheduleProducerRestart(i, fallback);
    } else {
        GetWeightIndex(fallback).Enable(i);
        if (State == EState::Starting && !fallback && WeightIndex.EnabledCount() >= Settings.MinimumWorkingDcsCount) {
            State = EState::Working;
            TWriteResponse response;
            response.MutableInit();
            StartPromise.SetValue(TProducerCreateResponse(std::move(response)));
        }
        if (fallback) {
            ResendLastHopeQueue(i);
        }
    }
}

void TMultiClusterProducer::OnProducerDead(size_t i, bool fallback) {
    if (State == EState::Dead) {
        return;
    }

    WARN_LOG("Subproducer[" << i << "] is dead: " << GetSubproducers(fallback)[i].DeadFuture.GetValue().GetDescription(), Settings.SourceIdPrefix, "");
    GetWeightIndex(fallback).Disable(i);
    if (!fallback && WeightIndex.EnabledCount() < Settings.MinimumWorkingDcsCount) {
        Destroy("Not enough subproducers is online");
        return;
    }
    ScheduleProducerRestart(i, fallback);
}

void TMultiClusterProducer::Destroy(const TString& description) {
    TError error;
    error.SetCode(NErrorCode::ERROR);
    error.SetDescription(description);
    Destroy(error);
}

void TMultiClusterProducer::DestroyWrites(const TError& error, TIntrusiveListWithAutoDelete<TWriteInfo, TDelete>& pendingWrites) {
    for (TWriteInfo& wi : pendingWrites) {
        TWriteResponse response;
        response.MutableError()->CopyFrom(error);
        wi.ResponsePromise.SetValue(TProducerCommitResponse(0, std::move(wi.Data), std::move(response)));
    }
    pendingWrites.Clear();
}

void TMultiClusterProducer::Destroy(const TError& error) {
    if (State == EState::Dead) {
        return;
    }

    DEBUG_LOG("Destroying multicluster producer. Reason: " << error, Settings.SourceIdPrefix, "");
    const EState prevState = State;
    State = EState::Dead;
    SubscribeDestroyed();
    if (prevState == EState::Starting) {
        TWriteResponse response;
        response.MutableError()->CopyFrom(error);
        StartPromise.SetValue(TProducerCreateResponse(std::move(response)));
    }
    for (size_t i = 0; i < Subproducers.size(); ++i) {
        Subproducers[i].Producer = nullptr;
        FallbackSubproducers[i].Producer = nullptr;
        DestroyWrites(error, FallbackSubproducers[i].LastHopePendingWrites);
    }
    DestroyWrites(error, PendingWrites);
    DeadPromise.SetValue(error);

    DestroyPQLibRef();
}

void TMultiClusterProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, TProducerSeqNo seqNo, TData data) noexcept {
    return promise.SetValue(MakeErrorCommitResponse("MultiCluster producer doesn't allow to write with explicitly specified seq no.", seqNo, data));
}

void TMultiClusterProducer::Write(NThreading::TPromise<TProducerCommitResponse>& promise, TData data) noexcept {
    Y_VERIFY(data.IsEncoded());

    if (State != EState::Working) {
        TWriteResponse res;
        res.MutableError()->SetDescription(TStringBuilder() << "producer is " << (State == EState::Dead ? "dead" : "not ready") << ". Marker# PQLib01");
        res.MutableError()->SetCode(NErrorCode::ERROR);
        promise.SetValue(TProducerCommitResponse{0, std::move(data), std::move(res)});
        return;
    }

    TWriteInfo* wi = new TWriteInfo();
    PendingWrites.PushBack(wi);

    const size_t i = WeightIndex.RandomChoose();
    wi->Data = std::move(data);
    wi->ResponsePromise = promise;
    DelegateWrite(wi, i);
}

void TMultiClusterProducer::DelegateWrite(TWriteInfo* wi, size_t i, bool fallback) {
    if (wi->History.empty() || wi->History.back() != i) {
        wi->History.push_back(i);
    }
    DEBUG_LOG("Delegate" << (fallback ? " fallback" : "") << " write to subproducer[" << i << "]", Settings.SourceIdPrefix, "");
    wi->ResponseFuture = GetSubproducers(fallback)[i].Producer->Write(wi->Data);
    wi->WaitingCallback = true;

    std::weak_ptr<TMultiClusterProducer> self(shared_from_this());
    PQLib->Subscribe(wi->ResponseFuture,
                     this,
                     [self, wi](const auto&) {
                         auto selfShared = self.lock();
                         if (selfShared) {
                             selfShared->OnWriteResponse(wi);
                         }
                     });
}

void TMultiClusterProducer::ResendLastHopeQueue(size_t i) {
    TSubproducerInfo& info = FallbackSubproducers[i];
    if (!info.LastHopePendingWrites.Empty()) {
        for (TWriteInfo& wi : info.LastHopePendingWrites) {
            if (!wi.WaitingCallback) {
                DelegateWrite(&wi, i, true);
            }
        }
    }
}

void TMultiClusterProducer::OnWriteResponse(TWriteInfo* wi) {
    if (State == EState::Dead) {
        return;
    }

    Y_VERIFY(wi->WaitingCallback);
    Y_VERIFY(!wi->History.empty());

    DEBUG_LOG("Write response: " << wi->ResponseFuture.GetValue().Response, Settings.SourceIdPrefix, "");
    wi->WaitingCallback = false;
    if (wi->ResponseFuture.GetValue().Response.HasAck()) {
        wi->ResponsePromise.SetValue(wi->ResponseFuture.GetValue());
        wi->Unlink();
        delete wi;
    } else if (wi->ResponseFuture.GetValue().Response.HasError()) {
        // retry write to other dc
        if (wi->LastHope) {
            const size_t i = wi->History.back();
            if (FallbackWeightIndex.Enabled(i)) {
                DelegateWrite(wi, i, true);
            } // else we will retry when this producer will become online.
            return;
        }
        TWeightIndex index = FallbackWeightIndex;
        for (size_t i : wi->History) {
            index.Disable(i);
        }

        // choose new fallback dc
        size_t i;
        if (index.EnabledCount() == 0) {
            i = wi->History.back();
        } else {
            i = index.RandomChoose();
        }

        // define whether this dc is the last hope
        const bool lastHope = index.EnabledCount() <= 1;
        if (lastHope) {
            wi->LastHope = true;
            wi->Unlink();
            FallbackSubproducers[i].LastHopePendingWrites.PushBack(wi);
        }

        DEBUG_LOG("Retrying write to subproducer[" << i << "], lastHope: " << lastHope, Settings.SourceIdPrefix, "");
        DelegateWrite(wi, i, true);
    } else { // incorrect structure of response
        Y_VERIFY(false);
    }
}

NThreading::TFuture<TError> TMultiClusterProducer::IsDead() noexcept {
    return DeadPromise.GetFuture();
}

TString TMultiClusterProducer::MakeSourceId(const TStringBuf& prefix, const TStringBuf& srcSourceId,
                                       size_t producerIndex, bool fallback) {
    TStringBuilder ret;
    ret << prefix << "-" << producerIndex;
    if (!srcSourceId.empty()) {
        ret << "-"sv << srcSourceId;
    }
    if (fallback) {
        ret << "-multicluster-fallback"sv;
    } else {
        ret << "-multicluster"sv;
    }
    return std::move(ret);
}

std::shared_ptr<IProducerImpl> TMultiClusterProducer::CreateSubproducer(const TProducerSettings& srcSettings,
                                                                   size_t index, bool fallback) {
    TProducerSettings settings = srcSettings;
    settings.SourceId = MakeSourceId(Settings.SourceIdPrefix, srcSettings.SourceId, index, fallback);
    return PQLib->CreateRawRetryingProducer(settings, DestroyEventRef, Logger);
}

void TMultiClusterProducer::SubscribeDestroyed() {
    NThreading::TPromise<void> promise = ProducersDestroyed;
    auto handler = [promise](const auto&) mutable {
        promise.SetValue();
    };
    std::vector<NThreading::TFuture<void>> futures;
    futures.reserve(Subproducers.size() * 2 + 1);
    futures.push_back(DestroyedPromise.GetFuture());
    for (size_t i = 0; i < Subproducers.size(); ++i) {
        if (Subproducers[i].Producer) {
            futures.push_back(Subproducers[i].Producer->Destroyed());
        }
        if (FallbackSubproducers[i].Producer) {
            futures.push_back(FallbackSubproducers[i].Producer->Destroyed());
        }
    }
    WaitExceptionOrAll(futures).Subscribe(handler);
}

NThreading::TFuture<void> TMultiClusterProducer::Destroyed() noexcept {
    return ProducersDestroyed.GetFuture();
}

void TMultiClusterProducer::Cancel() {
    Destroy(GetCancelReason());
}

} // namespace NPersQueue

#include "agent_impl.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blob_depot/config.h>
#include <ydb/core/blob_depot/s3_error.h>
#include <ydb/core/blob_depot/s3_router_events.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>

#include <util/generic/scope.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    namespace {
        struct TS3Write : TRequestContext {
            std::atomic_bool Retried{false};
        };
    }

    void TBlobDepotAgent::InitS3(const TString& name) {
        if (S3BackendSettings) {
            auto& settings = S3BackendSettings->GetSettings();
            S3RouterGeneration = BlobDepotGeneration;
            S3WrapperId = {};
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
                new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(TabletId, S3RouterGeneration, *S3BackendSettings),
                0, ConnectionInstance);
            S3BasePath = MakeS3BasePath(settings, name);
        }
    }

    struct TBlobDepotAgent::TEvS3Result : TEventLocal<TEvS3Result, TEvPrivate::EvS3Result> {
        ES3Operation Operation;
        std::unique_ptr<IEventHandle> Response;

        TEvS3Result(ES3Operation operation, std::unique_ptr<IEventHandle> response)
            : Operation(operation)
            , Response(std::move(response))
        {}
    };

    // S3 wrappers address the original sender, without echoing the request cookie.
    class TBlobDepotAgent::TS3ReplyActor : public TActor<TS3ReplyActor> {
        const TActorId AgentId;
        const ui64 RequestId;
        const ES3Operation Operation;

        void Finish(TAutoPtr<IEventHandle> ev) {
            Send(AgentId, new TEvS3Result(Operation, std::unique_ptr<IEventHandle>(ev.Release())), 0, RequestId);
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            fFunc(NWrappers::NExternalStorage::EvGetObjectResponse, Finish)
            fFunc(NWrappers::NExternalStorage::EvPutObjectResponse, Finish)
            fFunc(TEvents::TSystem::Undelivered, Finish)
            fFunc(TEvents::TSystem::Poison, Finish)
        )

    public:
        TS3ReplyActor(TActorId agentId, ui64 requestId, ES3Operation operation)
            : TActor(&TThis::StateFunc)
            , AgentId(agentId)
            , RequestId(requestId)
            , Operation(operation)
        {}
    };

    void TBlobDepotAgent::HandleS3Result(TAutoPtr<IEventHandle> ev) {
        auto& result = *ev->Get<TEvS3Result>();
        switch (result.Operation) {
            case ES3Operation::Read:  HandleS3ReadResult(ev->Cookie, *result.Response); break;
            case ES3Operation::Write: HandleS3WriteResult(ev->Cookie, *result.Response); break;
        }
    }

    void TBlobDepotAgent::TQuery::IssueReadS3(const TString& key, ui32 offset, ui32 len, TFinishCallback finish, ui64 readId) {
        LimitS3Deadline(Deadline);
        auto read = std::make_shared<TS3Read>();
        read->RequestId = Agent.NextOtherRequestId++;
        read->Deadline = DeadlineMonotonic;
        read->ConnectionInstance = Agent.ConnectionInstance;
        read->Key = key;
        read->Offset = offset;
        read->Len = len;
        read->Finish = std::move(finish);
        read->ReadId = readId;
        read->Span = NWilson::TSpan(TWilsonBlobDepot::AgentInternals, Span.GetTraceId(),
            "BlobDepotAgent.ReadS3", NWilson::EFlags::AUTO_END);
        if (read->Span) {
            read->Span.Attribute("size", static_cast<i64>(len));
        }

        Agent.RegisterRequest(read->RequestId, this, read, [agent = &Agent, system = TActivationContext::ActorSystem(), read] {
            if (read->Queued) {
                agent->PendingS3Reads.erase(*read->Queued);
                *agent->S3GetsPendingQueueSizeCounter = agent->PendingS3Reads.size();
            } else if (read->ReplyActorId) {
                system->Send(read->ReplyActorId, new TEvents::TEvPoison);
            }
        }, false);
        Agent.IssueOrEnqueueS3Read(*read);
    }

    void TBlobDepotAgent::IssueOrEnqueueS3Read(TS3Read& read) {
        ApplyMaxS3GetsInFlight();

        const TMonotonic now = TActivationContext::Monotonic();
        const bool timeThrottled = now < S3GetThrottleUntil;
        const bool concurrencyThrottled = S3GetsInFlight >= CurrentMaxS3GetsInFlight;

        if (IsWaitingForS3Router() || timeThrottled || concurrencyThrottled) {
            YDB_LOG_DEBUG_COMP(BLOB_DEPOT_AGENT, "S3 read queued",
                {"marker", "BDA65"},
                {"agentId", LogId},
                {"readId", read.ReadId},
                {"key", read.Key},
                {"timeThrottled", timeThrottled},
                {"concurrencyThrottled", concurrencyThrottled},
                {"S3GetsInFlight", S3GetsInFlight},
                {"currentMaxS3GetsInFlight", CurrentMaxS3GetsInFlight},
                {"queueSize", PendingS3Reads.size()});
            read.Queued = PendingS3Reads.insert(PendingS3Reads.end(), read.RequestId);
            *S3GetsPendingQueueSizeCounter = PendingS3Reads.size();
            if (timeThrottled && !S3GetWakeupScheduled) {
                TActivationContext::Schedule(S3GetThrottleUntil, new IEventHandle(TEvPrivate::EvS3GetThrottleWakeup,
                    0, SelfId(), {}, nullptr, 0));
                S3GetWakeupScheduled = true;
            }
            return;
        }

        DispatchS3Read(read);
    }

    void TBlobDepotAgent::DispatchS3Read(TS3Read& read) {
        if (TActivationContext::Monotonic() >= read.Deadline) {
            return; // The watchdog will complete the query with TIMEOUT.
        }
        if (Mode != EMode::Connected || !S3BackendSettings || !S3WrapperId || read.ConnectionInstance != ConnectionInstance) {
            ++*S3GetsError;
            auto request = ExtractRequest(read.RequestId, OtherRequestInFlight);
            request.value().Context->Obtain<TS3Read>().Complete(NKikimrProto::ERROR, {}, "S3 route changed before read");
            return;
        }
        ++S3GetsInFlight;
        *S3GetsInFlightCounter = S3GetsInFlight;

        YDB_LOG_DEBUG_COMP(BLOB_DEPOT_AGENT, "Starting S3 read",
            {"marker", "BDA66"},
            {"agentId", LogId},
            {"readId", read.ReadId},
            {"key", read.Key},
            {"offset", read.Offset},
            {"len", read.Len},
            {"slowDownRetries", read.SlowDownRetries},
            {"S3GetsInFlight", S3GetsInFlight},
            {"currentMaxS3GetsInFlight", CurrentMaxS3GetsInFlight});

        read.ReplyActorId = RegisterWithSameMailbox(new TS3ReplyActor(SelfId(), read.RequestId, ES3Operation::Read));
        if (!S3BackendSettings || !S3WrapperId) {
            Send(read.ReplyActorId, new TEvents::TEvUndelivered(NWrappers::TEvExternalStorage::TEvGetObjectRequest::EventType,
                TEvents::TEvUndelivered::ReasonActorUnknown));
            return;
        }
        auto request = Aws::S3::Model::GetObjectRequest()
            .WithBucket(S3BackendSettings->GetSettings().GetBucket())
            .WithKey(read.Key)
            .WithRange(TStringBuilder() << "bytes=" << read.Offset << '-' << ui64(read.Offset) + read.Len - 1);
        request.SetContinueRequestHandler([active = read.weak_from_this()](const auto*) { return !active.expired(); });
        TActivationContext::Send(new IEventHandle(S3WrapperId, read.ReplyActorId,
            new NWrappers::TEvExternalStorage::TEvGetObjectRequest(request, std::make_shared<TS3GetContext>(read.Deadline)),
            IEventHandle::FlagTrackDelivery));
    }

    void TBlobDepotAgent::HandleS3ReadResult(ui64 id, IEventHandle& result) {
        bool success = false;
        Y_DEFER { OnS3GetCompleted(success); };
        auto it = OtherRequestInFlight.find(id);
        if (it == OtherRequestInFlight.end()) {
            return;
        }
        auto read = std::static_pointer_cast<TS3Read>(it->Context);
        read->ReplyActorId = {};
        if (TActivationContext::Monotonic() >= read->Deadline) {
            ExtractRequest(id, OtherRequestInFlight);
            return;
        }
        auto* msg = result.Type == TEvents::TSystem::Undelivered ? nullptr
            : result.Get<NWrappers::TEvExternalStorage::TEvGetObjectResponse>();
        if (msg && msg->IsSuccess() && msg->Body.size() == read->Len) {
            success = true;
            ++*S3GetsOk;
            *S3GetBytesOk += msg->Body.size();
            ExtractRequest(id, OtherRequestInFlight);
            read->Complete(NKikimrProto::OK, std::move(msg->Body), "");
            return;
        }

        ++*S3GetsError;
        NKikimrProto::EReplyStatus status = NKikimrProto::ERROR;
        TString reason;
        if (!msg) {
            reason = "wrapper actor terminated";
        } else if (msg->IsSuccess()) {
            reason = TStringBuilder() << "S3 read returned " << msg->Body.size() << " bytes instead of " << read->Len;
        } else {
            const auto& error = msg->GetError();
            IncS3HttpErrorCounter("Gets", static_cast<int>(error.GetResponseCode()));
            if (read->ConnectionInstance != ConnectionInstance || !S3BackendSettings) {
                reason = "S3 route changed during read";
            } else if (IsS3SlowDown(error)) {
                ++*S3GetsSlowDown;
                NotifyS3GetSlowDown();
                if (read->SlowDownRetries < MaxS3GetSlowDownRetries) {
                    ++read->SlowDownRetries;
                    IssueOrEnqueueS3Read(*read);
                    return;
                }
                reason = TStringBuilder() << "too many S3 SlowDown retries: " << error.GetMessage();
            } else if (IsS3MissingKey(error)) {
                status = NKikimrProto::NODATA;
                reason = "data has disappeared from S3";
            } else {
                reason = error.GetMessage();
            }
        }
        ExtractRequest(id, OtherRequestInFlight);
        read->Complete(status, {}, reason.c_str());
    }

    void TBlobDepotAgent::NotifyS3GetSlowDown() {
        CurrentMaxS3GetsInFlight = 1;
        *S3GetsMaxInFlightCounter = CurrentMaxS3GetsInFlight;
        ConsecutiveSuccessfulGetBatches = 0;
        ++*S3GetThrottleActivations;
        const TDuration delay = S3GetBackoff.Next();
        S3GetThrottleUntil = TActivationContext::Monotonic() + delay;

        YDB_LOG_WARN_COMP(BLOB_DEPOT_AGENT, "S3 get throttled",
            {"marker", "BDA67"},
            {"agentId", LogId},
            {"delay", delay},
            {"currentMaxS3GetsInFlight", CurrentMaxS3GetsInFlight},
            {"S3GetsInFlight", S3GetsInFlight},
            {"queueSize", PendingS3Reads.size()});
        YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "S3_get_throttled",
            {"marker", "BDEV44"},
            {"VG", VirtualGroupId},
            {"BDT", TabletId},
            {"G", BlobDepotGeneration},
            {"delayMs", delay.MilliSeconds()},
            {"queueSize", PendingS3Reads.size()});

        if (!S3GetWakeupScheduled) {
            TActivationContext::Schedule(S3GetThrottleUntil, new IEventHandle(TEvPrivate::EvS3GetThrottleWakeup,
                0, SelfId(), {}, nullptr, 0));
            S3GetWakeupScheduled = true;
        }
    }

    void TBlobDepotAgent::OnS3GetCompleted(bool success) {
        Y_ABORT_UNLESS(S3GetsInFlight);
        --S3GetsInFlight;
        *S3GetsInFlightCounter = S3GetsInFlight;

        const ui32 maxS3GetsInFlight = MaxS3GetsInFlight();
        if (success && CurrentMaxS3GetsInFlight < maxS3GetsInFlight) {
            if (++ConsecutiveSuccessfulGetBatches >= SuccessesPerGetConcurrencyStepUp) {
                ConsecutiveSuccessfulGetBatches = 0;
                ++CurrentMaxS3GetsInFlight;
                if (CurrentMaxS3GetsInFlight >= maxS3GetsInFlight) {
                    CurrentMaxS3GetsInFlight = maxS3GetsInFlight;
                    S3GetBackoff.Reset();
                }

                *S3GetsMaxInFlightCounter = CurrentMaxS3GetsInFlight;
            }
        }

        RunPendingS3ReadsIfPossible();
    }

    void TBlobDepotAgent::RunPendingS3ReadsIfPossible() {
        if (IsWaitingForS3Router()) {
            return;
        }
        const TMonotonic now = TActivationContext::Monotonic();
        if (now < S3GetThrottleUntil) {
            if (!S3GetWakeupScheduled && !PendingS3Reads.empty()) {
                TActivationContext::Schedule(S3GetThrottleUntil, new IEventHandle(TEvPrivate::EvS3GetThrottleWakeup,
                    0, SelfId(), {}, nullptr, 0));
                S3GetWakeupScheduled = true;
            }
            return;
        }

        ApplyMaxS3GetsInFlight();

        while (!PendingS3Reads.empty() && S3GetsInFlight < CurrentMaxS3GetsInFlight) {
            const auto it = OtherRequestInFlight.find(PendingS3Reads.front());
            Y_ABORT_UNLESS(it != OtherRequestInFlight.end());
            auto& read = it->Context->Obtain<TS3Read>();
            read.Queued.reset();
            PendingS3Reads.pop_front();
            DispatchS3Read(read);
        }
        *S3GetsPendingQueueSizeCounter = PendingS3Reads.size();
    }

    void TBlobDepotAgent::HandleS3GetThrottleWakeup() {
        S3GetWakeupScheduled = false;
        RunPendingS3ReadsIfPossible();
    }

    void TBlobDepotAgent::RetirePendingS3Reads() {
        while (!PendingS3Reads.empty()) {
            auto request = ExtractRequest(PendingS3Reads.front(), OtherRequestInFlight);
            PendingS3Reads.pop_front();
            auto& read = request.value().Context->Obtain<TS3Read>();
            read.Queued.reset();
            if (TActivationContext::Monotonic() < read.Deadline) {
                read.Complete(NKikimrProto::ERROR, {}, "S3 route removed during read");
            }
        }
        *S3GetsPendingQueueSizeCounter = 0;
    }

    void TBlobDepotAgent::HandleS3WriteResult(ui64 id, IEventHandle& result) {
        Y_ABORT_UNLESS(S3PutsInFlight);
        *S3PutsInFlightCounter = --S3PutsInFlight;
        auto it = OtherRequestInFlight.find(id);
        if (it == OtherRequestInFlight.end()) {
            return;
        }
        TS3PutResult outcome;
        outcome.MayArriveLate = it->Context->Obtain<TS3Write>().Retried.load();
        if (result.Type == TEvents::TSystem::Undelivered) {
            outcome.Error = "event undelivered";
            outcome.MayArriveLate |= result.Get<TEvents::TEvUndelivered>()->Unsure;
        } else {
            const auto& msg = *result.Get<NWrappers::TEvExternalStorage::TEvPutObjectResponse>();
            if (!msg.IsSuccess()) {
                outcome.Error.emplace(msg.GetError().GetMessage());
                outcome.SlowDown = IsS3SlowDown(msg.GetError());
                const int status = static_cast<int>(msg.GetError().GetResponseCode());
                outcome.MayArriveLate |= S3PutMayArriveAfterError(msg.GetError());
                IncS3HttpErrorCounter("Puts", status);
            }
        }
        OnRequestComplete(id, std::move(outcome), OtherRequestInFlight);
    }

    void TBlobDepotAgent::TQuery::IssueWriteS3(TString&& key, TRope&& buffer, TLogoBlobID id, TS3Locator locator) {
        ++Agent.S3PutsInFlight;
        *Agent.S3PutsInFlightCounter = Agent.S3PutsInFlight;

        const ui64 requestId = Agent.NextOtherRequestId++;
        const auto writerActorId = Agent.RegisterWithSameMailbox(new TS3ReplyActor(Agent.SelfId(), requestId, ES3Operation::Write));
        auto write = std::make_shared<TS3Write>();
        Agent.RegisterRequest(requestId, this, write, [system = TActivationContext::ActorSystem(), writerActorId] {
            system->Send(writerActorId, new TEvents::TEvPoison);
        }, false);

        YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "Issue_S3_write",
            {"marker", "BDEV38"},
            {"VG", Agent.VirtualGroupId},
            {"BDT", Agent.TabletId},
            {"G", Agent.BlobDepotGeneration},
            {"Q", QueryId},
            {"blobId", id},
            {"locator", locator});

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithBucket(Agent.S3BackendSettings->GetSettings().GetBucket())
            .WithKey(std::move(key))
            .AddMetadata("key", id.ToString());
        const std::weak_ptr<TS3Write> active = write;
        request.SetContinueRequestHandler([active](const auto*) { return !active.expired(); });
        request.SetRequestRetryHandler([active](const auto&) {
            if (const auto write = active.lock()) {
                write->Retried.store(true);
            }
        });
        TActivationContext::Send(new IEventHandle(Agent.S3WrapperId, writerActorId,
            new NWrappers::TEvExternalStorage::TEvPutObjectRequest(request,
                buffer.ExtractUnderlyingContainerOrCopy<TString>(),
                std::make_shared<TS3PutContext>(DeadlineMonotonic, TS3PutContext::ESource::Agent)),
            IEventHandle::FlagTrackDelivery));
    }

} // NKikimr::NBlobDepot

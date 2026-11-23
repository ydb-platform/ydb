#include "agent_impl.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blob_depot/config.h>
#include <ydb/core/blob_depot/s3_error.h>
#include <ydb/core/blob_depot/s3_router_events.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT_AGENT

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::InitS3(const TString& name) {
        if (S3BackendSettings) {
            auto& settings = S3BackendSettings->GetSettings();
            // Fire-and-forget acquire: NodeWarden registers the per-node router under its
            // well-known service id before any later event we send can be processed, so
            // we can use the service id immediately.
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
                new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(TabletId, BlobDepotGeneration, *S3BackendSettings));
            S3WrapperId = MakeBlobDepotS3RouterID(TabletId);
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
        auto read = std::make_shared<TS3Read>();
        read->RequestId = Agent.NextOtherRequestId++;
        read->Deadline = DeadlineMonotonic;
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

        Agent.RegisterRequest(read->RequestId, this, read, [agent = &Agent, read] {
            if (read->Queued) {
                agent->PendingS3Reads.erase(*read->Queued);
                *agent->S3GetsPendingQueueSizeCounter = agent->PendingS3Reads.size();
            }
        }, false);
        Agent.IssueOrEnqueueS3Read(*read);
    }

    void TBlobDepotAgent::IssueOrEnqueueS3Read(TS3Read& read) {
        ApplyMaxS3GetsInFlight();

        const TMonotonic now = TActivationContext::Monotonic();
        const bool timeThrottled = now < S3GetThrottleUntil;
        const bool concurrencyThrottled = S3GetsInFlight >= CurrentMaxS3GetsInFlight;

        if (timeThrottled || concurrencyThrottled) {
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

    void TBlobDepotAgent::DispatchS3Read(const TS3Read& read) {
        if (TActivationContext::Monotonic() >= read.Deadline) {
            return; // The watchdog will complete the query with TIMEOUT.
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

        const TActorId actorId = RegisterWithSameMailbox(new TS3ReplyActor(SelfId(), read.RequestId, ES3Operation::Read));
        auto request = std::make_unique<NWrappers::TEvExternalStorage::TEvGetObjectRequest>(
            Aws::S3::Model::GetObjectRequest()
                .WithBucket(S3BackendSettings->GetSettings().GetBucket())
                .WithKey(read.Key)
                .WithRange(TStringBuilder() << "bytes=" << read.Offset << '-' << ui64(read.Offset) + read.Len - 1)
        );
        TActivationContext::Send(new IEventHandle(S3WrapperId, actorId, request.release(), IEventHandle::FlagTrackDelivery));
    }

    void TBlobDepotAgent::HandleS3ReadResult(ui64 id, IEventHandle& result) {
        auto it = OtherRequestInFlight.find(id);
        if (it == OtherRequestInFlight.end()) {
            OnS3GetCompleted(false);
            return;
        }
        auto read = std::static_pointer_cast<TS3Read>(it->Context);
        if (TActivationContext::Monotonic() >= read->Deadline) {
            ExtractRequest(id, OtherRequestInFlight);
            OnS3GetCompleted(false);
            return;
        }
        auto* msg = result.Type == TEvents::TSystem::Undelivered ? nullptr
            : result.Get<NWrappers::TEvExternalStorage::TEvGetObjectResponse>();
        if (msg && msg->IsSuccess() && msg->Body.size() == read->Len) {
            ++*S3GetsOk;
            *S3GetBytesOk += msg->Body.size();
            ExtractRequest(id, OtherRequestInFlight);
            read->Complete(NKikimrProto::OK, std::move(msg->Body), "");
            OnS3GetCompleted(true);
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
            if (IsS3SlowDown(error)) {
                ++*S3GetsSlowDown;
                NotifyS3GetSlowDown();
                if (read->SlowDownRetries < MaxS3GetSlowDownRetries) {
                    ++read->SlowDownRetries;
                    IssueOrEnqueueS3Read(*read);
                    OnS3GetCompleted(false);
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
        OnS3GetCompleted(false);
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

    void TBlobDepotAgent::HandleS3WriteResult(ui64 id, IEventHandle& result) {
        Y_ABORT_UNLESS(S3PutsInFlight);
        *S3PutsInFlightCounter = --S3PutsInFlight;
        if (!OtherRequestInFlight.contains(id)) {
            return;
        }
        TS3PutResult outcome;
        if (result.Type == TEvents::TSystem::Undelivered) {
            outcome.Error = "event undelivered";
        } else {
            const auto& msg = *result.Get<NWrappers::TEvExternalStorage::TEvPutObjectResponse>();
            if (!msg.IsSuccess()) {
                outcome.Error.emplace(msg.GetError().GetMessage());
                outcome.SlowDown = IsS3SlowDown(msg.GetError());
                IncS3HttpErrorCounter("Puts", static_cast<int>(msg.GetError().GetResponseCode()));
            }
        }
        OnRequestComplete(id, std::move(outcome), OtherRequestInFlight);
    }

    void TBlobDepotAgent::TQuery::IssueWriteS3(TString&& key, TRope&& buffer, TLogoBlobID id, TS3Locator locator) {
        ++Agent.S3PutsInFlight;
        *Agent.S3PutsInFlightCounter = Agent.S3PutsInFlight;

        const ui64 requestId = Agent.NextOtherRequestId++;
        Agent.RegisterRequest(requestId, this, nullptr, {}, false);
        const auto writerActorId = Agent.RegisterWithSameMailbox(new TS3ReplyActor(Agent.SelfId(), requestId, ES3Operation::Write));

        YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "Issue_S3_write",
            {"marker", "BDEV38"},
            {"VG", Agent.VirtualGroupId},
            {"BDT", Agent.TabletId},
            {"G", Agent.BlobDepotGeneration},
            {"Q", QueryId},
            {"blobId", id},
            {"locator", locator});

        TActivationContext::Send(new IEventHandle(Agent.S3WrapperId, writerActorId,
            new NWrappers::TEvExternalStorage::TEvPutObjectRequest(
                Aws::S3::Model::PutObjectRequest()
                    .WithBucket(std::move(Agent.S3BackendSettings->GetSettings().GetBucket()))
                    .WithKey(std::move(key))
                    .AddMetadata("key", id.ToString()),
                buffer.ExtractUnderlyingContainerOrCopy<TString>(),
                std::make_shared<TS3PutContext>(TActivationContext::Monotonic() +
                    GetS3PutTimeout(Agent.S3BackendSettings->GetSettings(), AppData()->AwsClientConfig),
                    TS3PutContext::ESource::Agent)),
            IEventHandle::FlagTrackDelivery));

    }

} // NKikimr::NBlobDepot

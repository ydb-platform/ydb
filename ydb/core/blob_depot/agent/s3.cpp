#include "agent_impl.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blob_depot/s3_router_events.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>

namespace NKikimr::NBlobDepot {

    static bool IsSlowDown(const Aws::S3::S3Error& error) {
        return error.GetErrorType() == Aws::S3::S3Errors::SLOW_DOWN
            || error.GetExceptionName() == "SlowDown"
            || error.GetExceptionName() == "TooManyRequests";
    }

    void TBlobDepotAgent::InitS3(const TString& name) {
        if (S3BackendSettings) {
            auto& settings = S3BackendSettings->GetSettings();
            // Fire-and-forget acquire: NodeWarden registers the per-node router under its
            // well-known service id before any later event we send can be processed, so
            // we can use the service id immediately.
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()),
                new NStorage::TEvNodeWardenAcquireBlobDepotS3Router(TabletId, *S3BackendSettings));
            S3WrapperId = MakeBlobDepotS3RouterID(TabletId);
            S3BasePath = TStringBuilder() << settings.GetObjectKeyPattern() << '/' << name;
        }
    }

    void TBlobDepotAgent::TQuery::IssueReadS3(const TString& key, ui32 offset, ui32 len, TFinishCallback finish, ui64 readId) {
        Agent.IssueOrEnqueueS3Read(TPendingS3Read{
            .Key = key,
            .Offset = offset,
            .Len = len,
            .Finish = std::move(finish),
            .ReadId = readId,
        });
    }

    void TBlobDepotAgent::IssueOrEnqueueS3Read(TPendingS3Read&& read) {
        const TMonotonic now = TActivationContext::Monotonic();
        const bool timeThrottled = now < S3GetThrottleUntil;
        const bool concurrencyThrottled = S3GetsInFlight >= CurrentMaxS3GetsInFlight;

        if (timeThrottled || concurrencyThrottled) {
            STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA65, "S3 read queued", (AgentId, LogId), (ReadId, read.ReadId),
                (Key, read.Key), (TimeThrottled, timeThrottled), (ConcurrencyThrottled, concurrencyThrottled),
                (S3GetsInFlight, S3GetsInFlight), (CurrentMaxS3GetsInFlight, CurrentMaxS3GetsInFlight),
                (QueueSize, PendingS3Reads.size()));
            PendingS3Reads.push_back(std::move(read));
            if (timeThrottled && !S3GetWakeupScheduled) {
                TActivationContext::Schedule(S3GetThrottleUntil, new IEventHandle(TEvPrivate::EvS3GetThrottleWakeup,
                    0, SelfId(), {}, nullptr, 0));
                S3GetWakeupScheduled = true;
            }
            return;
        }

        DispatchS3Read(std::move(read));
    }

    void TBlobDepotAgent::DispatchS3Read(TPendingS3Read&& read) {
        class TGetActor : public TActor<TGetActor> {
            TBlobDepotAgent& Agent;
            TPendingS3Read Read;

        public:
            TGetActor(TBlobDepotAgent& agent, TPendingS3Read&& read)
                : TActor(&TThis::StateFunc)
                , Agent(agent)
                , Read(std::move(read))
            {}

            void Handle(NWrappers::TEvExternalStorage::TEvGetObjectResponse::TPtr ev) {
                auto& msg = *ev->Get();

                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA55, "received TEvGetObjectResponse",
                    (AgentId, Agent.LogId), (ReadId, Read.ReadId),
                    (Response, msg.Result), (BodyLen, std::size(msg.Body)));

                if (msg.IsSuccess()) {
                    ++*Agent.S3GetsOk;
                    *Agent.S3GetBytesOk += msg.Body.size();
                    const ui64 bytes = msg.Body.size();
                    Read.Finish(std::move(msg.Body), "");
                    Agent.OnS3GetCompleted(/*success=*/true, bytes);
                    PassAway();
                    return;
                }

                ++*Agent.S3GetsError;
                const auto& error = msg.GetError();

                if (IsSlowDown(error)) {
                    ++*Agent.S3GetsSlowDown;
                    BDEV(BDEV43, "S3_get_slow_down", (VG, Agent.VirtualGroupId), (BDT, Agent.TabletId),
                        (G, Agent.BlobDepotGeneration), (ReadId, Read.ReadId), (Key, Read.Key),
                        (Retry, Read.SlowDownRetries));

                    Agent.NotifyS3GetSlowDown();
                    Agent.OnS3GetCompleted(/*success=*/false, 0);

                    if (Read.SlowDownRetries >= MaxS3GetSlowDownRetries) {
                        const TString reason = TStringBuilder()
                            << "too many S3 SlowDown retries: " << error.GetMessage();
                        Read.Finish(std::nullopt, reason.c_str());
                    } else {
                        ++Read.SlowDownRetries;
                        Agent.IssueOrEnqueueS3Read(std::move(Read));
                    }
                    PassAway();
                    return;
                }

                if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
                    Read.Finish(std::nullopt, "data has disappeared from S3");
                } else {
                    Read.Finish(std::nullopt, error.GetMessage().c_str());
                }
                Agent.OnS3GetCompleted(/*success=*/false, 0);
                PassAway();
            }

            void HandleUndelivered() {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA56, "received TEvUndelivered",
                    (AgentId, Agent.LogId), (ReadId, Read.ReadId));
                ++*Agent.S3GetsError;
                Read.Finish(std::nullopt, "wrapper actor terminated");
                Agent.OnS3GetCompleted(/*success=*/false, 0);
                PassAway();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(NWrappers::TEvExternalStorage::TEvGetObjectResponse, Handle)
                cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )
        };

        ++S3GetsInFlight;

        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA66, "starting S3 read", (AgentId, LogId), (ReadId, read.ReadId),
            (Key, read.Key), (Offset, read.Offset), (Len, read.Len), (SlowDownRetries, read.SlowDownRetries),
            (S3GetsInFlight, S3GetsInFlight), (CurrentMaxS3GetsInFlight, CurrentMaxS3GetsInFlight));

        const TString key = read.Key;
        const ui32 offset = read.Offset;
        const ui32 len = read.Len;

        const TActorId actorId = RegisterWithSameMailbox(new TGetActor(*this, std::move(read)));

        auto request = std::make_unique<NWrappers::TEvExternalStorage::TEvGetObjectRequest>(
            Aws::S3::Model::GetObjectRequest()
                .WithBucket(S3BackendSettings->GetSettings().GetBucket())
                .WithKey(key)
                .WithRange(TStringBuilder() << "bytes=" << offset << '-' << offset + len - 1)
        );
        TActivationContext::Send(new IEventHandle(S3WrapperId, actorId, request.release(), IEventHandle::FlagTrackDelivery));
    }

    void TBlobDepotAgent::NotifyS3GetSlowDown() {
        CurrentMaxS3GetsInFlight = 1;
        ConsecutiveSuccessfulGetBatches = 0;
        const TDuration delay = S3GetBackoff.Next();
        S3GetThrottleUntil = TActivationContext::Monotonic() + delay;

        STLOG(PRI_WARN, BLOB_DEPOT_AGENT, BDA67, "S3 get throttled", (AgentId, LogId),
            (Delay, delay), (CurrentMaxS3GetsInFlight, CurrentMaxS3GetsInFlight),
            (S3GetsInFlight, S3GetsInFlight), (QueueSize, PendingS3Reads.size()));
        BDEV(BDEV44, "S3_get_throttled", (VG, VirtualGroupId), (BDT, TabletId), (G, BlobDepotGeneration),
            (DelayMs, delay.MilliSeconds()), (QueueSize, PendingS3Reads.size()));

        if (!S3GetWakeupScheduled) {
            TActivationContext::Schedule(S3GetThrottleUntil, new IEventHandle(TEvPrivate::EvS3GetThrottleWakeup,
                0, SelfId(), {}, nullptr, 0));
            S3GetWakeupScheduled = true;
        }
    }

    void TBlobDepotAgent::OnS3GetCompleted(bool success, ui64 bytes) {
        Y_UNUSED(bytes);
        Y_ABORT_UNLESS(S3GetsInFlight);
        --S3GetsInFlight;

        if (success && CurrentMaxS3GetsInFlight < MaxS3GetsInFlight) {
            if (++ConsecutiveSuccessfulGetBatches >= SuccessesPerGetConcurrencyStepUp) {
                ConsecutiveSuccessfulGetBatches = 0;
                ++CurrentMaxS3GetsInFlight;
                if (CurrentMaxS3GetsInFlight >= MaxS3GetsInFlight) {
                    CurrentMaxS3GetsInFlight = MaxS3GetsInFlight;
                    S3GetBackoff.Reset();
                }
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

        while (!PendingS3Reads.empty() && S3GetsInFlight < CurrentMaxS3GetsInFlight) {
            auto read = std::move(PendingS3Reads.front());
            PendingS3Reads.pop_front();
            DispatchS3Read(std::move(read));
        }
    }

    void TBlobDepotAgent::HandleS3GetThrottleWakeup() {
        S3GetWakeupScheduled = false;
        RunPendingS3ReadsIfPossible();
    }

    TActorId TBlobDepotAgent::TQuery::IssueWriteS3(TString&& key, TRope&& buffer, TLogoBlobID id, TS3Locator locator) {
        class TWriteActor : public TActor<TWriteActor> {
            std::weak_ptr<TLifetimeToken> LifetimeToken;
            TQuery* const Query;
            TLogoBlobID Id;
            TS3Locator Locator;

        public:
            TWriteActor(std::weak_ptr<TLifetimeToken> lifetimeToken, TQuery *query, TLogoBlobID id, TS3Locator locator)
                : TActor(&TThis::StateFunc)
                , LifetimeToken(std::move(lifetimeToken))
                , Query(query)
                , Id(id)
                , Locator(locator)
            {}

            void Handle(NWrappers::TEvExternalStorage::TEvPutObjectResponse::TPtr ev) {
                auto& msg = *ev->Get();
                if (msg.IsSuccess()) {
                    Finish(std::nullopt, false);
                } else {
                    const auto& error = msg.GetError();
                    Finish(std::make_optional<TString>(error.GetMessage()), IsSlowDown(error));
                }
            }

            void HandleUndelivered() {
                Finish("event undelivered", false);
            }

            void Finish(std::optional<TString>&& error, bool slowDown) {
                if (!LifetimeToken.expired()) {
                    InvokeOtherActor(Query->Agent, &TBlobDepotAgent::Invoke, [&] {
                        auto& Agent = Query->Agent;
                        const auto& QueryId = Query->QueryId;
                        BDEV_QUERY(BDEV37, "written_to_S3", (BlobId, Id), (Locator, Locator));
                        Query->OnPutS3ObjectResponse(std::move(error), slowDown);
                    });
                }
                PassAway();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(NWrappers::TEvExternalStorage::TEvPutObjectResponse, Handle)
                cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )
        };

        if (!LifetimeToken) {
            LifetimeToken = std::make_shared<TLifetimeToken>();
        }

        const TActorId writerActorId = Agent.RegisterWithSameMailbox(new TWriteActor(LifetimeToken, this, id, locator));

        BDEV_QUERY(BDEV38, "issue_S3_write", (BlobId, id), (Locator, locator));

        TActivationContext::Send(new IEventHandle(Agent.S3WrapperId, writerActorId,
            new NWrappers::TEvExternalStorage::TEvPutObjectRequest(
                Aws::S3::Model::PutObjectRequest()
                    .WithBucket(std::move(Agent.S3BackendSettings->GetSettings().GetBucket()))
                    .WithKey(std::move(key))
                    .AddMetadata("key", id.ToString()),
                buffer.ExtractUnderlyingContainerOrCopy<TString>()),
            IEventHandle::FlagTrackDelivery));

        return writerActorId;
    }

} // NKikimr::NBlobDepot

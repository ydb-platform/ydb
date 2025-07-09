#include "agent_impl.h"

#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::InitS3(const TString& name) {
        if (S3BackendSettings) {
            auto& settings = S3BackendSettings->GetSettings();
            auto externalStorageConfig = NWrappers::IExternalStorageConfig::Construct(settings);
            S3WrapperId = Register(NWrappers::CreateS3Wrapper(externalStorageConfig->ConstructStorageOperator()));
            S3BasePath = TStringBuilder() << settings.GetObjectKeyPattern() << '/' << name;
        }
    }

    void TBlobDepotAgent::TQuery::IssueReadS3(const TString& key, ui32 offset, ui32 len, TFinishCallback finish, ui64 readId) {
        class TGetActor : public TActor<TGetActor> {
            TFinishCallback Finish;

            TString AgentLogId;
            TString QueryId;
            ui64 ReadId;

            NMonitoring::TDynamicCounters::TCounterPtr GetBytesOk;
            NMonitoring::TDynamicCounters::TCounterPtr GetsOk;
            NMonitoring::TDynamicCounters::TCounterPtr GetsError;

        public:
            TGetActor(TQuery *query, TFinishCallback finish, ui64 readId,
                    NMonitoring::TDynamicCounters::TCounterPtr getBytesOk,
                    NMonitoring::TDynamicCounters::TCounterPtr getsOk,
                    NMonitoring::TDynamicCounters::TCounterPtr getsError)
                : TActor(&TThis::StateFunc)
                , Finish(std::move(finish))
                , AgentLogId(query->Agent.LogId)
                , QueryId(query->GetQueryId())
                , ReadId(readId)
                , GetBytesOk(std::move(getBytesOk))
                , GetsOk(std::move(getsOk))
                , GetsError(std::move(getsError))
            {}

            void Handle(NWrappers::TEvExternalStorage::TEvGetObjectResponse::TPtr ev) {
                auto& msg = *ev->Get();

                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA55, "received TEvGetObjectResponse",
                    (AgentId, AgentLogId), (QueryId, QueryId), (ReadId, ReadId),
                    (Response, msg.Result), (BodyLen, std::size(msg.Body)));

                if (msg.IsSuccess()) {
                    ++*GetsOk;
                    *GetBytesOk += msg.Body.size();
                } else {
                    ++*GetsError;
                }

                if (msg.IsSuccess()) {
                    Finish(std::move(msg.Body), "");
                } else if (const auto& error = msg.GetError(); error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
                    Finish(std::nullopt, "data has disappeared from S3");
                } else {
                    Finish(std::nullopt, msg.GetError().GetMessage().c_str());
                }
                PassAway();
            }

            void HandleUndelivered() {
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA56, "received TEvUndelivered",
                    (AgentId, AgentLogId), (QueryId, QueryId), (ReadId, ReadId));
                Finish(std::nullopt, "wrapper actor terminated");
                PassAway();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(NWrappers::TEvExternalStorage::TEvGetObjectResponse, Handle)
                cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
                cFunc(TEvents::TSystem::Poison, PassAway)
            )
        };
        const TActorId actorId = Agent.RegisterWithSameMailbox(new TGetActor(this, std::move(finish), readId,
            Agent.S3GetBytesOk, Agent.S3GetsOk, Agent.S3GetsError));
        auto request = std::make_unique<NWrappers::TEvExternalStorage::TEvGetObjectRequest>(
            Aws::S3::Model::GetObjectRequest()
                .WithBucket(Agent.S3BackendSettings->GetSettings().GetBucket())
                .WithKey(std::move(key))
                .WithRange(TStringBuilder() << "bytes=" << offset << '-' << offset + len - 1)
        );
        TActivationContext::Send(new IEventHandle(Agent.S3WrapperId, actorId, request.release(), IEventHandle::FlagTrackDelivery));
    }

    TActorId TBlobDepotAgent::TQuery::IssueWriteS3(TString&& key, TRope&& buffer, TLogoBlobID id) {
        class TWriteActor : public TActor<TWriteActor> {
            std::weak_ptr<TLifetimeToken> LifetimeToken;
            TQuery* const Query;

        public:
            TWriteActor(std::weak_ptr<TLifetimeToken> lifetimeToken, TQuery *query)
                : TActor(&TThis::StateFunc)
                , LifetimeToken(std::move(lifetimeToken))
                , Query(query)
            {}

            void Handle(NWrappers::TEvExternalStorage::TEvPutObjectResponse::TPtr ev) {
                auto& msg = *ev->Get();
                Finish(msg.IsSuccess()
                    ? std::nullopt
                    : std::make_optional<TString>(msg.GetError().GetMessage()));
            }

            void HandleUndelivered() {
                Finish("event undelivered");
            }

            void Finish(std::optional<TString>&& error) {
                if (!LifetimeToken.expired()) {
                    InvokeOtherActor(Query->Agent, &TBlobDepotAgent::Invoke, [&] {
                        Query->OnPutS3ObjectResponse(std::move(error));
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

        const TActorId writerActorId = Agent.RegisterWithSameMailbox(new TWriteActor(LifetimeToken, this));

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

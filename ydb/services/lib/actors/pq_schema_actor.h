#pragma once

#include <ydb/core/grpc_services/rpc_scheme_base.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NGRpcProxy::V1 {

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
        const TString& name,
        const Ydb::PersQueue::V1::TopicSettings& settings,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        const TActorContext& ctx,
        bool alter,
        TString& error
    );

    struct TClientServiceType {
        TString Name;
        ui32 MaxCount;
    };
    typedef std::map<TString, TClientServiceType> TClientServiceTypes;
    TClientServiceTypes GetSupportedClientServiceTypes(const TActorContext& ctx);

    // Returns true if have duplicated read rules
    bool CheckReadRulesConfig(const NKikimrPQ::TPQTabletConfig& config, const TClientServiceTypes& supportedReadRuleServiceTypes, TString& error);

    TString AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig *config,
        const Ydb::PersQueue::V1::TopicSettings::ReadRule& rr,
        const TClientServiceTypes& supportedReadRuleServiceTypes,
        const TActorContext& ctx
    );
    TString RemoveReadRuleFromConfig(
        NKikimrPQ::TPQTabletConfig *config,
        const NKikimrPQ::TPQTabletConfig& originalConfig,
        const TString& consumerName,
        const TActorContext& ctx
    );
    NYql::TIssue FillIssue(const TString &errorReason, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode);


    template<class TDerived, class TRequest>
    class TPQGrpcSchemaBase : public NKikimr::NGRpcService::TRpcSchemeRequestActor<TDerived, TRequest> {
    protected:
        using TBase = NKikimr::NGRpcService::TRpcSchemeRequestActor<TDerived, TRequest>;

    public:
        TPQGrpcSchemaBase(TRequest *request, const TString& topicPath)
            : TBase(request)
            , TopicPath(topicPath)
        {
        }

        void PrepareTopicPath(const NActors::TActorContext &ctx) { // ToDo !!
            TopicPath = NPersQueue::GetFullTopicPath(ctx, this->Request_->GetDatabaseName(), TopicPath);
        }

        TString GetTopicPath(const NActors::TActorContext& ctx) {
            PrepareTopicPath(ctx);
            return TopicPath;
        }

    protected:
        // TDerived must implement FillProposeRequest(TEvProposeTransaction&, const TActorContext& ctx, TString workingDir, TString name);
        void SendProposeRequest(const NActors::TActorContext &ctx) {
            PrepareTopicPath(ctx);
            std::pair <TString, TString> pathPair;
            try {
                pathPair = NKikimr::NGRpcService::SplitPath(TopicPath);
            } catch (const std::exception &ex) {
                this->Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
                return this->ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
            }

            const auto &workingDir = pathPair.first;
            const auto &name = pathPair.second;

            std::unique_ptr <TEvTxUserProxy::TEvProposeTransaction> proposal(
                    new TEvTxUserProxy::TEvProposeTransaction());

            SetDatabase(proposal.get(), *this->Request_);

            if (this->Request_->GetInternalToken().empty()) {
                if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                    return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                          "Unauthenticated access is forbidden, please provide credentials", ctx);
                }
            } else {
                proposal->Record.SetUserToken(this->Request_->GetInternalToken());
            }

            static_cast<TDerived*>(this)->FillProposeRequest(*proposal, ctx, workingDir, name);

            if (!IsDead) {
                ctx.Send(MakeTxProxyID(), proposal.release());
            }
        }

        void SendDescribeProposeRequest(const NActors::TActorContext& ctx) {
            PrepareTopicPath(ctx);
            auto navigateRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
            navigateRequest->DatabaseName = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = NKikimr::SplitPath(TopicPath);
            entry.SyncVersion = true;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTopic;
            navigateRequest->ResultSet.emplace_back(entry);

            if (this->Request_->GetInternalToken().empty()) {
                if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                    return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                          "Unauthenticated access is forbidden, please provide credentials", ctx);
                }
            } else {
                navigateRequest->UserToken = new NACLib::TUserToken(this->Request_->GetInternalToken());
            }
            if (!IsDead) {
                ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigateRequest.release()));
            }
        }

        bool ReplyIfNotTopic(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_VERIFY(result->ResultSet.size() == 1);
            const auto& response = result->ResultSet.front();
            const TString path  = JoinPath(response.Path);

            if (ev->Get()->Request.Get()->ResultSet.size() != 1 ||
                ev->Get()->Request.Get()->ResultSet.begin()->Kind !=
                NSchemeCache::TSchemeCacheNavigate::KindTopic) {
                this->Request_->RaiseIssue(
                    FillIssue(
                              TStringBuilder() << "path '" << path << "' is not a stream",
                              Ydb::PersQueue::ErrorCode::ERROR
                              )
                    );
                TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                return true;
            }

            return false;
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_VERIFY(result->ResultSet.size() == 1);
            const auto& response = result->ResultSet.front();
            const TString path  = JoinPath(response.Path);

            switch (response.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok: {
                return static_cast<TDerived*>(this)->HandleCacheNavigateResponse(ev, ctx);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "path '" << path << "' does not exist or you " <<
                        "do not have access rights",
                        Ydb::PersQueue::ErrorCode::ERROR
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "table creation is not completed",
                        Ydb::PersQueue::ErrorCode::ERROR
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "path '" << path << "' is not a table",
                        Ydb::PersQueue::ErrorCode::ERROR
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "unknown database root",
                        Ydb::PersQueue::ErrorCode::ERROR
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            break;

            default:
                return TBase::Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }

        void ReplyWithError(Ydb::StatusIds::StatusCode status,
                            Ydb::PersQueue::ErrorCode::ErrorCode pqStatus,
                            const TString& messageText, const NActors::TActorContext& ctx) {
            this->Request_->RaiseIssue(FillIssue(messageText, pqStatus));
            this->Request_->ReplyWithYdbStatus(status);
            this->Die(ctx);
            IsDead = true;
        }

        void ReplyWithResult(Ydb::StatusIds::StatusCode status, const NActors::TActorContext& ctx) {
            this->Request_->ReplyWithYdbStatus(status);
            this->Die(ctx);
            IsDead = true;
        }

        template<class TProtoResult>
        void ReplyWithResult(Ydb::StatusIds::StatusCode status, const TProtoResult& result, const NActors::TActorContext& ctx) {
            this->Request_->SendResult(result, status);
            this->Die(ctx);
            IsDead = true;
        }

        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default: TBase::StateWork(ev, ctx);
            }
        }

    private:
        bool IsDead = false;
        TString TopicPath;
    };

    //-----------------------------------------------------------------------------------

    template<class TDerived, class TRequest>
    class TUpdateSchemeActor : public TPQGrpcSchemaBase<TDerived, TRequest> {
        using TBase = TPQGrpcSchemaBase<TDerived, TRequest>;

    public:
        TUpdateSchemeActor(TRequest* request, const TString& topicPath)
            : TBase(request, topicPath)
        {}
        ~TUpdateSchemeActor() = default;

        void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                const TString& workingDir, const TString& name)
        {
            Y_UNUSED(name);
            const auto& response = DescribeSchemeResult->Get()->Request.Get()->ResultSet.front();
            NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
            modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
            modifyScheme.SetWorkingDir(workingDir);

            auto* config = modifyScheme.MutableAlterPersQueueGroup();
            Y_VERIFY(response.Self);
            Y_VERIFY(response.PQGroupInfo);
            config->CopyFrom(response.PQGroupInfo->Description);

            {
                auto applyIf = modifyScheme.AddApplyIf();
                applyIf->SetPathId(response.Self->Info.GetPathId());
                applyIf->SetPathVersion(response.Self->Info.GetPathVersion());
            }

            static_cast<TDerived*>(this)->ModifyPersqueueConfig(
                ctx,
                *config,
                response.PQGroupInfo->Description,
                response.Self->Info
            );

            this->DescribeSchemeResult.Reset();
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_VERIFY(result->ResultSet.size() == 1);
            DescribeSchemeResult = std::move(ev);
            return this->SendProposeRequest(ctx);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            switch (ev->GetTypeRewrite()) {
            default: TBase::StateWork(ev, ctx);
            }
        }

    private:
        THolder<NActors::TEventHandle<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> DescribeSchemeResult;
    };

}

#pragma once

#include <ydb/core/grpc_services/rpc_scheme_base.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/hfunc.h>


struct TMsgPqCodes {
    TString Message;
    Ydb::PersQueue::ErrorCode::ErrorCode PQCode;

    TMsgPqCodes(TString const& message, Ydb::PersQueue::ErrorCode::ErrorCode pqCode)
    : Message(message), PQCode(pqCode) {}
};

struct TYdbPqCodes {
    Ydb::StatusIds::StatusCode YdbCode;
    Ydb::PersQueue::ErrorCode::ErrorCode PQCode;

    TYdbPqCodes(Ydb::StatusIds::StatusCode YdbCode, Ydb::PersQueue::ErrorCode::ErrorCode PQCode)
    : YdbCode(YdbCode),
        PQCode(PQCode) {}
};


namespace NKikimr::NGRpcProxy::V1 {

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
        const TString& name,
        const Ydb::PersQueue::V1::TopicSettings& settings,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        const TActorContext& ctx,
        bool alter,
        TString& error,
        const TString& path,
        const TString& database = TString(),
        const TString& localDc = TString()
    );

    TYdbPqCodes FillProposeRequestImpl(
        const TString& name,
        const Ydb::Topic::CreateTopicRequest& request,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        const TActorContext& ctx,
        TString& error,
        const TString& path,
        const TString& database = TString(),
        const TString& localDc = TString()
    );

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
        const Ydb::Topic::AlterTopicRequest& request,
        NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr,
        const TActorContext& ctx,
        TString& error,
        bool isCdcStream
    );


    struct TClientServiceType {
        TString Name;
        ui32 MaxCount;
        TVector<TString> PasswordHashes;
    };
    typedef std::map<TString, TClientServiceType> TClientServiceTypes;
    TClientServiceTypes GetSupportedClientServiceTypes(const TActorContext& ctx);

    // Returns true if have duplicated read rules
    Ydb::StatusIds::StatusCode CheckConfig(const NKikimrPQ::TPQTabletConfig& config, const TClientServiceTypes& supportedReadRuleServiceTypes,
                                            TString& error, const TActorContext& ctx,
                                            const Ydb::StatusIds::StatusCode dubsStatus = Ydb::StatusIds::BAD_REQUEST);

    TMsgPqCodes AddReadRuleToConfig(
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
    NYql::TIssue FillIssue(const TString &errorReason, const size_t errorCode);

    template <typename T>
    class THasCdcStreamCompatibility {
        template <typename U> static constexpr std::false_type Detect(...);
        template <typename U> static constexpr auto Detect(U*)
            -> typename std::is_same<bool, decltype(std::declval<U>().IsCdcStreamCompatible())>::type;
    public:
        static constexpr bool Value = decltype(Detect<T>(0))::value;
    };

    struct TCdcStreamCompatible {
        bool IsCdcStreamCompatible() const {
            return true;
        }
    };

    template<class TDerived, class TRequest>
    class TPQGrpcSchemaBase : public NKikimr::NGRpcService::TRpcSchemeRequestActor<TDerived, TRequest> {
    protected:
        using TBase = NKikimr::NGRpcService::TRpcSchemeRequestActor<TDerived, TRequest>;

        using TProtoRequest = typename TRequest::TRequest;

    public:

        TPQGrpcSchemaBase(NGRpcService::IRequestOpCtx *request, const TString& topicPath)
            : TBase(request)
            , TopicPath(topicPath)
        {
        }

        TString GetTopicPath(const NActors::TActorContext& ctx) {
            auto path = NPersQueue::GetFullTopicPath(ctx, this->Request_->GetDatabaseName(), TopicPath);
            if (PrivateTopicName) {
                path = JoinPath(ChildPath(NKikimr::SplitPath(path), *PrivateTopicName));
            }
            return path;
        }

        const TMaybe<TString>& GetCdcStreamName() const {
            return CdcStreamName;
        }

    protected:
        // TDerived must implement FillProposeRequest(TEvProposeTransaction&, const TActorContext& ctx, TString workingDir, TString name);
        void SendProposeRequest(const NActors::TActorContext &ctx) {
            std::pair <TString, TString> pathPair;
            try {
                pathPair = NKikimr::NGRpcService::SplitPath(GetTopicPath(ctx));
            } catch (const std::exception &ex) {
                this->Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
                return this->ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
            }

            const auto &workingDir = pathPair.first;
            const auto &name = pathPair.second;

            std::unique_ptr <TEvTxUserProxy::TEvProposeTransaction> proposal(
                    new TEvTxUserProxy::TEvProposeTransaction());

            SetDatabase(proposal.get(), *this->Request_);

            if (this->Request_->GetSerializedToken().empty()) {
                if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                    return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                          "Unauthenticated access is forbidden, please provide credentials", ctx);
                }
            } else {
                proposal->Record.SetUserToken(this->Request_->GetSerializedToken());
            }

            static_cast<TDerived*>(this)->FillProposeRequest(*proposal, ctx, workingDir, name);

            if (!IsDead) {
                ctx.Send(MakeTxProxyID(), proposal.release());
            }
        }

        void SendDescribeProposeRequest(const NActors::TActorContext& ctx, bool showPrivate = false) {
            auto navigateRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
            navigateRequest->DatabaseName = CanonizePath(this->Request_->GetDatabaseName().GetOrElse(""));

            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = NKikimr::SplitPath(GetTopicPath(ctx));
            entry.SyncVersion = true;
            entry.ShowPrivatePath = showPrivate || PrivateTopicName.Defined();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            navigateRequest->ResultSet.emplace_back(entry);

            if (this->Request_->GetSerializedToken().empty()) {
                if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                    return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                          "Unauthenticated access is forbidden, please provide credentials", ctx);
                }
            } else {
                navigateRequest->UserToken = new NACLib::TUserToken(this->Request_->GetSerializedToken());
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
                              Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
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
                if (response.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                    if constexpr (THasCdcStreamCompatibility<TDerived>::Value) {
                        if (static_cast<TDerived*>(this)->IsCdcStreamCompatible()) {
                            Y_VERIFY(response.ListNodeEntry->Children.size() == 1);
                            PrivateTopicName = response.ListNodeEntry->Children.at(0).Name;

                            if (response.Self) {
                                CdcStreamName = response.Self->Info.GetName();
                            }

                            return SendDescribeProposeRequest(ctx);
                        }
                    }

                    this->Request_->RaiseIssue(
                        FillIssue(
                            TStringBuilder() << "path '" << path << "' is not compatible scheme object",
                            Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                        )
                    );
                    return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                } else if (!response.PQGroupInfo) {
                    this->Request_->RaiseIssue(
                        FillIssue(
                            TStringBuilder() << "path '" << path << "' creation is not completed",
                            Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                        )
                    );
                    return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
                }
                return static_cast<TDerived*>(this)->HandleCacheNavigateResponse(ev, ctx);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "path '" << path << "' does not exist or you " <<
                        "do not have access rights",
                        Ydb::PersQueue::ErrorCode::ACCESS_DENIED
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "table creation is not completed",
                        Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "path '" << path << "' is not a table",
                        Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                    )
                );
                return TBase::Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown: {
                this->Request_->RaiseIssue(
                    FillIssue(
                        TStringBuilder() << "unknown database root",
                        Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
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

        void ReplyWithError(Ydb::StatusIds::StatusCode status, size_t additionalStatus,
                            const TString& messageText, const NActors::TActorContext& ctx) {
            this->Request_->RaiseIssue(FillIssue(messageText, additionalStatus));
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
        const TString TopicPath;
        TMaybe<TString> PrivateTopicName;
        TMaybe<TString> CdcStreamName;
    };

    //-----------------------------------------------------------------------------------

    template<class TDerived, class TRequest>
    class TUpdateSchemeActor : public TPQGrpcSchemaBase<TDerived, TRequest> {
        using TBase = TPQGrpcSchemaBase<TDerived, TRequest>;

    public:
        TUpdateSchemeActor(NGRpcService::IRequestOpCtx* request, const TString& topicPath)
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

            if constexpr (THasCdcStreamCompatibility<TDerived>::Value) {
                modifyScheme.SetAllowAccessToPrivatePaths(static_cast<TDerived*>(this)->IsCdcStreamCompatible());
            }

            auto* config = modifyScheme.MutableAlterPersQueueGroup();
            Y_VERIFY(response.Self);
            Y_VERIFY(response.PQGroupInfo);
            config->CopyFrom(response.PQGroupInfo->Description);

            // keep previous values or set in ModifyPersqueueConfig
            config->ClearTotalGroupCount();
            config->MutablePQTabletConfig()->ClearPartitionKeySchema();

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

        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
            auto msg = ev->Get();
            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(ev->Get()->Record.GetStatus());

            if (status ==  TEvTxUserProxy::TResultStatus::ExecError && msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPreconditionFailed)
            {
                return TBase::ReplyWithError(Ydb::StatusIds::OVERLOADED,
                                                         Ydb::PersQueue::ErrorCode::ERROR,
                                                         TStringBuilder() << "Topic with name " << TBase::GetTopicPath(ctx) << " has another alter in progress",
                                                         ctx);
            }

            return TBase::TBase::Handle(ev, ctx);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
                default: TBase::StateWork(ev, ctx);
            }
        }

    private:
        THolder<NActors::TEventHandle<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> DescribeSchemeResult;
    };

}

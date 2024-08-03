#pragma once

#include <ydb/core/grpc_services/rpc_scheme_base.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>


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
        TAppData* appData,
        TString& error,
        const TString& path,
        const TString& database = TString(),
        const TString& localDc = TString()
    );

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
        const Ydb::Topic::AlterTopicRequest& request,
        NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr,
        TAppData* appData,
        TString& error,
        bool isCdcStream
    );


    struct TClientServiceType {
        TString Name;
        ui32 MaxCount;
        TVector<TString> PasswordHashes;
    };
    typedef std::map<TString, TClientServiceType> TClientServiceTypes;
    TClientServiceTypes GetSupportedClientServiceTypes(const NKikimrPQ::TPQConfig& pqConfig);

    // Returns true if have duplicated read rules
    Ydb::StatusIds::StatusCode CheckConfig(const NKikimrPQ::TPQTabletConfig& config, const TClientServiceTypes& supportedReadRuleServiceTypes,
                                            TString& error, const NKikimrPQ::TPQConfig& pqConfig,
                                            const Ydb::StatusIds::StatusCode dubsStatus = Ydb::StatusIds::BAD_REQUEST);

    TMsgPqCodes AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig *config,
        const Ydb::PersQueue::V1::TopicSettings::ReadRule& rr,
        const TClientServiceTypes& supportedReadRuleServiceTypes,
        const NKikimrPQ::TPQConfig& pqConfig
    );
    TString RemoveReadRuleFromConfig(
        NKikimrPQ::TPQTabletConfig *config,
        const NKikimrPQ::TPQTabletConfig& originalConfig,
        const TString& consumerName,
        const NKikimrPQ::TPQConfig& pqConfig
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


    template<class TDerived>
    class TPQSchemaBase {
    public:

        TPQSchemaBase(const TString& topicPath, const TString& database)
            : TopicPath(topicPath)
            , Database(database)
        {
        }

    protected:
        virtual TString GetTopicPath() const = 0;
        virtual void RespondWithCode(Ydb::StatusIds::StatusCode status, bool notFound = false) = 0;
        virtual void AddIssue(const NYql::TIssue& issue) = 0;
        virtual bool SetRequestToken(NSchemeCache::TSchemeCacheNavigate* request) const = 0;

        virtual bool ProcessCdc(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
            Y_UNUSED(entry);
            return false;
        };

        void SendDescribeProposeRequest(const NActors::TActorContext& ctx, bool showPrivate) {
            auto navigateRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
            if (!SetRequestToken(navigateRequest.get())) {
                AddIssue(FillIssue("Unauthenticated access is forbidden, please provide credentials",
                                   Ydb::PersQueue::ErrorCode::ACCESS_DENIED));
                return RespondWithCode(Ydb::StatusIds::UNAUTHORIZED);
            }

            navigateRequest->DatabaseName = CanonizePath(Database);
            navigateRequest->ResultSet.emplace_back(NSchemeCache::TSchemeCacheNavigate::TEntry{
                .Path = NKikimr::SplitPath(GetTopicPath()),
                .Access = CheckAccessWithWriteTopicPermission ? NACLib::UpdateRow : NACLib::DescribeSchema,
                .Operation = NSchemeCache::TSchemeCacheNavigate::OpList,
                .ShowPrivatePath = showPrivate,
                .SyncVersion = true,
            });
            if (!IsDead) {
                ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigateRequest.release()));
            }
        }

        bool ReplyIfNotTopic(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            auto const& entries = ev->Get()->Request.Get()->ResultSet;
            Y_ABORT_UNLESS(entries.size() == 1);
            auto const& entry = entries.front();
            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindTopic) {
                return false;
            }
            AddIssue(FillIssue(TStringBuilder() << "path '" << JoinPath(entry.Path) << "' is not a topic",
                               Ydb::PersQueue::ErrorCode::VALIDATION_ERROR));
            RespondWithCode(Ydb::StatusIds::SCHEME_ERROR, true);
            return true;
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            auto const& entries = ev->Get()->Request.Get()->ResultSet;
            Y_ABORT_UNLESS(entries.size() == 1);
            const auto& response = entries.front();
            const TString path  = JoinPath(response.Path);

            switch (response.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok: {
                if (response.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
                    if (ProcessCdc(response)) {
                        return;
                    }

                    AddIssue(
                        FillIssue(
                            TStringBuilder() << "path '" << path << "' is not compatible scheme object",
                            Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                        )
                    );
                    return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
                } else if (!response.PQGroupInfo) {
                    AddIssue(
                        FillIssue(
                            TStringBuilder() << "path '" << path << "' creation is not completed",
                            Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                        )
                    );
                    return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
                }
                return static_cast<TDerived*>(this)->HandleCacheNavigateResponse(ev);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown: {
                AddIssue(
                    FillIssue(
                        TStringBuilder() << "path '" << path << "' does not exist or you " <<
                        "do not have access rights",
                        Ydb::PersQueue::ErrorCode::ACCESS_DENIED
                    )
                );
                return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR, true);
            }
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete: {
                AddIssue(
                    FillIssue(
                        TStringBuilder() << "table creation is not completed",
                        Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                    )
                );
                return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable: {
                AddIssue(
                    FillIssue(
                        TStringBuilder() << "path '" << path << "' is not a table",
                        Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                    )
                );
                return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
            }
            break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown: {
                AddIssue(
                    FillIssue(
                        TStringBuilder() << "unknown database root",
                        Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                    )
                );
                return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
            }
            break;

            default:
                return RespondWithCode(Ydb::StatusIds::GENERIC_ERROR);
            }
        }

    public:
        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                Y_ABORT();
            }
        }

    protected:
        bool IsDead = false;
        bool CheckAccessWithWriteTopicPermission = false;
        const TString TopicPath;
        const TString Database;
    };


    template<class TDerived, class TRequest>
    class TPQGrpcSchemaBase : public NKikimr::NGRpcService::TRpcSchemeRequestActor<TDerived, TRequest>,
                              public TPQSchemaBase<TPQGrpcSchemaBase<TDerived, TRequest>> {
    protected:
        using TBase = NKikimr::NGRpcService::TRpcSchemeRequestActor<TDerived, TRequest>;
        using TActorBase = TPQSchemaBase<TPQGrpcSchemaBase<TDerived, TRequest>>;

        using TProtoRequest = typename TRequest::TRequest;

    public:

        TPQGrpcSchemaBase(NGRpcService::IRequestOpCtx *request, const TString& topicPath)
            : TBase(request)
            , TActorBase(topicPath, this->Request_->GetDatabaseName().GetOrElse(""))
        {
        }
        TPQGrpcSchemaBase(NGRpcService::IRequestOpCtx* request)
            : TBase(request)
            , TActorBase(TBase::GetProtoRequest()->path(), this->Request_->GetDatabaseName().GetOrElse(""))
        {
        }

        TString GetTopicPath() const override {
            auto path = NPersQueue::GetFullTopicPath(this->ActorContext(), this->Request_->GetDatabaseName(), TActorBase::TopicPath);
            if (PrivateTopicName) {
                path = JoinPath(ChildPath(NKikimr::SplitPath(path), *PrivateTopicName));
            }
            return path;
        }

        const TMaybe<TString>& GetCdcStreamName() const {
            return CdcStreamName;
        }
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            return static_cast<TDerived*>(this)->HandleCacheNavigateResponse(ev);
        }


    protected:
        // TDerived must implement FillProposeRequest(TEvProposeTransaction&, const TActorContext& ctx, TString workingDir, TString name);
        void SendProposeRequest(const NActors::TActorContext& ctx) {
            std::pair <TString, TString> pathPair;
            try {
                pathPair = NKikimr::NGRpcService::SplitPath(GetTopicPath());
            } catch (const std::exception &ex) {
                this->Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
                return this->RespondWithCode(Ydb::StatusIds::BAD_REQUEST);
            }

            const auto& workingDir = pathPair.first;
            const auto& name = pathPair.second;

            std::unique_ptr <TEvTxUserProxy::TEvProposeTransaction> proposal(
                    new TEvTxUserProxy::TEvProposeTransaction());

            SetDatabase(proposal.get(), *this->Request_);

            if (this->Request_->GetSerializedToken().empty()) {
                if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                    return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                          "Unauthenticated access is forbidden, please provide credentials");
                }
            } else {
                proposal->Record.SetUserToken(this->Request_->GetSerializedToken());
            }

            static_cast<TDerived*>(this)->FillProposeRequest(*proposal, ctx, workingDir, name);

            if (!TActorBase::IsDead) {
                ctx.Send(MakeTxProxyID(), proposal.release());
            }
        }

        void SendDescribeProposeRequest(const NActors::TActorContext& ctx, bool showPrivate = false) {
            return TActorBase::SendDescribeProposeRequest(ctx, showPrivate || PrivateTopicName.Defined());
        }

        bool SetRequestToken(NSchemeCache::TSchemeCacheNavigate* request) const override {
            if (auto const& token = this->Request_->GetSerializedToken()) {
                request->UserToken = new NACLib::TUserToken(token);
                return true;
            }
            return !(AppData()->PQConfig.GetRequireCredentialsInNewProtocol());
        }

        bool ProcessCdc(const NSchemeCache::TSchemeCacheNavigate::TEntry& response) override {
            if constexpr (THasCdcStreamCompatibility<TDerived>::Value) {
                if (static_cast<TDerived*>(this)->IsCdcStreamCompatible()) {
                    Y_ABORT_UNLESS(response.ListNodeEntry->Children.size() == 1);
                    PrivateTopicName = response.ListNodeEntry->Children.at(0).Name;

                    if (response.Self) {
                        CdcStreamName = response.Self->Info.GetName();
                    }

                    SendDescribeProposeRequest(TBase::ActorContext());
                    return true;
                }
            }
            return false;
        }

        void AddIssue(const NYql::TIssue& issue) override {
            this->Request_->RaiseIssue(issue);
        }

        void ReplyWithError(Ydb::StatusIds::StatusCode status, size_t additionalStatus,
                            const TString& messageText) {
            if (TActorBase::IsDead)
                return;
            this->Request_->RaiseIssue(FillIssue(messageText, additionalStatus));
            this->Request_->ReplyWithYdbStatus(status);
            this->Die(this->ActorContext());
            TActorBase::IsDead = true;
        }

        void RespondWithCode(Ydb::StatusIds::StatusCode status, bool notFound = false) override {
            if (TActorBase::IsDead)
                return;
            this->Request_->ReplyWithYdbStatus(status);
            this->Die(this->ActorContext());
            TActorBase::IsDead = true;
            Y_UNUSED(notFound);
        }

        template<class TProtoResult>
        void ReplyWithResult(Ydb::StatusIds::StatusCode status, const TProtoResult& result, const NActors::TActorContext& ctx) {
            if (TActorBase::IsDead)
                return;
            this->Request_->SendResult(result, status);
            this->Die(ctx);
            TActorBase::IsDead = true;
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, TActorBase::Handle);
            default: TBase::StateWork(ev);
            }
        }

    private:
        TMaybe<TString> PrivateTopicName;
        TMaybe<TString> CdcStreamName;
    };

    //-----------------------------------------------------------------------------------

    template<class TDerived>
    class TUpdateSchemeActorBase {
    public:
        ~TUpdateSchemeActorBase() = default;

        virtual void ModifyPersqueueConfig(TAppData* appData,
            NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
            const NKikimrSchemeOp::TDirEntry& selfInfo
        ) = 0;

    protected:
        void FillModifyScheme(NKikimrSchemeOp::TModifyScheme& modifyScheme, const TActorContext& ctx,
                              const TString& workingDir, const TString& name)
        {
            Y_UNUSED(name);
            modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
            modifyScheme.SetWorkingDir(workingDir);

            if constexpr (THasCdcStreamCompatibility<TDerived>::Value) {
                modifyScheme.SetAllowAccessToPrivatePaths(static_cast<TDerived*>(this)->IsCdcStreamCompatible());
            }

            auto* config = modifyScheme.MutableAlterPersQueueGroup();
            const auto& response = DescribeSchemeResult->Get()->Request.Get()->ResultSet.front();

            Y_ABORT_UNLESS(response.Self);
            Y_ABORT_UNLESS(response.PQGroupInfo);
            config->CopyFrom(response.PQGroupInfo->Description);

            // keep previous values or set in ModifyPersqueueConfig
            config->ClearTotalGroupCount();
            config->MutablePQTabletConfig()->ClearPartitionKeySchema();

            {
                auto applyIf = modifyScheme.AddApplyIf();
                applyIf->SetPathId(response.Self->Info.GetPathId());
                applyIf->SetPathVersion(response.Self->Info.GetPathVersion());
            }

            ModifyPersqueueConfig(
                AppData(ctx),
                *config,
                response.PQGroupInfo->Description,
                response.Self->Info
            );
        }

        virtual void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            Y_ABORT_UNLESS(result->ResultSet.size() == 1);
            DescribeSchemeResult = std::move(ev);
        }
    protected:
        THolder<NActors::TEventHandle<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> DescribeSchemeResult;

    };


    template<class TDerived, class TRequest>
    class TUpdateSchemeActor : public TPQGrpcSchemaBase<TDerived, TRequest>,
                               public TUpdateSchemeActorBase<TUpdateSchemeActor<TDerived, TRequest>> {
        using TBase = TPQGrpcSchemaBase<TDerived, TRequest>;
        using TUpdateSchemeBase = TUpdateSchemeActorBase<TUpdateSchemeActor<TDerived, TRequest>>;

    public:
        TUpdateSchemeActor(NGRpcService::IRequestOpCtx* request, const TString& topicPath)
            : TBase(request, topicPath)
        {}
        TUpdateSchemeActor(NGRpcService::IRequestOpCtx* request)
            : TBase(request)
        {}
        ~TUpdateSchemeActor() = default;


        void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                const TString& workingDir, const TString& name)
        {
            NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
            this->FillModifyScheme(modifyScheme, ctx, workingDir, name);
            this->DescribeSchemeResult.Reset();
        }


        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) override {
            TUpdateSchemeBase::HandleCacheNavigateResponse(ev);
            return this->SendProposeRequest(this->ActorContext());
        }


        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
            auto msg = ev->Get();
            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(ev->Get()->Record.GetStatus());

            if (status == TEvTxUserProxy::TResultStatus::ExecError && msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPreconditionFailed)
            {
                return TBase::ReplyWithError(Ydb::StatusIds::OVERLOADED,
                                                         Ydb::PersQueue::ErrorCode::OVERLOAD,
                                                         TStringBuilder() << "Topic with name " << TBase::GetTopicPath() << " has another alter in progress");
            }

            return TBase::TBase::Handle(ev, ctx);
        }

        void StateWork(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
                default: TBase::StateWork(ev);
            }
        }
    };

    template<class TDerived, class TRequest, class TEvResponse>
    class TPQInternalSchemaActor : public TPQSchemaBase<TPQInternalSchemaActor<TDerived, TRequest, TEvResponse>>
                                 , public TActorBootstrapped<TPQInternalSchemaActor<TDerived, TRequest, TEvResponse>>
    {
    protected:
        using TBase = TPQSchemaBase<TPQInternalSchemaActor<TDerived, TRequest, TEvResponse>>;

    public:

        TPQInternalSchemaActor(const TRequest& request, const TActorId& requester, bool notExistsOk = false)
            : TBase(request.Topic, request.Database)
            , Request(request)
            , Requester(requester)
            , Response(MakeHolder<TEvResponse>())
        {
        }

        virtual void Bootstrap(const TActorContext&) = 0;
        virtual void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) = 0;

        TString GetTopicPath() const override {
            return TBase::TopicPath;
        }

        void SendDescribeProposeRequest() {
            return TBase::SendDescribeProposeRequest(this->ActorContext(), false);
        }

        bool HandleCacheNavigateResponseBase(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            Y_ABORT_UNLESS(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
            if (this->ReplyIfNotTopic(ev)) {
                return false;
            }

            auto& item = ev->Get()->Request->ResultSet[0];
            PQGroupInfo = item.PQGroupInfo;
            Self = item.Self;

            return true;
        }

        bool SetRequestToken(NSchemeCache::TSchemeCacheNavigate* request) const override {
            if (Request.Token.empty()) {
                return !(AppData()->PQConfig.GetRequireCredentialsInNewProtocol());
            } else {
                request->UserToken = new NACLib::TUserToken(Request.Token);
                return true;
            }
        }

        void AddIssue(const NYql::TIssue& issue) override {
            Response->Issues.AddIssue(issue);
        }

        void RespondWithCode(Ydb::StatusIds::StatusCode status, bool notFound = false) override {
            if (!RespondOverride(status, notFound)) {
                Response->Status = status;
                this->ActorContext().Send(Requester, Response.Release());
            }
            this->Die(this->ActorContext());
            TBase::IsDead = true;
        }

    protected:
        const TRequest& GetRequest() const {
            return Request;
        }

        virtual bool RespondOverride(Ydb::StatusIds::StatusCode status, bool notFound) {
            Y_UNUSED(status);
            Y_UNUSED(notFound);
            return false;
        }


    private:
        TRequest Request;
        TActorId Requester;
        TMaybe<TString> PrivateTopicName;

    protected:
        THolder<TEvResponse> Response;
        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TDirEntryInfo> Self;
    };

}

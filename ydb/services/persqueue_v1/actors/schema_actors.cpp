#include "schema_actors.h"

#include "persqueue_utils.h"

#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>

namespace NKikimr::NGRpcProxy::V1 {

constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";

TDropTopicActor::TDropTopicActor(NKikimr::NGRpcService::TEvDropTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}
TDropTopicActor::TDropTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request)
    : TBase(request)
{
}

void TDropTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TDropTopicActor::StateWork);
}

TPQDropTopicActor::TPQDropTopicActor(NKikimr::NGRpcService::TEvPQDropTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}

void TPQDropTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TPQDropTopicActor::StateWork);
}


void TDropPropose::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                         const TString& workingDir, const TString& name)
{
    Y_UNUSED(ctx);
    NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
    modifyScheme.SetWorkingDir(workingDir);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
    modifyScheme.MutableDrop()->SetName(name);
}

TPQDescribeTopicActor::TPQDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}

void TPQDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev, ctx);
    }
}


void TPQDescribeTopicActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev, ctx)) {
        return;
    }

    const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    const TString path = JoinSeq("/", response.Path);

    Ydb::PersQueue::V1::DescribeTopicResult result;

    Ydb::Scheme::Entry *selfEntry = result.mutable_self();
    ConvertDirectoryEntry(response.Self->Info, selfEntry, true);
    if (const auto& name = GetCdcStreamName()) {
        selfEntry->set_name(*name);
    }

    auto settings = result.mutable_settings();
    if (response.PQGroupInfo) {
        const auto &pqDescr = response.PQGroupInfo->Description;
        settings->set_partitions_count(pqDescr.GetTotalGroupCount());

        const auto &config = pqDescr.GetPQTabletConfig();
        if (!config.GetRequireAuthWrite()) {
            (*settings->mutable_attributes())["_allow_unauthenticated_write"] = "true";
        }

        if (!config.GetRequireAuthRead()) {
            (*settings->mutable_attributes())["_allow_unauthenticated_read"] = "true";
        }

        if (pqDescr.GetPartitionPerTablet() != 2) {
            (*settings->mutable_attributes())["_partitions_per_tablet"] =
                TStringBuilder() << pqDescr.GetPartitionPerTablet();
        }
        if (config.HasAbcId()) {
            (*settings->mutable_attributes())["_abc_id"] = TStringBuilder() << config.GetAbcId();
        }
        if (config.HasAbcSlug()) {
            (*settings->mutable_attributes())["_abc_slug"] = config.GetAbcSlug();
        }
        if (config.HasFederationAccount()) {
            (*settings->mutable_attributes())["_federation_account"] = config.GetFederationAccount();
        }
        bool local = config.GetLocalDC();
        settings->set_client_write_disabled(!local);
        const auto &partConfig = config.GetPartitionConfig();
        i64 msip = partConfig.GetMaxSizeInPartition();
        if (msip != Max<i64>())
            settings->set_max_partition_storage_size(msip);
        settings->set_retention_period_ms(partConfig.GetLifetimeSeconds() * 1000);
        if (partConfig.GetStorageLimitBytes() > 0)
            settings->set_retention_storage_bytes(partConfig.GetStorageLimitBytes());

        settings->set_message_group_seqno_retention_period_ms(partConfig.GetSourceIdLifetimeSeconds() * 1000);
        settings->set_max_partition_message_groups_seqno_stored(partConfig.GetSourceIdMaxCounts());

        if (local || AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            settings->set_max_partition_write_speed(partConfig.GetWriteSpeedInBytesPerSecond());
            settings->set_max_partition_write_burst(partConfig.GetBurstSize());
        }

        settings->set_supported_format(
                                       (Ydb::PersQueue::V1::TopicSettings::Format) (config.GetFormatVersion() + 1));

        for (const auto &codec : config.GetCodecs().GetIds()) {
            settings->add_supported_codecs((Ydb::PersQueue::V1::Codec) (codec + 1));
        }

        const auto& pqConfig = AppData(ctx)->PQConfig;
        for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
            auto rr = settings->add_read_rules();
            auto consumerName = NPersQueue::ConvertOldConsumerName(config.GetReadRules(i), ctx);
            rr->set_consumer_name(consumerName);
            rr->set_starting_message_timestamp_ms(config.GetReadFromTimestampsMs(i));
            rr->set_supported_format(
                                     (Ydb::PersQueue::V1::TopicSettings::Format) (config.GetConsumerFormatVersions(i) + 1));
            rr->set_version(config.GetReadRuleVersions(i));
            for (const auto &codec : config.GetConsumerCodecs(i).GetIds()) {
                rr->add_supported_codecs((Ydb::PersQueue::V1::Codec) (codec + 1));
            }
            bool important = false;
            for (const auto &c : partConfig.GetImportantClientId()) {
                if (c == config.GetReadRules(i)) {
                    important = true;
                    break;
                }
            }
            rr->set_important(important);

            if (i < config.ReadRuleServiceTypesSize()) {
                rr->set_service_type(config.GetReadRuleServiceTypes(i));
            } else {
                if (pqConfig.GetDisallowDefaultClientServiceType()) {
                    this->Request_->RaiseIssue(FillIssue(
                        "service type must be set for all read rules",
                        Ydb::PersQueue::ErrorCode::ERROR
                    ));
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, ctx);
                    return;
                }
                rr->set_service_type(pqConfig.GetDefaultClientServiceType().GetName());
            }
        }
        if (partConfig.HasMirrorFrom()) {
            auto rmr = settings->mutable_remote_mirror_rule();
            TStringBuilder endpoint;
            if (partConfig.GetMirrorFrom().GetUseSecureConnection()) {
                endpoint << GRPCS_ENDPOINT_PREFIX;
            }
            endpoint << partConfig.GetMirrorFrom().GetEndpoint() << ":"
                     << partConfig.GetMirrorFrom().GetEndpointPort();
            rmr->set_endpoint(endpoint);
            rmr->set_topic_path(partConfig.GetMirrorFrom().GetTopic());
            rmr->set_consumer_name(partConfig.GetMirrorFrom().GetConsumer());
            rmr->set_starting_message_timestamp_ms(partConfig.GetMirrorFrom().GetReadFromTimestampsMs());
            if (partConfig.GetMirrorFrom().HasCredentials()) {
                if (partConfig.GetMirrorFrom().GetCredentials().HasOauthToken()) {
                    rmr->mutable_credentials()->set_oauth_token(
                                                                NPersQueue::ObfuscateString(
                                                                                            partConfig.GetMirrorFrom().GetCredentials().GetOauthToken())
                                                                );
                } else if (partConfig.GetMirrorFrom().GetCredentials().HasJwtParams()) {
                    rmr->mutable_credentials()->set_jwt_params(
                                                               NPersQueue::ObfuscateString(
                                                                                           partConfig.GetMirrorFrom().GetCredentials().GetJwtParams())
                                                               );
                } else if (partConfig.GetMirrorFrom().GetCredentials().HasIam()) {
                    rmr->mutable_credentials()->mutable_iam()->set_endpoint(
                                                                            partConfig.GetMirrorFrom().GetCredentials().GetIam().GetEndpoint()
                                                                            );
                    rmr->mutable_credentials()->mutable_iam()->set_service_account_key(
                                                                                       NPersQueue::ObfuscateString(
                                                                                                                   partConfig.GetMirrorFrom().GetCredentials().GetIam().GetServiceAccountKey())
                                                                                       );
                }
            }
            rmr->set_database(partConfig.GetMirrorFrom().GetDatabase());
        }
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
}


void TPQDescribeTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);

    SendDescribeProposeRequest(ctx);
    Become(&TPQDescribeTopicActor::StateWork);
}


TAddReadRuleActor::TAddReadRuleActor(NKikimr::NGRpcService::TEvPQAddReadRuleRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}

void TAddReadRuleActor::Bootstrap(const NActors::TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    SendDescribeProposeRequest(ctx);
    Become(&TBase::StateWork);
}

void TAddReadRuleActor::ModifyPersqueueConfig(
    const TActorContext& ctx,
    NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
    const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
    const NKikimrSchemeOp::TDirEntry& selfInfo
) {
    Y_UNUSED(pqGroupDescription);

    auto* pqConfig = groupConfig.MutablePQTabletConfig();
    auto rule = GetProtoRequest()->read_rule();

    if (rule.version() == 0) {
        rule.set_version(selfInfo.GetVersion().GetPQVersion());
    }
    auto serviceTypes = GetSupportedClientServiceTypes(ctx);

    TString error;
    auto messageAndCode = AddReadRuleToConfig(pqConfig, rule, serviceTypes, ctx);
    auto status = messageAndCode.PQCode == Ydb::PersQueue::ErrorCode::OK ?
                                CheckConfig(*pqConfig, serviceTypes, messageAndCode.Message, ctx, Ydb::StatusIds::ALREADY_EXISTS)
                                : Ydb::StatusIds::BAD_REQUEST;
    if (status != Ydb::StatusIds::SUCCESS) {
        return ReplyWithError(status,
                              status == Ydb::StatusIds::ALREADY_EXISTS ? Ydb::PersQueue::ErrorCode::OK
                                                                       : Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                              messageAndCode.Message, ctx);
    }
}

TRemoveReadRuleActor::TRemoveReadRuleActor(NKikimr::NGRpcService::TEvPQRemoveReadRuleRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
    Y_ASSERT(request);
}

void TRemoveReadRuleActor::Bootstrap(const NActors::TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    SendDescribeProposeRequest(ctx);
    Become(&TBase::StateWork);
}

void TRemoveReadRuleActor::ModifyPersqueueConfig(
    const TActorContext& ctx,
    NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
    const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
    const NKikimrSchemeOp::TDirEntry& selfInfo
) {
    Y_UNUSED(selfInfo);

    auto error = RemoveReadRuleFromConfig(
        groupConfig.MutablePQTabletConfig(),
        pqGroupDescription.GetPQTabletConfig(),
        GetProtoRequest()->consumer_name(),
        ctx
    );
    if (!error.Empty()) {
        return ReplyWithError(Ydb::StatusIds::NOT_FOUND, Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
    }
}

TPQCreateTopicActor::TPQCreateTopicActor(NKikimr::NGRpcService::TEvPQCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters)
    : TBase(request, request->GetProtoRequest()->path())
    , LocalCluster(localCluster)
    , Clusters(clusters)
{
    Y_ASSERT(request);
}

void TPQCreateTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TPQCreateTopicActor::StateWork);
}

TCreateTopicActor::TCreateTopicActor(NKikimr::NGRpcService::TEvCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters)
    : TBase(request, request->GetProtoRequest()->path())
    , LocalCluster(localCluster)
    , Clusters(clusters)
{
    Y_ASSERT(request);
}

TCreateTopicActor::TCreateTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request)
    : TBase(request)
{
    Y_ASSERT(request);
}

void TCreateTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TCreateTopicActor::StateWork);
}



TPQAlterTopicActor::TPQAlterTopicActor(NKikimr::NGRpcService::TEvPQAlterTopicRequest* request, const TString& localCluster)
    : TBase(request, request->GetProtoRequest()->path())
    , LocalCluster(localCluster)
{
    Y_ASSERT(request);
}

void TPQAlterTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TPQAlterTopicActor::StateWork);
}

void TPQCreateTopicActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                            const TString& workingDir, const TString& name)
{
    NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
    modifyScheme.SetWorkingDir(workingDir);

    {
        TString error;
        auto status = FillProposeRequestImpl(name, GetProtoRequest()->settings(), modifyScheme, ctx, false, error,
                                             workingDir, proposal.Record.GetDatabaseName(), LocalCluster);
        if (!error.empty()) {
            Request_->RaiseIssue(FillIssue(error, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
            return ReplyWithResult(status, ctx);
        }
    }

    const auto& pqDescr = modifyScheme.GetCreatePersQueueGroup();
    const auto& config = pqDescr.GetPQTabletConfig();
    if (!LocalCluster.empty() && config.GetLocalDC() && config.GetDC() != LocalCluster) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Local cluster is not correct - provided '" << config.GetDC()
                                    << "' instead of " << LocalCluster, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
    }
    if (Count(Clusters, config.GetDC()) == 0 && !Clusters.empty()) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Unknown cluster '" << config.GetDC() << "'", Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
    }
}



void TCreateTopicActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                            const TString& workingDir, const TString& name)
{
    NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
    modifyScheme.SetWorkingDir(workingDir);

    {
        TString error;

        auto status = FillProposeRequestImpl(name, *GetProtoRequest(), modifyScheme, ctx, error,
                                             workingDir, proposal.Record.GetDatabaseName(), LocalCluster).YdbCode;

        if (!error.empty()) {
            Request_->RaiseIssue(FillIssue(error, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
            return ReplyWithResult(status, ctx);
        }
    }

    const auto& pqDescr = modifyScheme.GetCreatePersQueueGroup();
    const auto& config = pqDescr.GetPQTabletConfig();

    if (!LocalCluster.empty() && config.GetLocalDC() && config.GetDC() != LocalCluster) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Local cluster is not correct - provided '" << config.GetDC()
                                    << "' instead of " << LocalCluster, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
    }
    if (Count(Clusters, config.GetDC()) == 0 && !Clusters.empty()) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Unknown cluster '" << config.GetDC() << "'", Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
    }
}



void TPQAlterTopicActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                            const TString& workingDir, const TString& name) {
    NKikimrSchemeOp::TModifyScheme &modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
    modifyScheme.SetWorkingDir(workingDir);
    TString error;
    auto status = FillProposeRequestImpl(name, GetProtoRequest()->settings(), modifyScheme, ctx, true, error, workingDir,
                                         proposal.Record.GetDatabaseName(), LocalCluster);
    if (!error.empty()) {
        Request_->RaiseIssue(FillIssue(error, Ydb::PersQueue::ErrorCode::BAD_REQUEST));

        return ReplyWithResult(status, ctx);
    }
}


TAlterTopicActor::TAlterTopicActor(NKikimr::NGRpcService::TEvAlterTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}

TAlterTopicActor::TAlterTopicActor(NKikimr::NGRpcService::IRequestOpCtx* request)
    : TBase(request)
{
}

void TAlterTopicActor::Bootstrap(const NActors::TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    SendDescribeProposeRequest(ctx);
    Become(&TBase::StateWork);
}

void TAlterTopicActor::ModifyPersqueueConfig(
    const TActorContext& ctx,
    NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
    const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
    const NKikimrSchemeOp::TDirEntry& selfInfo
) {
    Y_UNUSED(pqGroupDescription);
    Y_UNUSED(selfInfo);
    TString error;
    Y_UNUSED(selfInfo);

    auto status = FillProposeRequestImpl(*GetProtoRequest(), groupConfig, ctx, error, GetCdcStreamName().Defined());
    if (!error.empty()) {
        Request_->RaiseIssue(FillIssue(error, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return ReplyWithResult(status, ctx);
    }
}


TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::TEvDescribeTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
    , TDescribeTopicActorImpl("")
{
}

TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::IRequestOpCtx * ctx)
    : TBase(ctx, dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->path())
    , TDescribeTopicActorImpl("")
{
}



TDescribeConsumerActor::TDescribeConsumerActor(NKikimr::NGRpcService::TEvDescribeConsumerRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
    , TDescribeTopicActorImpl(request->GetProtoRequest()->consumer())
{
}

TDescribeConsumerActor::TDescribeConsumerActor(NKikimr::NGRpcService::IRequestOpCtx * ctx)
    : TBase(ctx, dynamic_cast<const Ydb::Topic::DescribeConsumerRequest*>(ctx->GetRequest())->path())
    , TDescribeTopicActorImpl(dynamic_cast<const Ydb::Topic::DescribeConsumerRequest*>(ctx->GetRequest())->consumer())
{
}


TDescribeTopicActorImpl::TDescribeTopicActorImpl(const TString& consumer)
    : Consumer(consumer)
{
}


bool TDescribeTopicActorImpl::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(NKikimr::TEvPersQueue::TEvStatusResponse, Handle);
        HFunc(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse, Handle);
        default: return false;
    }
    return true;
}

void TDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    if (!TDescribeTopicActorImpl::StateWork(ev, ctx)) {
        TBase::StateWork(ev, ctx);
    }
}

void TDescribeConsumerActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    if (!TDescribeTopicActorImpl::StateWork(ev, ctx)) {
        TBase::StateWork(ev, ctx);
    }
}


void TDescribeTopicActorImpl::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        RestartTablet(ev->Get()->TabletId, ctx, ev->Sender);
    } else {
        auto it = Tablets.find(ev->Get()->TabletId);
        if (it == Tablets.end()) return;
        it->second.NodeId = ev->Get()->ServerId.NodeId();
    }
}

void TDescribeTopicActorImpl::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    RestartTablet(ev->Get()->TabletId, ctx, ev->Sender);
}

void TDescribeTopicActor::RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
    this->Request_->RaiseIssue(FillIssue(error, errorCode));
    TBase::Reply(status, ctx);
}

void TDescribeConsumerActor::RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
    this->Request_->RaiseIssue(FillIssue(error, errorCode));
    TBase::Reply(status, ctx);
}


void TDescribeTopicActorImpl::RestartTablet(ui64 tabletId, const TActorContext& ctx, TActorId pipe, const TDuration& delay) {
    auto it = Tablets.find(tabletId);
    if (it == Tablets.end()) return;
    if (pipe && pipe != it->second.Pipe) return;
    if (--it->second.RetriesLeft == 0) {
        return RaiseError(TStringBuilder() << "Tablet " << tabletId << " unresponsible", Ydb::PersQueue::ErrorCode::ERROR, Ydb::StatusIds::INTERNAL_ERROR, ctx);
    }
    Y_VERIFY(RequestsInfly > 0);
    --RequestsInfly;
    if (delay == TDuration::Zero()) {
        RequestTablet(it->second, ctx);
    } else {
        ++RequestsInfly;
        ctx.Schedule(delay, new TEvPQProxy::TEvRequestTablet(tabletId));
    }
}

void TDescribeTopicActorImpl::Handle(TEvPQProxy::TEvRequestTablet::TPtr& ev, const TActorContext& ctx) {
    --RequestsInfly;
    auto it = Tablets.find(ev->Get()->TabletId);
    if (it == Tablets.end()) return;
    RequestTablet(it->second, ctx);
}

void TDescribeTopicActorImpl::RequestTablet(TTabletInfo& tablet, const TActorContext& ctx) {
    tablet.Pipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tablet.TabletId, NTabletPipe::TClientConfig(NTabletPipe::TClientRetryPolicy::WithRetries())));

    if (tablet.TabletId == BalancerTabletId) {
        THolder<NKikimr::TEvPersQueue::TEvGetReadSessionsInfo> ev(new NKikimr::TEvPersQueue::TEvGetReadSessionsInfo(Consumer));
        NTabletPipe::SendData(ctx, tablet.Pipe, ev.Release());

    } else {
        THolder<NKikimr::TEvPersQueue::TEvStatus> ev(new NKikimr::TEvPersQueue::TEvStatus(Consumer.empty() ? "" : NPersQueue::ConvertNewConsumerName(Consumer), Consumer.empty()));
        NTabletPipe::SendData(ctx, tablet.Pipe, ev.Release());
    }
    ++RequestsInfly;
}

void TDescribeTopicActorImpl::Handle(NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
    auto it = Tablets.find(ev->Get()->Record.GetTabletId());
    if (it == Tablets.end()) return;
    --RequestsInfly;
    NTabletPipe::CloseClient(ctx, it->second.Pipe);
    it->second.Pipe = TActorId{};

    auto& record = ev->Get()->Record;
    for (auto& partResult : record.GetPartResult()) {
        if (partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_INITIALIZING ||
            partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_UNKNOWN) {
            RestartTablet(record.GetTabletId(), ctx, {}, TDuration::MilliSeconds(100));
            return;
        }
    }

    ApplyResponse(it->second, ev, ctx);

    if (RequestsInfly == 0) {
        RequestAdditionalInfo(ctx);
        if (RequestsInfly == 0) {
            Reply(ctx);
        }
    }
}


void TDescribeTopicActorImpl::Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    if (BalancerTabletId == 0)
        return;
    auto it = Tablets.find(BalancerTabletId);
    Y_VERIFY(it != Tablets.end());
    --RequestsInfly;
    NTabletPipe::CloseClient(ctx, it->second.Pipe);
    it->second.Pipe = TActorId{};
    BalancerTabletId = 0;

    ApplyResponse(it->second, ev, ctx);

    if (RequestsInfly == 0) {
        RequestAdditionalInfo(ctx);
        if (RequestsInfly == 0) {
            Reply(ctx);
        }
    }
}


void TDescribeTopicActorImpl::RequestAdditionalInfo(const TActorContext& ctx) {
    if (BalancerTabletId) {
        RequestTablet(BalancerTabletId, ctx);
    }
}

void TDescribeTopicActorImpl::RequestTablet(ui64 tabletId, const TActorContext& ctx) {
    auto it = Tablets.find(tabletId);
    if (it != Tablets.end()) {
        RequestTablet(it->second, ctx);
    }
}


template<class T>
void SetProtoTime(T* proto, const ui64 ms) {
    proto->set_seconds(ms / 1000);
    proto->set_nanos((ms % 1000) * 1'000'000);
}

template<class T>
void UpdateProtoTime(T* proto, const ui64 ms, bool storeMin) {
    ui64 storedMs = proto->seconds() * 1000 + proto->nanos() / 1'000'000;
    if ((ms < storedMs) == storeMin) {
        SetProtoTime(proto, ms);
    }
}


void TDescribeTopicActor::ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    Y_UNUSED(tabletInfo);
    Y_UNUSED(ev);
    Y_FAIL("");
}


void AddWindowsStat(Ydb::Topic::MultipleWindowsStat *stat, ui64 perMin, ui64 perHour, ui64 perDay) {
    stat->set_per_minute(stat->per_minute() + perMin);
    stat->set_per_hour(stat->per_hour() + perHour);
    stat->set_per_day(stat->per_day() + perDay);
}

void TDescribeTopicActor::ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);

    auto& record = ev->Get()->Record;

    std::map<ui32, NKikimrPQ::TStatusResponse::TPartResult> res;

    auto topicStats = Result.mutable_topic_stats();

    if (record.PartResultSize() > 0) { // init with first value

        SetProtoTime(topicStats->mutable_min_last_write_time(), record.GetPartResult(0).GetLastWriteTimestampMs());
        SetProtoTime(topicStats->mutable_max_write_time_lag(), record.GetPartResult(0).GetWriteLagMs());
    }

    std::map<TString, Ydb::Topic::Consumer*> consumersInfo;
    for (auto& consumer : *Result.mutable_consumers()) {
        consumersInfo[NPersQueue::ConvertNewConsumerName(consumer.name(), ctx)] = &consumer;
    }

    for (auto& partResult : record.GetPartResult()) {
        res[partResult.GetPartition()] = partResult;

        topicStats->set_store_size_bytes(topicStats->store_size_bytes() + partResult.GetPartitionSize());

        UpdateProtoTime(topicStats->mutable_min_last_write_time(), partResult.GetLastWriteTimestampMs(), true);
        UpdateProtoTime(topicStats->mutable_max_write_time_lag(), partResult.GetWriteLagMs(), false);

        AddWindowsStat(topicStats->mutable_bytes_written(), partResult.GetAvgWriteSpeedPerMin(), partResult.GetAvgWriteSpeedPerHour(), partResult.GetAvgWriteSpeedPerDay());


        for (auto& cons : partResult.GetConsumerResult()) {
            auto it = consumersInfo.find(cons.GetConsumer());
            if (it == consumersInfo.end()) continue;

            if (!it->second->has_consumer_stats()) {
                auto* stats = it->second->mutable_consumer_stats();

                SetProtoTime(stats->mutable_min_partitions_last_read_time(), cons.GetLastReadTimestampMs());
                SetProtoTime(stats->mutable_max_read_time_lag(), cons.GetReadLagMs());
                SetProtoTime(stats->mutable_max_write_time_lag(), cons.GetWriteLagMs());
            } else {
                auto* stats = it->second->mutable_consumer_stats();

                UpdateProtoTime(stats->mutable_min_partitions_last_read_time(), cons.GetLastReadTimestampMs(), true);
                UpdateProtoTime(stats->mutable_max_read_time_lag(), cons.GetReadLagMs(), false);
                UpdateProtoTime(stats->mutable_max_write_time_lag(), cons.GetWriteLagMs(), false);
            }

            AddWindowsStat(it->second->mutable_consumer_stats()->mutable_bytes_read(), cons.GetAvgReadSpeedPerMin(), cons.GetAvgReadSpeedPerHour(), cons.GetAvgReadSpeedPerDay());
        }
    }

    for (auto& partRes : *(Result.mutable_partitions())) {
        auto it = res.find(partRes.partition_id());
        if (it == res.end()) continue;

        const auto& partResult = it->second;
        auto partStats = partRes.mutable_partition_stats();

        partStats->set_store_size_bytes(partResult.GetPartitionSize());
        partStats->mutable_partition_offsets()->set_start(partResult.GetStartOffset());
        partStats->mutable_partition_offsets()->set_end(partResult.GetEndOffset());

        SetProtoTime(partStats->mutable_last_write_time(), partResult.GetLastWriteTimestampMs());
        SetProtoTime(partStats->mutable_max_write_time_lag(), partResult.GetWriteLagMs());

        AddWindowsStat(partStats->mutable_bytes_written(), partResult.GetAvgWriteSpeedPerMin(), partResult.GetAvgWriteSpeedPerHour(), partResult.GetAvgWriteSpeedPerDay());

        partStats->set_partition_node_id(tabletInfo.NodeId);
    }
}


void TDescribeTopicActor::Reply(const TActorContext& ctx) {
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}

void TDescribeConsumerActor::Reply(const TActorContext& ctx) {
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}


void TDescribeConsumerActor::ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    Y_UNUSED(tabletInfo);

    std::map<ui32, NKikimrPQ::TReadSessionsInfoResponse::TPartitionInfo> res;

    for (const auto& partInfo : ev->Get()->Record.GetPartitionInfo()) {
        res[partInfo.GetPartition()] = partInfo;
    }
    for (auto& partRes : *(Result.mutable_partitions())) {
        auto it = res.find(partRes.partition_id());
        if (it == res.end()) continue;
        auto consRes = partRes.mutable_partition_consumer_stats();
        consRes->set_read_session_id(it->second.GetSession());
        SetProtoTime(consRes->mutable_partition_read_session_create_time(), it->second.GetTimestampMs());
        consRes->set_connection_node_id(it->second.GetProxyNodeId());
        consRes->set_reader_name(it->second.GetClientNode());
    }
}


void TDescribeConsumerActor::ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    Y_UNUSED(tabletInfo);

    auto& record = ev->Get()->Record;

    std::map<ui32, NKikimrPQ::TStatusResponse::TPartResult> res;

    for (auto& partResult : record.GetPartResult()) {
        res[partResult.GetPartition()] = partResult;
    }

    for (auto& partRes : *(Result.mutable_partitions())) {
        auto it = res.find(partRes.partition_id());
        if (it == res.end()) continue;

        const auto& partResult = it->second;
        auto partStats = partRes.mutable_partition_stats();

        partStats->set_store_size_bytes(partResult.GetPartitionSize());
        partStats->mutable_partition_offsets()->set_start(partResult.GetStartOffset());
        partStats->mutable_partition_offsets()->set_end(partResult.GetEndOffset());

        SetProtoTime(partStats->mutable_last_write_time(), partResult.GetLastWriteTimestampMs());
        SetProtoTime(partStats->mutable_max_write_time_lag(), partResult.GetWriteLagMs());


        AddWindowsStat(partStats->mutable_bytes_written(), partResult.GetAvgWriteSpeedPerMin(), partResult.GetAvgWriteSpeedPerHour(), partResult.GetAvgWriteSpeedPerDay());

        partStats->set_partition_node_id(tabletInfo.NodeId);

        if (Consumer) {
            auto consStats = partRes.mutable_partition_consumer_stats();

            consStats->set_last_read_offset(partResult.GetLagsInfo().GetReadPosition().GetOffset());
            consStats->set_committed_offset(partResult.GetLagsInfo().GetWritePosition().GetOffset());

            SetProtoTime(consStats->mutable_last_read_time(), partResult.GetLagsInfo().GetLastReadTimestampMs());
            SetProtoTime(consStats->mutable_max_read_time_lag(), partResult.GetLagsInfo().GetReadLagMs());
            SetProtoTime(consStats->mutable_max_write_time_lag(), partResult.GetLagsInfo().GetWriteLagMs());

            AddWindowsStat(consStats->mutable_bytes_read(), partResult.GetAvgReadSpeedPerMin(), partResult.GetAvgReadSpeedPerHour(), partResult.GetAvgReadSpeedPerDay());

            if (!Result.consumer().has_consumer_stats()) {
                auto* stats = Result.mutable_consumer()->mutable_consumer_stats();

                SetProtoTime(stats->mutable_min_partitions_last_read_time(), partResult.GetLagsInfo().GetLastReadTimestampMs());
                SetProtoTime(stats->mutable_max_read_time_lag(), partResult.GetLagsInfo().GetReadLagMs());
                SetProtoTime(stats->mutable_max_write_time_lag(), partResult.GetLagsInfo().GetWriteLagMs());

                AddWindowsStat(consStats->mutable_bytes_read(), partResult.GetAvgReadSpeedPerMin(), partResult.GetAvgReadSpeedPerHour(), partResult.GetAvgReadSpeedPerDay());
            } else {
                auto* stats = Result.mutable_consumer()->mutable_consumer_stats();

                UpdateProtoTime(stats->mutable_min_partitions_last_read_time(), partResult.GetLagsInfo().GetLastReadTimestampMs(), true);
                UpdateProtoTime(stats->mutable_max_read_time_lag(), partResult.GetLagsInfo().GetReadLagMs(), false);
                UpdateProtoTime(stats->mutable_max_write_time_lag(), partResult.GetLagsInfo().GetWriteLagMs(), false);

                AddWindowsStat(consStats->mutable_bytes_read(), partResult.GetAvgReadSpeedPerMin(), partResult.GetAvgReadSpeedPerHour(), partResult.GetAvgReadSpeedPerDay());
            }
        }
    }
}



bool FillConsumerProto(Ydb::Topic::Consumer *rr, const NKikimrPQ::TPQTabletConfig& config, ui32 i,
                        const NActors::TActorContext& ctx, Ydb::StatusIds::StatusCode& status, TString& error)
{
    const auto &partConfig = config.GetPartitionConfig();
    const auto& pqConfig = AppData(ctx)->PQConfig;

    auto consumerName = NPersQueue::ConvertOldConsumerName(config.GetReadRules(i), ctx);
    rr->set_name(consumerName);
    rr->mutable_read_from()->set_seconds(config.GetReadFromTimestampsMs(i) / 1000);
    auto version = config.GetReadRuleVersions(i);
    if (version != 0)
        (*rr->mutable_attributes())["_version"] = TStringBuilder() << version;
    for (const auto &codec : config.GetConsumerCodecs(i).GetIds()) {
        rr->mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec) (codec + 1));
    }
    bool important = false;
    for (const auto &c : partConfig.GetImportantClientId()) {
        if (c == config.GetReadRules(i)) {
            important = true;
            break;
        }
    }
    rr->set_important(important);
    TString serviceType = "";
    if (i < config.ReadRuleServiceTypesSize()) {
        serviceType = config.GetReadRuleServiceTypes(i);
    } else {
        if (pqConfig.GetDisallowDefaultClientServiceType()) {
            error = "service type must be set for all read rules";
            status = Ydb::StatusIds::INTERNAL_ERROR;
            return false;
        }
        serviceType = pqConfig.GetDefaultClientServiceType().GetName();
    }
    (*rr->mutable_attributes())["_service_type"] = serviceType;
    return true;
}

void TDescribeTopicActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev, ctx)) {
        return;
    }

    const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    const TString path = JoinSeq("/", response.Path);

    Ydb::Scheme::Entry *selfEntry = Result.mutable_self();
    ConvertDirectoryEntry(response.Self->Info, selfEntry, true);
    if (const auto& name = GetCdcStreamName()) {
        selfEntry->set_name(*name);
    }

    if (response.PQGroupInfo) {
        const auto &pqDescr = response.PQGroupInfo->Description;
        Result.mutable_partitioning_settings()->set_min_active_partitions(pqDescr.GetTotalGroupCount());
        for(ui32 i = 0; i < pqDescr.GetTotalGroupCount(); ++i) {
            auto part = Result.add_partitions();
            part->set_partition_id(i);
            part->set_active(true);
        }

        const auto &config = pqDescr.GetPQTabletConfig();
        if (!config.GetRequireAuthWrite()) {
            (*Result.mutable_attributes())["_allow_unauthenticated_write"] = "true";
        }

        if (!config.GetRequireAuthRead()) {
            (*Result.mutable_attributes())["_allow_unauthenticated_read"] = "true";
        }

        if (pqDescr.GetPartitionPerTablet() != 2) {
            (*Result.mutable_attributes())["_partitions_per_tablet"] =
                TStringBuilder() << pqDescr.GetPartitionPerTablet();
        }
        if (config.HasAbcId()) {
            (*Result.mutable_attributes())["_abc_id"] = TStringBuilder() << config.GetAbcId();
        }
        if (config.HasAbcSlug()) {
            (*Result.mutable_attributes())["_abc_slug"] = config.GetAbcSlug();
        }
        if (config.HasFederationAccount()) {
            (*Result.mutable_attributes())["_federation_account"] = config.GetFederationAccount();
        }
        bool local = config.GetLocalDC();
        const auto &partConfig = config.GetPartitionConfig();
        i64 msip = partConfig.GetMaxSizeInPartition();
        if (partConfig.HasMaxSizeInPartition() && msip != Max<i64>()) {
            (*Result.mutable_attributes())["_max_partition_storage_size"] = TStringBuilder() << msip;
        }
        Result.mutable_retention_period()->set_seconds(partConfig.GetLifetimeSeconds());
        Result.set_retention_storage_mb(partConfig.GetStorageLimitBytes() / 1024 / 1024);
        (*Result.mutable_attributes())["_message_group_seqno_retention_period_ms"] = TStringBuilder() << (partConfig.GetSourceIdLifetimeSeconds() * 1000);
        (*Result.mutable_attributes())["__max_partition_message_groups_seqno_stored"] = TStringBuilder() << partConfig.GetSourceIdMaxCounts();

        const auto& pqConfig = AppData(ctx)->PQConfig;

        if (local || pqConfig.GetTopicsAreFirstClassCitizen()) {
            Result.set_partition_write_speed_bytes_per_second(partConfig.GetWriteSpeedInBytesPerSecond());
            Result.set_partition_write_burst_bytes(partConfig.GetBurstSize());
        }

        for (const auto &codec : config.GetCodecs().GetIds()) {
            Result.mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec)(codec + 1));
        }

        if (pqConfig.GetBillingMeteringConfig().GetEnabled()) {
            switch (config.GetMeteringMode()) {
                case NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY:
                    Result.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
                    break;
                case NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS:
                    Result.set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);
                    break;
                default:
                    break;
            }
        }
        auto consumerName = NPersQueue::ConvertNewConsumerName(Consumer, ctx);
        bool found = false;
        for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
            if (consumerName == config.GetReadRules(i)) found = true;
            auto rr = Result.add_consumers();
            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillConsumerProto(rr, config, i, ctx, status, error)) {
                return RaiseError(error, Ydb::PersQueue::ErrorCode::ERROR, status, ctx);
            }
        }

        if (GetProtoRequest()->include_stats()) {
            if (Consumer && !found) {
                Request_->RaiseIssue(FillIssue(TStringBuilder() << "no consumer '" << Consumer << "' in topic", Ydb::PersQueue::ErrorCode::ERROR));
                return ReplyWithResult(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }

            ProcessTablets(pqDescr, ctx);
            return;
        }
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}

void TDescribeConsumerActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev, ctx)) {
        return;
    }
    const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    const TString path = JoinSeq("/", response.Path);

    Ydb::Scheme::Entry *selfEntry = Result.mutable_self();
    ConvertDirectoryEntry(response.Self->Info, selfEntry, true);
    //TODO: change entry
    if (const auto& name = GetCdcStreamName()) {
        selfEntry->set_name(*name);
    }
    selfEntry->set_name(selfEntry->name() + "/" + Consumer);

    if (response.PQGroupInfo) {
        const auto& pqDescr = response.PQGroupInfo->Description;
        const auto& config = pqDescr.GetPQTabletConfig();

        for(ui32 i = 0; i < pqDescr.GetTotalGroupCount(); ++i) {
            auto part = Result.add_partitions();
            part->set_partition_id(i);
            part->set_active(true);
        }

        auto consumerName = NPersQueue::ConvertNewConsumerName(Consumer, ctx);
        bool found = false;
        for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
            if (consumerName != config.GetReadRules(i))
                continue;
            found = true;
            auto rr = Result.mutable_consumer();
            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillConsumerProto(rr, config, i, ctx, status, error)) {
                return RaiseError(error, Ydb::PersQueue::ErrorCode::ERROR, status, ctx);
            }
            break;
        }
        if (!found) {
            Request_->RaiseIssue(FillIssue(TStringBuilder() << "no consumer '" << Consumer << "' in topic", Ydb::PersQueue::ErrorCode::ERROR));
            return ReplyWithResult(Ydb::StatusIds::SCHEME_ERROR, ctx);
        }

        if (GetProtoRequest()->include_stats()) {
            ProcessTablets(pqDescr, ctx);
            return;
        }
    }

    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}


bool TDescribeTopicActorImpl::ProcessTablets(const NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr, const TActorContext& ctx) {
    for (ui32 i = 0; i < pqDescr.PartitionsSize(); ++i) {
        const auto& pi = pqDescr.GetPartitions(i);
        Tablets[pi.GetTabletId()].Partitions.push_back(pi.GetPartitionId());
        Tablets[pi.GetTabletId()].TabletId = pi.GetTabletId();
    }
    for (auto& pair : Tablets) {
        RequestTablet(pair.second, ctx);
    }
    if (!Consumer.empty()) {
        BalancerTabletId = pqDescr.GetBalancerTabletID();
        Tablets[BalancerTabletId].TabletId = BalancerTabletId;
    }

    if (RequestsInfly == 0) {
        Reply(ctx);
        return false;
    }
    return true;
}

void TDescribeTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);

    SendDescribeProposeRequest(ctx);
    Become(&TDescribeTopicActor::StateWork);
}

void TDescribeConsumerActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);

    SendDescribeProposeRequest(ctx);
    Become(&TDescribeConsumerActor::StateWork);
}

}

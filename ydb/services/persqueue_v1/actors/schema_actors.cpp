#include "schema_actors.h"

#include "persqueue_utils.h"

#include <ydb/core/client/server/ic_nodes_cache_service.h>
#include <ydb/core/persqueue/utils.h>
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

void TPQDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev);
    }
}


void TPQDescribeTopicActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev)) {
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

        if (local || AppData(ActorContext())->PQConfig.GetTopicsAreFirstClassCitizen()) {
            settings->set_max_partition_write_speed(partConfig.GetWriteSpeedInBytesPerSecond());
            settings->set_max_partition_write_burst(partConfig.GetBurstSize());
        }

        settings->set_supported_format(
                                       (Ydb::PersQueue::V1::TopicSettings::Format) (config.GetFormatVersion() + 1));

        for (const auto &codec : config.GetCodecs().GetIds()) {
            settings->add_supported_codecs((Ydb::PersQueue::V1::Codec) (codec + 1));
        }

        const auto& pqConfig = AppData(ActorContext())->PQConfig;

        for (const auto& consumer : config.GetConsumers()) {
            auto rr = settings->add_read_rules();
            auto consumerName = NPersQueue::ConvertOldConsumerName(consumer.GetName(), ActorContext());
            rr->set_consumer_name(consumerName);
            rr->set_starting_message_timestamp_ms(consumer.GetReadFromTimestampsMs());
            rr->set_supported_format(
                                     (Ydb::PersQueue::V1::TopicSettings::Format) (consumer.GetFormatVersion() + 1));
            rr->set_version(consumer.GetVersion());
            for (const auto &codec : consumer.GetCodec().GetIds()) {
                rr->add_supported_codecs((Ydb::PersQueue::V1::Codec) (codec + 1));
            }
            rr->set_important(consumer.GetImportant());

            if (consumer.HasServiceType()) {
                rr->set_service_type(consumer.GetServiceType());
            } else {
                if (pqConfig.GetDisallowDefaultClientServiceType()) {
                    this->Request_->RaiseIssue(FillIssue(
                        "service type must be set for all read rules",
                        Ydb::PersQueue::ErrorCode::ERROR
                    ));
                    Reply(Ydb::StatusIds::INTERNAL_ERROR, ActorContext());
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
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ActorContext());
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
            return RespondWithCode(status);
        }
    }

    const auto& pqDescr = modifyScheme.GetCreatePersQueueGroup();
    const auto& config = pqDescr.GetPQTabletConfig();
    if (!LocalCluster.empty() && config.GetLocalDC() && config.GetDC() != LocalCluster) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Local cluster is not correct - provided '" << config.GetDC()
                                    << "' instead of " << LocalCluster, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return RespondWithCode(Ydb::StatusIds::BAD_REQUEST);
    }
    if (Count(Clusters, config.GetDC()) == 0 && !Clusters.empty()) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Unknown cluster '" << config.GetDC() << "'", Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return RespondWithCode(Ydb::StatusIds::BAD_REQUEST);
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
            return RespondWithCode(status);
        }
    }

    const auto& pqDescr = modifyScheme.GetCreatePersQueueGroup();
    const auto& config = pqDescr.GetPQTabletConfig();

    if (!LocalCluster.empty() && config.GetLocalDC() && config.GetDC() != LocalCluster) {
        Request_->RaiseIssue(FillIssue(TStringBuilder() << "Local cluster is not correct - provided '" << config.GetDC()
                                    << "' instead of " << LocalCluster, Ydb::PersQueue::ErrorCode::BAD_REQUEST));
        return RespondWithCode(Ydb::StatusIds::BAD_REQUEST);
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

        return RespondWithCode(status);
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
        return RespondWithCode(status);
    }
}


TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::TEvDescribeTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::DescribeTopic(
            request->GetProtoRequest()->include_stats(),
            request->GetProtoRequest()->include_location()))
{
    ALOG_DEBUG(NKikimrServices::PQ_READ_PROXY, "TDescribeTopicActor for request " << request->GetProtoRequest()->DebugString());
}

TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::IRequestOpCtx * ctx)
    : TBase(ctx, dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->path())
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::DescribeTopic(
            dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->include_stats(),
            dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->include_location()))
{
}

TDescribeConsumerActor::TDescribeConsumerActor(NKikimr::NGRpcService::TEvDescribeConsumerRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::DescribeConsumer(
            request->GetProtoRequest()->consumer(),
            request->GetProtoRequest()->include_stats(),
            request->GetProtoRequest()->include_location()))
{
    ALOG_DEBUG(NKikimrServices::PQ_READ_PROXY, "TDescribeConsumerActor for request " << request->GetProtoRequest()->DebugString());
}

TDescribeConsumerActor::TDescribeConsumerActor(NKikimr::NGRpcService::IRequestOpCtx * ctx)
    : TBase(ctx, dynamic_cast<const Ydb::Topic::DescribeConsumerRequest*>(ctx->GetRequest())->path())
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::DescribeConsumer(
            dynamic_cast<const Ydb::Topic::DescribeConsumerRequest*>(ctx->GetRequest())->consumer(),
            dynamic_cast<const Ydb::Topic::DescribeConsumerRequest*>(ctx->GetRequest())->include_stats(),
            dynamic_cast<const Ydb::Topic::DescribeConsumerRequest*>(ctx->GetRequest())->include_location()))
{
}

TDescribeTopicActorImpl::TDescribeTopicActorImpl(const TDescribeTopicActorSettings& settings)
    : Settings(settings)
{

}


bool TDescribeTopicActorImpl::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    switch (ev->GetTypeRewrite()) {
        HFuncCtx(TEvTabletPipe::TEvClientDestroyed, Handle, ctx);
        HFuncCtx(TEvTabletPipe::TEvClientConnected, Handle, ctx);
        HFuncCtx(NKikimr::TEvPersQueue::TEvStatusResponse, Handle, ctx);
        HFuncCtx(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse, Handle, ctx);
        HFuncCtx(TEvPersQueue::TEvGetPartitionsLocationResponse, Handle, ctx);
        HFuncCtx(TEvPQProxy::TEvRequestTablet, Handle, ctx);
        default: return false;
    }
    return true;
}

void TDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    if (!TDescribeTopicActorImpl::StateWork(ev, this->ActorContext())) {
        TBase::StateWork(ev);
    }
}

void TDescribeConsumerActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    if (!TDescribeTopicActorImpl::StateWork(ev, this->ActorContext())) {
        TBase::StateWork(ev);
    }
}


void TDescribeTopicActorImpl::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        RestartTablet(ev->Get()->TabletId, ctx, ev->Sender);
    } else {
        auto it = Tablets.find(ev->Get()->TabletId);
        if (it == Tablets.end()) return;
        it->second.NodeId = ev->Get()->ServerId.NodeId();
        it->second.Generation = ev->Get()->Generation;
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

    auto& tabletInfo = it->second;
    if (pipe && pipe != tabletInfo.Pipe) return;
    if (tabletInfo.ResultRecived) return;

    if (tabletId == BalancerTabletId) {
        if (GotLocation && GotReadSessions) {
            return;
        }
        BalancerPipe = nullptr;
    }

    NTabletPipe::CloseClient(ctx, tabletInfo.Pipe);
    tabletInfo.Pipe = TActorId{};

    if (--it->second.RetriesLeft == 0) {
        return RaiseError(TStringBuilder() << "Tablet " << tabletId << " unresponsible", Ydb::PersQueue::ErrorCode::ERROR, Ydb::StatusIds::INTERNAL_ERROR, ctx);
    }

    ctx.Schedule(delay, new TEvPQProxy::TEvRequestTablet(tabletId));
}

void TDescribeTopicActorImpl::Handle(TEvPQProxy::TEvRequestTablet::TPtr& ev, const TActorContext& ctx) {
    auto it = Tablets.find(ev->Get()->TabletId);
    if (it == Tablets.end()) return;
    auto& tabletInfo = it->second;

    if (ev->Get()->TabletId == BalancerTabletId) {
        if (GotLocation && GotReadSessions) {
            return;
        }
        if (!GotLocation) {
            Y_ABORT_UNLESS(RequestsInfly > 0);
            --RequestsInfly;
        }
        if (!GotReadSessions) {
            Y_ABORT_UNLESS(RequestsInfly > 0);
            --RequestsInfly;
        }
    } else if (tabletInfo.ResultRecived) {
        return;
    } else {
        Y_ABORT_UNLESS(RequestsInfly > 0);
        --RequestsInfly;
    }

    RequestTablet(tabletInfo, ctx);
}

void TDescribeTopicActorImpl::RequestTablet(ui64 tabletId, const TActorContext& ctx) {
    auto it = Tablets.find(tabletId);
    if (it != Tablets.end()) {
        RequestTablet(it->second, ctx);
    }
}

TActorId CreatePipe(ui64 tabletId, const TActorContext& ctx) {
    return ctx.Register(NTabletPipe::CreateClient(
            ctx.SelfID, tabletId, NTabletPipe::TClientConfig(NTabletPipe::TClientRetryPolicy::WithRetries())
    ));
}

void TDescribeTopicActorImpl::RequestTablet(TTabletInfo& tablet, const TActorContext& ctx) {
    if (!tablet.Pipe)
        tablet.Pipe = CreatePipe(tablet.TabletId, ctx);

    if (tablet.TabletId == BalancerTabletId) {
        BalancerPipe = &tablet.Pipe;
        RequestBalancer(ctx);
    } else {
        RequestPartitionStatus(tablet, ctx);
    }
}

void TDescribeTopicActorImpl::RequestBalancer(const TActorContext& ctx) {
    Y_ABORT_UNLESS(BalancerTabletId);
    if (Settings.RequireLocation) {
        if (!GotLocation) {
            RequestPartitionsLocation(ctx);
        }
    } else {
        GotLocation = true;
    }

    if (Settings.Mode == TDescribeTopicActorSettings::EMode::DescribeConsumer && Settings.RequireStats) {
        if (!GotReadSessions) {
            RequestReadSessionsInfo(ctx);
        }
    } else {
        GotReadSessions = true;
    }
}

void TDescribeTopicActorImpl::RequestPartitionStatus(const TTabletInfo& tablet, const TActorContext& ctx) {
    THolder<NKikimr::TEvPersQueue::TEvStatus> ev;
    if (Settings.Consumers.empty()) {
        ev = MakeHolder<NKikimr::TEvPersQueue::TEvStatus>(
            Settings.Consumer.empty() ? "" : NPersQueue::ConvertNewConsumerName(Settings.Consumer, ctx),
            Settings.Consumer.empty()
        );
    } else {
        ev = MakeHolder<NKikimr::TEvPersQueue::TEvStatus>();
        for (const auto& consumer : Settings.Consumers) {
            ev->Record.AddConsumers(consumer);
        }
    }
    NTabletPipe::SendData(ctx, tablet.Pipe, ev.Release());
    ++RequestsInfly;
}

void TDescribeTopicActorImpl::RequestPartitionsLocation(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "DescribeTopicImpl " << ctx.SelfID.ToString() << ": Request location");

    THashSet<ui64> partIds;
    TVector<ui64> partsVector;
    for (auto p : Settings.Partitions) {
        if (p >= TotalPartitions) {
            return RaiseError(
                TStringBuilder() << "No partition " << Settings.Partitions[0] << " in topic",
                Ydb::PersQueue::ErrorCode::BAD_REQUEST, Ydb::StatusIds::BAD_REQUEST, ctx
            );
        }
        auto res = partIds.insert(p);
        if (res.second) {
            partsVector.push_back(p);
        }
    }
    NTabletPipe::SendData(
        ctx, *BalancerPipe,
        new TEvPersQueue::TEvGetPartitionsLocation(partsVector)
    );
    ++RequestsInfly;
}

void TDescribeTopicActorImpl::RequestReadSessionsInfo(const TActorContext& ctx) {
    Y_ABORT_UNLESS(Settings.Mode == TDescribeTopicActorSettings::EMode::DescribeConsumer);
    NTabletPipe::SendData(
            ctx, *BalancerPipe,
                    new TEvPersQueue::TEvGetReadSessionsInfo(NPersQueue::ConvertNewConsumerName(Settings.Consumer, ctx))
            );
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "DescribeTopicImpl " << ctx.SelfID.ToString() << ": Request sessions");
    ++RequestsInfly;
}

void TDescribeTopicActorImpl::Handle(NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
    auto it = Tablets.find(ev->Get()->Record.GetTabletId());
    if (it == Tablets.end()) return;

    auto& tabletInfo = it->second;

    if (tabletInfo.ResultRecived) return;

    auto& record = ev->Get()->Record;
    bool doRestart = (record.PartResultSize() == 0);

    for (auto& partResult : record.GetPartResult()) {
        if (partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_INITIALIZING ||
            partResult.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_UNKNOWN) {
                doRestart = true;
                break;
        }
    }
    if (doRestart) {
        RestartTablet(record.GetTabletId(), ctx, {}, TDuration::MilliSeconds(100));
        return;
    }

    tabletInfo.ResultRecived = true;
    Y_ABORT_UNLESS(RequestsInfly > 0);
    --RequestsInfly;

    NTabletPipe::CloseClient(ctx, tabletInfo.Pipe);
    tabletInfo.Pipe = TActorId{};

    ApplyResponse(tabletInfo, ev, ctx);

    if (RequestsInfly == 0) {
        Reply(ctx);
    }
}


void TDescribeTopicActorImpl::Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "DescribeTopicImpl " << ctx.SelfID.ToString() << ": Got sessions");

    if (GotReadSessions)
        return;

    auto it = Tablets.find(BalancerTabletId);
    Y_ABORT_UNLESS(it != Tablets.end());

    GotReadSessions = true;
    Y_ABORT_UNLESS(RequestsInfly > 0);
    --RequestsInfly;

    CheckCloseBalancerPipe(ctx);
    ApplyResponse(it->second, ev, ctx);

    if (RequestsInfly == 0) {
        Reply(ctx);
    }
}

void TDescribeTopicActorImpl::Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "DescribeTopicImpl " << ctx.SelfID.ToString() << ": Got location");

    if (GotLocation)
        return;

    auto it = Tablets.find(BalancerTabletId);
    Y_ABORT_UNLESS(it != Tablets.end());

    const auto& record = ev->Get()->Record;
    if (record.GetStatus()) {
        auto res = ApplyResponse(ev, ctx);
        if (res) {
            GotLocation = true;
            Y_ABORT_UNLESS(RequestsInfly > 0);
            --RequestsInfly;

            CheckCloseBalancerPipe(ctx);

            if (RequestsInfly == 0) {
                Reply(ctx);
            }
            return;
        }
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "DescribeTopicImpl " << ctx.SelfID.ToString() << ": Something wrong on location, retry. Response: " << record.DebugString());
    //Something gone wrong, retry
    ctx.Schedule(TDuration::MilliSeconds(200), new TEvPQProxy::TEvRequestTablet(BalancerTabletId));
}

void TDescribeTopicActorImpl::CheckCloseBalancerPipe(const TActorContext& ctx) {
    if (!GotLocation || !GotReadSessions)
        return;
    NTabletPipe::CloseClient(ctx, *BalancerPipe);
    *BalancerPipe = TActorId{};
    BalancerTabletId = 0;
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

void SetPartitionLocation(const NKikimrPQ::TPartitionLocation& location, Ydb::Topic::PartitionLocation* result) {
    result->set_node_id(location.GetNodeId());
    result->set_generation(location.GetGeneration());
}


void TDescribeTopicActor::ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    Y_UNUSED(tabletInfo);
    Y_UNUSED(ev);
    Y_ABORT("");
}


void AddWindowsStat(Ydb::Topic::MultipleWindowsStat *stat, ui64 perMin, ui64 perHour, ui64 perDay) {
    stat->set_per_minute(stat->per_minute() + perMin);
    stat->set_per_hour(stat->per_hour() + perHour);
    stat->set_per_day(stat->per_day() + perDay);
}

void FillPartitionStats(const NKikimrPQ::TStatusResponse::TPartResult& partResult, Ydb::Topic::PartitionStats* partStats, ui64 nodeId) {
    partStats->set_store_size_bytes(partResult.GetPartitionSize());
    partStats->mutable_partition_offsets()->set_start(partResult.GetStartOffset());
    partStats->mutable_partition_offsets()->set_end(partResult.GetEndOffset());

    SetProtoTime(partStats->mutable_last_write_time(), partResult.GetLastWriteTimestampMs());
    SetProtoTime(partStats->mutable_max_write_time_lag(), partResult.GetWriteLagMs());

    AddWindowsStat(partStats->mutable_bytes_written(), partResult.GetAvgWriteSpeedPerMin(), partResult.GetAvgWriteSpeedPerHour(), partResult.GetAvgWriteSpeedPerDay());

    partStats->set_partition_node_id(nodeId);
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
        if (it == res.end())
            continue;
        FillPartitionStats(it->second, partRes.mutable_partition_stats(), tabletInfo.NodeId);
    }
}

bool TDescribeTopicActor::ApplyResponse(
        TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext&
) {
    const auto& record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.LocationsSize() == TotalPartitions);
    Y_ABORT_UNLESS(Settings.RequireLocation);

    for (auto i = 0u; i < TotalPartitions; ++i) {
        const auto& location = record.GetLocations(i);
        auto* locationResult = Result.mutable_partitions(i)->mutable_partition_location();
        SetPartitionLocation(location, locationResult);

    }
    return true;
}



void TDescribeTopicActor::Reply(const TActorContext& ctx) {
    if (TBase::IsDead) {
        return;
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}

void TDescribeConsumerActor::Reply(const TActorContext& ctx) {
    if (TBase::IsDead) {
        return;
    }
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

        if (Settings.Consumer) {
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
            } else {
                auto* stats = Result.mutable_consumer()->mutable_consumer_stats();

                UpdateProtoTime(stats->mutable_min_partitions_last_read_time(), partResult.GetLagsInfo().GetLastReadTimestampMs(), true);
                UpdateProtoTime(stats->mutable_max_read_time_lag(), partResult.GetLagsInfo().GetReadLagMs(), false);
                UpdateProtoTime(stats->mutable_max_write_time_lag(), partResult.GetLagsInfo().GetWriteLagMs(), false);
            }
        }
    }
}

bool TDescribeConsumerActor::ApplyResponse(
        TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext&
) {
    const auto& record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.LocationsSize() == TotalPartitions);
    Y_ABORT_UNLESS(Settings.RequireLocation);
    for (auto i = 0u; i < TotalPartitions; ++i) {
        const auto& location = record.GetLocations(i);
        auto* locationResult = Result.mutable_partitions(i)->mutable_partition_location();
        SetPartitionLocation(location, locationResult);
    }
    return true;
}


bool FillConsumerProto(Ydb::Topic::Consumer *rr, const NKikimrPQ::TPQTabletConfig::TConsumer& consumer,
                        const NActors::TActorContext& ctx, Ydb::StatusIds::StatusCode& status, TString& error)
{
    const auto& pqConfig = AppData(ctx)->PQConfig;

    auto consumerName = NPersQueue::ConvertOldConsumerName(consumer.GetName(), ctx);
    rr->set_name(consumerName);
    rr->mutable_read_from()->set_seconds(consumer.GetReadFromTimestampsMs() / 1000);
    auto version = consumer.GetVersion();
    if (version != 0)
        (*rr->mutable_attributes())["_version"] = TStringBuilder() << version;
    for (const auto &codec : consumer.GetCodec().GetIds()) {
        rr->mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec) (codec + 1));
    }

    rr->set_important(consumer.GetImportant());
    TString serviceType = "";
    if (consumer.HasServiceType()) {
        serviceType = consumer.GetServiceType();
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

void TDescribeTopicActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev)) {
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
        const auto& pqDescr = response.PQGroupInfo->Description;
        for(ui32 i = 0; i < pqDescr.GetTotalGroupCount(); ++i) {
            auto part = Result.add_partitions();
            part->set_partition_id(i);
            part->set_active(true);
        }

        const auto &config = pqDescr.GetPQTabletConfig();
        Result.mutable_partitioning_settings()->set_min_active_partitions(config.GetPartitionStrategy().GetMinPartitionCount());
        Result.mutable_partitioning_settings()->set_max_active_partitions(config.GetPartitionStrategy().GetMaxPartitionCount());
        switch(config.GetPartitionStrategy().GetPartitionStrategyType()) {
            case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT:
                Result.mutable_partitioning_settings()->mutable_autoscaling_settings()->set_strategy(Ydb::Topic::AutoscalingStrategy::AUTOSCALING_STRATEGY_SCALE_UP);
                break;
            case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE:
                Result.mutable_partitioning_settings()->mutable_autoscaling_settings()->set_strategy(Ydb::Topic::AutoscalingStrategy::AUTOSCALING_STRATEGY_SCALE_UP_AND_DOWN);
                break;
            default:
                Result.mutable_partitioning_settings()->mutable_autoscaling_settings()->set_strategy(Ydb::Topic::AutoscalingStrategy::AUTOSCALING_STRATEGY_DISABLED);
                break;
        }
        Result.mutable_partitioning_settings()->mutable_autoscaling_settings()->mutable_partition_write_speed()->mutable_threshold_time()->set_seconds(config.GetPartitionStrategy().GetScaleThresholdSeconds());
        Result.mutable_partitioning_settings()->mutable_autoscaling_settings()->mutable_partition_write_speed()->set_scale_down_threshold_percent(config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent());
        Result.mutable_partitioning_settings()->mutable_autoscaling_settings()->mutable_partition_write_speed()->set_scale_up_threshold_percent(config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent());

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

        const auto& pqConfig = AppData(ActorContext())->PQConfig;

        if (local || pqConfig.GetTopicsAreFirstClassCitizen()) {
            Result.set_partition_write_speed_bytes_per_second(partConfig.GetWriteSpeedInBytesPerSecond());
            Result.set_partition_write_burst_bytes(partConfig.GetBurstSize());
        }

        if (pqConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()) {
            auto readSpeedPerConsumer = partConfig.GetWriteSpeedInBytesPerSecond() * 2;
            Result.set_partition_total_read_speed_bytes_per_second(readSpeedPerConsumer * pqConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition());
            Result.set_partition_consumer_read_speed_bytes_per_second(readSpeedPerConsumer);
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
        auto consumerName = NPersQueue::ConvertNewConsumerName(Settings.Consumer, ActorContext());
        bool found = false;
        for (const auto& consumer : config.GetConsumers()) {
            if (consumerName == consumer.GetName()) {
                 found = true;
            }
            auto rr = Result.add_consumers();
            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillConsumerProto(rr, consumer, ActorContext(), status, error)) {
                return RaiseError(error, Ydb::PersQueue::ErrorCode::ERROR, status, ActorContext());
            }
        }

        if (GetProtoRequest()->include_stats() || GetProtoRequest()->include_location()) {
            if (Settings.Consumer && !found) {
                Request_->RaiseIssue(FillIssue(
                        TStringBuilder() << "no consumer '" << Settings.Consumer << "' in topic",
                        Ydb::PersQueue::ErrorCode::BAD_REQUEST
                ));
                return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
            }

            ProcessTablets(pqDescr, ActorContext());
            return;
        }
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ActorContext());
}

void TDescribeConsumerActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev)) {
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
    selfEntry->set_name(selfEntry->name() + "/" + Settings.Consumer);

    if (response.PQGroupInfo) {
        const auto& pqDescr = response.PQGroupInfo->Description;
        const auto& config = pqDescr.GetPQTabletConfig();

        for(ui32 i = 0; i < pqDescr.GetTotalGroupCount(); ++i) {
            auto part = Result.add_partitions();
            part->set_partition_id(i);
            part->set_active(true);
        }

        auto consumerName = NPersQueue::ConvertNewConsumerName(Settings.Consumer, ActorContext());
        bool found = false;
        for (const auto& consumer : config.GetConsumers()) {
            if (consumerName != consumer.GetName()) {
                continue;
            }
            found = true;

            auto rr = Result.mutable_consumer();
            Ydb::StatusIds::StatusCode status;
            TString error;
            if (!FillConsumerProto(rr, consumer, ActorContext(), status, error)) {
                return RaiseError(error, Ydb::PersQueue::ErrorCode::ERROR, status, ActorContext());
            }
            break;
        }
        if (!found) {
            Request_->RaiseIssue(FillIssue(
                    TStringBuilder() << "no consumer '" << Settings.Consumer << "' in topic",
                    Ydb::PersQueue::ErrorCode::BAD_REQUEST
            ));
            return RespondWithCode(Ydb::StatusIds::SCHEME_ERROR);
        }

        if (GetProtoRequest()->include_stats() || GetProtoRequest()->include_location()) {
            ProcessTablets(pqDescr, ActorContext());
            return;
        }
    }

    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ActorContext());
}



bool TDescribeTopicActorImpl::ProcessTablets(
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr, const TActorContext& ctx
) {
    std::unordered_set<ui32> partitionSet(Settings.Partitions.begin(), Settings.Partitions.end());
    auto partitionFilter = [&] (ui32 partId) {
        if (Settings.Mode == TDescribeTopicActorSettings::EMode::DescribePartitions) {
            return Settings.RequireStats && partId == Settings.Partitions[0];
        } else if (Settings.Mode == TDescribeTopicActorSettings::EMode::DescribeTopic) {
            return Settings.RequireStats && (partitionSet.empty() || partitionSet.find(partId) != partitionSet.end());
        } else {
            return Settings.RequireStats;
        }
        return true;
    };
    TotalPartitions = pqDescr.GetTotalGroupCount();

    BalancerTabletId = pqDescr.GetBalancerTabletID();
    Tablets[BalancerTabletId].TabletId = BalancerTabletId;

    for (ui32 i = 0; i < pqDescr.PartitionsSize(); ++i) {
        const auto& pi = pqDescr.GetPartitions(i);
        if (!partitionFilter(pi.GetPartitionId())) {
            continue;
        }
        Tablets[pi.GetTabletId()].Partitions.push_back(pi.GetPartitionId());
        Tablets[pi.GetTabletId()].TabletId = pi.GetTabletId();
    }

    for (auto& pair : Tablets) {
        RequestTablet(pair.second, ctx);
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
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "Describe topic actor for path " << GetProtoRequest()->path());
}

void TDescribeConsumerActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);

    SendDescribeProposeRequest(ctx);
    Become(&TDescribeConsumerActor::StateWork);
}


template<class TProtoType>
TDescribeTopicActorSettings SettingsFromDescribePartRequest(TProtoType* request) {
    return TDescribeTopicActorSettings::DescribePartitionSettings(
        request->partition_id(), request->include_stats(), request->include_location()
    );
}

TDescribePartitionActor::TDescribePartitionActor(NKikimr::NGRpcService::TEvDescribePartitionRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
    , TDescribeTopicActorImpl(SettingsFromDescribePartRequest(request->GetProtoRequest()))
{
    ALOG_DEBUG(NKikimrServices::PQ_READ_PROXY, "TDescribePartitionActor for request " << request->GetProtoRequest()->DebugString());
}

TDescribePartitionActor::TDescribePartitionActor(NKikimr::NGRpcService::IRequestOpCtx* ctx)
    : TBase(ctx, dynamic_cast<const Ydb::Topic::DescribePartitionRequest*>(ctx->GetRequest())->path())
    , TDescribeTopicActorImpl(SettingsFromDescribePartRequest(dynamic_cast<const Ydb::Topic::DescribePartitionRequest*>(ctx->GetRequest())))
{
}

void TDescribePartitionActor::Bootstrap(const NActors::TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "TDescribePartitionActor" << ctx.SelfID.ToString() << ": Bootstrap");
    CheckAccessWithWriteTopicPermission = true;
    TBase::Bootstrap(ctx);
    SendDescribeProposeRequest(ctx);
    Become(&TDescribePartitionActor::StateWork);
}

void TDescribePartitionActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        case TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType:
            if (NeedToRequestWithDescribeSchema(ev)) {
                // We do not have the UpdateRow permission. Check if we're allowed to DescribeSchema.
                CheckAccessWithWriteTopicPermission = false;
                SendDescribeProposeRequest(ActorContext());
                break;
            }
            [[fallthrough]];
        default:
            if (!TDescribeTopicActorImpl::StateWork(ev, ActorContext())) {
                TBase::StateWork(ev);
            };
    }
}

// Return true if we need to send a second request to SchemeCache with DescribeSchema permission,
// because the first request checking the UpdateRow permission resulted in an AccessDenied error.
bool TDescribePartitionActor::NeedToRequestWithDescribeSchema(TAutoPtr<IEventHandle>& ev) {
    if (!CheckAccessWithWriteTopicPermission) {
        // We've already sent a request with DescribeSchema, ev is a response to it.
        return false;
    }

    auto evNav = *reinterpret_cast<typename TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr*>(&ev);
    auto const& entries = evNav->Get()->Request.Get()->ResultSet;
    Y_ABORT_UNLESS(entries.size() == 1);

    if (entries.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied) {
        // We do have access to the requested entity or there was an error.
        // Transfer ownership to the ev pointer, and let the base classes' StateWork methods handle the response.
        ev = *reinterpret_cast<TAutoPtr<IEventHandle>*>(&evNav);
        return false;
    }

    return true;
}

void TDescribePartitionActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto const& entries = ev->Get()->Request.Get()->ResultSet;
    Y_ABORT_UNLESS(entries.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev)) {
        return;
    }
    PQGroupInfo = entries[0].PQGroupInfo;
    auto* partRes = Result.mutable_partition();
    partRes->set_partition_id(Settings.Partitions[0]);
    partRes->set_active(true);
    ProcessTablets(PQGroupInfo->Description, this->ActorContext());
}

void TDescribePartitionActor::ApplyResponse(TTabletInfo&, NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr&, const TActorContext&) {
    Y_ABORT("");
}

void TDescribePartitionActor::ApplyResponse(TTabletInfo& tabletInfo, NKikimr::TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext&) {
    auto* partResult = Result.mutable_partition();

    const auto& record = ev->Get()->Record;
    for (auto partData : record.GetPartResult()) {
        if ((ui32)partData.GetPartition() != Settings.Partitions[0])
            continue;

        Y_ABORT_UNLESS((ui32)(partData.GetPartition()) == Settings.Partitions[0]);
        partResult->set_partition_id(partData.GetPartition());
        partResult->set_active(true);
        FillPartitionStats(partData, partResult->mutable_partition_stats(), tabletInfo.NodeId);
    }
}

bool TDescribePartitionActor::ApplyResponse(
        TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext&
) {
    const auto& record = ev->Get()->Record;
    if (Settings.Partitions) {
        Y_ABORT_UNLESS(record.LocationsSize() == 1);
    }

    const auto& location = record.GetLocations(0);
    auto* pResult = Result.mutable_partition();
    pResult->set_partition_id(location.GetPartitionId());
    pResult->set_active(true);
    auto* locationResult = pResult->mutable_partition_location();
    SetPartitionLocation(location, locationResult);
    return true;
}

void TDescribePartitionActor::RaiseError(
        const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status,
        const TActorContext&
) {
    if (TBase::IsDead)
        return;
    this->Request_->RaiseIssue(FillIssue(error, errorCode));
    TBase::RespondWithCode(status);
}

void TDescribePartitionActor::Reply(const TActorContext& ctx) {
    if (TBase::IsDead) {
        return;
    }
    if (Settings.RequireLocation) {
        Y_ABORT_UNLESS(Result.partition().has_partition_location());
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}

using namespace NIcNodeCache;

TPartitionsLocationActor::TPartitionsLocationActor(const TGetPartitionsLocationRequest& request, const TActorId& requester)
    : TBase(request, requester)
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::GetPartitionsLocation(request.PartitionIds))
{
}


void TPartitionsLocationActor::Bootstrap(const NActors::TActorContext&)
{
    SendDescribeProposeRequest();
    UnsafeBecome(&TPartitionsLocationActor::StateWork);
    SendNodesRequest();

}

void TPartitionsLocationActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvICNodesInfoCache::TEvGetAllNodesInfoResponse, Handle);
        default:
            if (!TDescribeTopicActorImpl::StateWork(ev, ActorContext())) {
                TBase::StateWork(ev);
            };
    }
}

void TPartitionsLocationActor::HandleCacheNavigateResponse(
    TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev
) {
    if (!TBase::HandleCacheNavigateResponseBase(ev)) {
        return;
    }

    if (ProcessTablets(PQGroupInfo->Description, this->ActorContext())) {
        Response->PathId = Self->Info.GetPathId();
        Response->SchemeShardId = Self->Info.GetSchemeshardId();
    }
}

bool TPartitionsLocationActor::ApplyResponse(
        TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext&
) {
    const auto& record = ev->Get()->Record;
    for (auto i = 0u; i < record.LocationsSize(); i++) {
        const auto& part = record.GetLocations(i);
        TEvPQProxy::TPartitionLocationInfo partLocation;
        ui64 nodeId = part.GetNodeId();

        partLocation.PartitionId = part.GetPartitionId();
        partLocation.Generation = part.GetGeneration();
        partLocation.NodeId = nodeId;
        Response->Partitions.emplace_back(std::move(partLocation));
    }
    if (GotNodesInfo)
        Finalize();
    else
        GotPartitions = true;
    return true;
}

void TPartitionsLocationActor::SendNodesRequest() const {
    auto* icEv = new TEvICNodesInfoCache::TEvGetAllNodesInfoRequest();
    ActorContext().Send(CreateICNodesInfoCacheServiceId(), icEv);

}

void TPartitionsLocationActor::Handle(TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev) {
    NodesInfoEv = ev;
    if (GotPartitions)
        Finalize();
    else
        GotNodesInfo = true;
}

void TPartitionsLocationActor::Finalize() {
    if (Settings.Partitions) {
        Y_ABORT_UNLESS(Response->Partitions.size() == Settings.Partitions.size());
    } else {
        Y_ABORT_UNLESS(Response->Partitions.size() == PQGroupInfo->Description.PartitionsSize());
    }
    for (auto& pInResponse : Response->Partitions) {
        auto iter = NodesInfoEv->Get()->NodeIdsMapping->find(pInResponse.NodeId);
        if (iter.IsEnd()) {
            return RaiseError(
                    TStringBuilder() << "Hostname not found for nodeId " << pInResponse.NodeId,
                    Ydb::PersQueue::ErrorCode::ERROR,
                    Ydb::StatusIds::INTERNAL_ERROR, ActorContext()
            );
        }
        pInResponse.Hostname = (*NodesInfoEv->Get()->Nodes)[iter->second].Host;
    }
    TBase::RespondWithCode(Ydb::StatusIds::SUCCESS);
}

void TPartitionsLocationActor::RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext&) {
    this->AddIssue(FillIssue(error, errorCode));
    this->RespondWithCode(status);
}

} // namespace NKikimr::NGRpcProxy::V1

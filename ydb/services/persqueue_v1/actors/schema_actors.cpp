#include "schema_actors.h"

#include "persqueue_utils.h"

#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>

namespace NKikimr::NGRpcProxy::V1 {

constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";

TDropTopicActor::TDropTopicActor(NKikimr::NGRpcService::TEvPQDropTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}

void TDropTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TDropTopicActor::StateWork);
}


void TDropTopicActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                         const TString& workingDir, const TString& name)
{
    Y_UNUSED(ctx);
    NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
    modifyScheme.SetWorkingDir(workingDir);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
    modifyScheme.MutableDrop()->SetName(name);
}

TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
{
}

void TDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev, ctx);
    }
}


void TDescribeTopicActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev, ctx)) {
        return;
    }

    const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    const TString path = JoinSeq("/", response.Path);

    Ydb::PersQueue::V1::DescribeTopicResult result;

    auto settings = result.mutable_settings();
    Ydb::Scheme::Entry *selfEntry = result.mutable_self();
    const auto& selfInfo = response.Self->Info;
    selfEntry->set_name(path + "/" + selfInfo.GetName());
    selfEntry->set_type(static_cast<Ydb::Scheme::Entry::Type>(selfInfo.GetPathType()));
    ConvertDirectoryEntry(selfInfo, selfEntry, true);
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
        settings->set_message_group_seqno_retention_period_ms(partConfig.GetSourceIdLifetimeSeconds() * 1000);
        settings->set_max_partition_message_groups_seqno_stored(partConfig.GetSourceIdMaxCounts());

        if (local) {
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


void TDescribeTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);

    SendDescribeProposeRequest(ctx);
    Become(&TDescribeTopicActor::StateWork);
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
    TString error = AddReadRuleToConfig(pqConfig, rule, serviceTypes, ctx);
    bool hasDuplicates = false;
    if (error.Empty()) {
        hasDuplicates = CheckReadRulesConfig(*pqConfig, serviceTypes, error);
    }

    if (!error.Empty()) {
        return ReplyWithError(hasDuplicates ? Ydb::StatusIds::ALREADY_EXISTS : Ydb::StatusIds::BAD_REQUEST,
                              hasDuplicates ? Ydb::PersQueue::ErrorCode::OK : Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
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

TCreateTopicActor::TCreateTopicActor(NKikimr::NGRpcService::TEvPQCreateTopicRequest* request, const TString& localCluster, const TVector<TString>& clusters)
    : TBase(request, request->GetProtoRequest()->path())
    , LocalCluster(localCluster)
    , Clusters(clusters)
{
    Y_ASSERT(request);
}

void TCreateTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TCreateTopicActor::StateWork);
}


TAlterTopicActor::TAlterTopicActor(NKikimr::NGRpcService::TEvPQAlterTopicRequest* request, const TString& localCluster)
    : TBase(request, request->GetProtoRequest()->path())
    , LocalCluster(localCluster)
{
    Y_ASSERT(request);
}

void TAlterTopicActor::Bootstrap(const NActors::TActorContext& ctx)
{
    TBase::Bootstrap(ctx);
    SendProposeRequest(ctx);
    Become(&TAlterTopicActor::StateWork);
}

void TCreateTopicActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
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


void TAlterTopicActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
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

}

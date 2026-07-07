#include "schema_actors.h"

#include "persqueue_utils.h"

#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/obfuscate/obfuscate.h>

#include <library/cpp/json/json_writer.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_READ_PROXY

namespace NKikimr::NGRpcProxy::V1 {

constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";

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
    AFL_ENSURE(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
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
        if (config.GetEnableCompactification()) {
            (*settings->mutable_attributes())["_cleanup_policy"] = "compact";
        }
        if (config.HasMetricsLevel()) {
            settings->set_metrics_level(config.GetMetricsLevel());
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
            settings->set_max_partition_write_messages_speed(partConfig.GetWriteSpeedInMessagesPerSecond());
            settings->set_max_partition_write_messages_burst(partConfig.GetBurstSizeInMessages());
        }

        settings->set_supported_format(
                                       (Ydb::PersQueue::V1::TopicSettings::Format) (config.GetFormatVersion() + 1));

        for (const auto &codec : config.GetCodecs().GetIds()) {
            settings->add_supported_codecs((Ydb::PersQueue::V1::Codec) (codec + 1));
        }

        const auto& pqConfig = AppData(ActorContext())->PQConfig;

        NJson::TJsonValue consumersAdvancedMonitoringSettings;
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
            if (consumer.HasAvailabilityPeriodMs()) {
                TDuration availabilityPeriod = TDuration::MilliSeconds(consumer.GetAvailabilityPeriodMs());
                rr->mutable_availability_period()->set_seconds(availabilityPeriod.Seconds());
                rr->mutable_availability_period()->set_nanos(availabilityPeriod.NanoSecondsOfSecond());
            }

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

            switch (consumer.GetType()) {
                case NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING: {
                    rr->mutable_streaming_consumer_type();
                    break;
                }
                case NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP: {
                    auto* shared = rr->mutable_shared_consumer_type();
                    shared->set_keep_messages_order(consumer.GetKeepMessageOrder());
                    if (consumer.GetDefaultProcessingTimeoutSeconds() > 0) {
                        shared->mutable_default_processing_timeout()->set_seconds(consumer.GetDefaultProcessingTimeoutSeconds());
                    }
                    if (const auto waitTimeMs = consumer.GetDefaultReceiveMessageWaitTimeMs(); waitTimeMs > 0) {
                        shared->mutable_receive_message_wait_time()->set_seconds(waitTimeMs / 1'000);
                        shared->mutable_receive_message_wait_time()->set_nanos((waitTimeMs % 1'000) * 1'000'000);
                    }
                    if (const auto delayTimeMs = consumer.GetDefaultDelayMessageTimeMs(); delayTimeMs > 0) {
                        shared->mutable_receive_message_delay()->set_seconds(delayTimeMs / 1'000);
                        shared->mutable_receive_message_delay()->set_nanos((delayTimeMs % 1'000) * 1'000'000);
                    }

                    if (consumer.GetDeadLetterPolicyEnabled()) {
                        auto* deadLetterPolicy = shared->mutable_dead_letter_policy();
                        deadLetterPolicy->set_enabled(true);
                        if (consumer.GetMaxProcessingAttempts() > 0) {
                            deadLetterPolicy->mutable_condition()->set_max_processing_attempts(consumer.GetMaxProcessingAttempts());
                        }
                        switch (consumer.GetDeadLetterPolicy()) {
                            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE:
                                deadLetterPolicy->mutable_move_action()->set_dead_letter_queue(consumer.GetDeadLetterQueue());
                                break;
                            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE:
                                deadLetterPolicy->mutable_delete_action();
                                break;
                            case NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED:
                                break;
                        }
                    }
                    break;
                }
            }

            NJson::TJsonValue customMonitoringSettings;
            if (consumer.HasMetricsLevel()) {
                customMonitoringSettings["metrics_level"] = consumer.GetMetricsLevel();
            }
            if (const auto& monitoringProjectId = consumer.GetMonitoringProjectId(); !monitoringProjectId.empty()) {
                customMonitoringSettings["monitoring_project_id"] = monitoringProjectId;
            }
            if (customMonitoringSettings.IsDefined()) { // at least one attribute is set
                consumersAdvancedMonitoringSettings[consumerName] = std::move(customMonitoringSettings);
            }
        }
        if (consumersAdvancedMonitoringSettings.IsDefined()) { // at least one consumer has custom monitoring settings
             (*settings->mutable_attributes())["_advanced_monitoring"] = WriteJson(consumersAdvancedMonitoringSettings, false, true);
        }

        if (NPQ::MirroringEnabled(config)) {
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

TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::TEvDescribeTopicRequest* request)
    : TBase(request, request->GetProtoRequest()->path())
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::DescribeTopic(
            request->GetProtoRequest()->include_stats(),
            request->GetProtoRequest()->include_location()))
{
    YDB_LOG_DEBUG("TDescribeTopicActor for request",
        {"request", request->GetProtoRequest()->DebugString()});
}

TDescribeTopicActor::TDescribeTopicActor(NKikimr::NGRpcService::IRequestOpCtx * ctx)
    : TBase(ctx, dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->path())
    , TDescribeTopicActorImpl(TDescribeTopicActorSettings::DescribeTopic(
            dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->include_stats(),
            dynamic_cast<const Ydb::Topic::DescribeTopicRequest*>(ctx->GetRequest())->include_location()))
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

void TDescribeTopicActorImpl::PassAway(const TActorContext& ctx) {
    for (auto& [_, tablet] : Tablets) {
        NTabletPipe::CloseClient(ctx, tablet.Pipe);
    }
}

void TDescribeTopicActor::StateWork(TAutoPtr<IEventHandle>& ev) {
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

void TDescribeTopicActorImpl::RestartTablet(ui64 tabletId, const TActorContext& ctx, TActorId pipe, const TDuration& delay) {
    auto it = Tablets.find(tabletId);
    if (it == Tablets.end()) return;

    auto& tabletInfo = it->second;
    if (pipe && pipe != tabletInfo.Pipe) return;
    if (tabletInfo.ResultRecived) return;

    if (tabletId == BalancerTabletId && GotLocation && GotReadSessions) {
        return;
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
            AFL_ENSURE(RequestsInfly > 0);
            --RequestsInfly;
        }
        if (!GotReadSessions) {
            AFL_ENSURE(RequestsInfly > 0);
            --RequestsInfly;
        }
    } else if (tabletInfo.ResultRecived) {
        return;
    } else {
        AFL_ENSURE(RequestsInfly > 0);
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
        RequestBalancer(ctx);
    } else {
        RequestPartitionStatus(tablet, ctx);
    }
}

void TDescribeTopicActorImpl::RequestBalancer(const TActorContext& ctx) {
    AFL_ENSURE(BalancerTabletId);
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
    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Request location",
        {"selfId", ctx.SelfID});

    THashSet<ui64> partIds;
    TVector<ui64> partsVector;
    for (auto p : Settings.Partitions) {
        if (p >= TotalPartitions) {
            return RaiseError(
                TStringBuilder() << "No partition " << p << " in topic",
                Ydb::PersQueue::ErrorCode::BAD_REQUEST, Ydb::StatusIds::BAD_REQUEST, ctx
            );
        }
        auto res = partIds.insert(p);
        if (res.second) {
            partsVector.push_back(p);
        }
    }
    NTabletPipe::SendData(
        ctx, Tablets[BalancerTabletId].Pipe,
        new TEvPersQueue::TEvGetPartitionsLocation(partsVector)
    );
    ++RequestsInfly;
}

void TDescribeTopicActorImpl::RequestReadSessionsInfo(const TActorContext& ctx) {
    AFL_ENSURE(Settings.Mode == TDescribeTopicActorSettings::EMode::DescribeConsumer);
    NTabletPipe::SendData(
            ctx, Tablets[BalancerTabletId].Pipe,
                    new TEvPersQueue::TEvGetReadSessionsInfo(NPersQueue::ConvertNewConsumerName(Settings.Consumer, ctx))
            );
    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Request sessions",
        {"selfId", ctx.SelfID});
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
    AFL_ENSURE(RequestsInfly > 0);
    --RequestsInfly;

    NTabletPipe::CloseClient(ctx, tabletInfo.Pipe);
    tabletInfo.Pipe = TActorId{};

    ApplyResponse(tabletInfo, ev, ctx);

    if (RequestsInfly == 0) {
        Reply(ctx);
    }
}


void TDescribeTopicActorImpl::Handle(NKikimr::TEvPersQueue::TEvReadSessionsInfoResponse::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Got sessions",
        {"selfId", ctx.SelfID});

    if (GotReadSessions)
        return;

    auto it = Tablets.find(BalancerTabletId);
    AFL_ENSURE(it != Tablets.end());

    GotReadSessions = true;
    AFL_ENSURE(RequestsInfly > 0);
    --RequestsInfly;

    CheckCloseBalancerPipe(ctx);
    ApplyResponse(it->second, ev, ctx);

    if (RequestsInfly == 0) {
        Reply(ctx);
    }
}

void TDescribeTopicActorImpl::Handle(TEvPersQueue::TEvGetPartitionsLocationResponse::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Got location",
        {"selfId", ctx.SelfID});

    if (GotLocation)
        return;

    auto it = Tablets.find(BalancerTabletId);
    AFL_ENSURE(it != Tablets.end());

    const auto& record = ev->Get()->Record;
    if (record.GetStatus()) {
        auto res = ApplyResponse(ev, ctx);
        if (res) {
            GotLocation = true;
            AFL_ENSURE(RequestsInfly > 0);
            --RequestsInfly;

            CheckCloseBalancerPipe(ctx);

            if (RequestsInfly == 0) {
                Reply(ctx);
            }
            return;
        }
    }

    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Something wrong on location, retry",
        {"selfId", ctx.SelfID},
        {"response", record.DebugString()});
    //Something gone wrong, retry
    ctx.Schedule(TDuration::MilliSeconds(200), new TEvPQProxy::TEvRequestTablet(BalancerTabletId));
}

void TDescribeTopicActorImpl::CheckCloseBalancerPipe(const TActorContext& ctx) {
    if (!GotLocation || !GotReadSessions) {
        return;
    }
    auto& balancerPipe = Tablets[BalancerTabletId].Pipe;
    if (balancerPipe) {
        NTabletPipe::CloseClient(ctx, balancerPipe);
        balancerPipe = {};
    }
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
                SetProtoTime(stats->mutable_max_committed_time_lag(), cons.GetCommitedLagMs());
            } else {
                auto* stats = it->second->mutable_consumer_stats();

                UpdateProtoTime(stats->mutable_min_partitions_last_read_time(), cons.GetLastReadTimestampMs(), true);
                UpdateProtoTime(stats->mutable_max_read_time_lag(), cons.GetReadLagMs(), false);
                UpdateProtoTime(stats->mutable_max_write_time_lag(), cons.GetWriteLagMs(), false);
                UpdateProtoTime(stats->mutable_max_committed_time_lag(), cons.GetCommitedLagMs(), false);
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
    AFL_ENSURE(Settings.RequireLocation);

    for (auto i = 0u; i < std::min<ui64>(record.LocationsSize(), TotalPartitions); ++i) {
        const auto& location = record.GetLocations(i);
        auto* locationResult = Result.mutable_partitions(i)->mutable_partition_location();
        SetPartitionLocation(location, locationResult);
    }
    return true;
}

void TDescribeTopicActor::PassAway() {
    TDescribeTopicActorImpl::PassAway(ActorContext());
    TBase::PassAway();
}


void TDescribeTopicActor::Reply(const TActorContext& ctx) {
    if (TBase::IsDead) {
        return;
    }
    return ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
}

void TDescribeTopicActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    AFL_ENSURE(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    if (ReplyIfNotTopic(ev)) {
        return;
    }

    const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    const TString path = JoinSeq("/", response.Path);

    if (response.PQGroupInfo) {
        const auto& pqDescr = response.PQGroupInfo->Description;
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!FillTopicDescription(Result, pqDescr, response.Self->Info, GetCdcStreamName(), status, error)) {
            return RaiseError(error, Ydb::PersQueue::ErrorCode::ERROR, status, ActorContext());
        }

        const auto &config = pqDescr.GetPQTabletConfig();
        auto consumerName = NPersQueue::ConvertNewConsumerName(Settings.Consumer, ActorContext());
        bool found = false;
        for (const auto& consumer : config.GetConsumers()) {
            if (consumerName == consumer.GetName()) {
                found = true;
                break;
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
    } else {
        Ydb::Scheme::Entry *selfEntry = Result.mutable_self();
        ConvertDirectoryEntry(response.Self->Info, selfEntry, true);
        if (const auto& name = GetCdcStreamName()) {
            selfEntry->set_name(*name);
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
    YDB_LOG_DEBUG_CTX(ctx, "Describe topic actor for path",
        {"path", GetProtoRequest()->path()});
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
}

void TPartitionsLocationActor::StateWork(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
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
    if (!record.GetStatus()) {
        this->RaiseError("Partition locations are not available", Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
            Ydb::StatusIds::UNAVAILABLE, ActorContext());
        return false;
    }
    for (auto i = 0u; i < record.LocationsSize(); i++) {
        const auto& part = record.GetLocations(i);
        TEvPQProxy::TPartitionLocationInfo partLocation;
        ui64 nodeId = part.GetNodeId();

        partLocation.PartitionId = part.GetPartitionId();
        partLocation.Generation = part.GetGeneration();
        partLocation.NodeId = nodeId;
        if (TopicPartitionsIds.contains(partLocation.PartitionId)) {
            Response->Partitions.emplace_back(std::move(partLocation));
        }
    }
    Finalize();
    return true;
}

void TPartitionsLocationActor::PassAway() {
    TDescribeTopicActorImpl::PassAway(ActorContext());
    TBase::PassAway();
}

void TPartitionsLocationActor::Finalize() {
    if (Settings.Partitions) {
        AFL_ENSURE(Response->Partitions.size() == Settings.Partitions.size())
            ("l", Response->Partitions.size())
            ("r", Settings.Partitions.size());
    } else {
        AFL_ENSURE(Response->Partitions.size() >= PQGroupInfo->Description.PartitionsSize())
            ("l", Response->Partitions.size())
            ("r", PQGroupInfo->Description.PartitionsSize());
    }
    TBase::RespondWithCode(Ydb::StatusIds::SUCCESS);
}

void TPartitionsLocationActor::RaiseError(const TString& error, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const Ydb::StatusIds::StatusCode status, const TActorContext&) {
    this->AddIssue(FillIssue(error, errorCode));
    this->RespondWithCode(status);
}

bool TPartitionsLocationActor::OnUnhandledException(const std::exception& exc) {
    YDB_LOG_ERROR("Unhandled exception",
        {"typeName", TypeName(exc)},
        {"exception", exc.what()},
        {"backTrace", TBackTrace::FromCurrentException().PrintToString()});

    this->RaiseError("Unhandled exception", Ydb::PersQueue::ErrorCode::ERROR, Ydb::StatusIds::UNAVAILABLE, ActorContext());

    return true;
}

} // namespace NKikimr::NGRpcProxy::V1

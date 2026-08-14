#include "schema_actors.h"

#include "persqueue_utils.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/obfuscate/obfuscate.h>

#include <library/cpp/json/json_writer.h>
#include <ydb/library/actors/core/log.h>

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
        settings->set_content_based_deduplication(config.GetContentBasedDeduplication());
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

        if (partConfig.HasReadSpeedInBytesPerSecond()) {
            settings->set_partition_total_read_speed_bytes_per_second(partConfig.GetReadSpeedInBytesPerSecond());
        }
        if (partConfig.HasReadSpeedInMessagesPerSecond()) {
            settings->set_partition_total_read_speed_messages_per_second(partConfig.GetReadSpeedInMessagesPerSecond());
        }

        // Read speed for reading a single partition without a consumer is stored in
        // TPartitionConfig.ReadQuota keyed by CLIENTID_WITHOUT_CONSUMER.
        if (const auto* readQuota = NPQ::GetReadQuota(config, NPQ::CLIENTID_WITHOUT_CONSUMER)) {
            if (readQuota->HasSpeedInBytesPerSecond()) {
                settings->set_partition_read_without_consumer_speed_bytes_per_second(readQuota->GetSpeedInBytesPerSecond());
            }
            if (readQuota->HasSpeedInMessagesPerSecond()) {
                settings->set_partition_read_without_consumer_speed_messages_per_second(readQuota->GetSpeedInMessagesPerSecond());
            }
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

            TString serviceType;
            TString serviceTypeError;
            if (!ResolveConsumerServiceType(consumer, pqConfig, true, serviceType, serviceTypeError)) {
                this->Request_->RaiseIssue(FillIssue(
                    serviceTypeError,
                    Ydb::PersQueue::ErrorCode::ERROR
                ));
                Reply(Ydb::StatusIds::INTERNAL_ERROR, ActorContext());
                return;
            }
            rr->set_service_type(serviceType);

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

            // Per-consumer read quota for a single partition is stored in TPartitionConfig.ReadQuota keyed by consumer name.
            if (const auto* readQuota = NPQ::GetReadQuota(config, consumer.GetName())) {
                if (readQuota->HasSpeedInBytesPerSecond()) {
                    rr->set_read_speed_bytes_per_second(readQuota->GetSpeedInBytesPerSecond());
                }
                if (readQuota->HasSpeedInMessagesPerSecond()) {
                    rr->set_read_speed_messages_per_second(readQuota->GetSpeedInMessagesPerSecond());
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
        HFuncCtx(TEvents::TEvWakeup, Handle, ctx);
        default: return false;
    }
    return true;
}

void TDescribeTopicActorImpl::PassAway(const TActorContext& ctx) {
    CancelRequestTimeout(ctx);
    for (auto& [_, tablet] : Tablets) {
        NTabletPipe::CloseClient(ctx, tablet.Pipe);
    }
}

void TDescribeTopicActorImpl::CancelRequestTimeout(const TActorContext& ctx) {
    if (TimeoutTimerActorId) {
        ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoison());
        TimeoutTimerActorId = {};
    }
}

TDuration TDescribeTopicActorImpl::RemainingRequestTimeout() const {
    if (!RequestStartTime) {
        return RequestTimeout;
    }
    const auto now = TAppData::TimeProvider->Now();
    if (now >= *RequestStartTime + RequestTimeout) {
        return TDuration::Zero();
    }
    return *RequestStartTime + RequestTimeout - now;
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

void TDescribeTopicActorImpl::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    TimeoutTimerActorId = {};
    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Request timed out",
        {"selfId", ctx.SelfID});
    RaiseError(
        "Describe topic request timed out",
        Ydb::PersQueue::ErrorCode::ERROR,
        Ydb::StatusIds::TIMEOUT,
        ctx
    );
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
        new TEvPersQueue::TEvGetPartitionsLocation(partsVector, RemainingRequestTimeout())
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
            LocationsBackoff.Reset();
            AFL_ENSURE(RequestsInfly > 0);
            --RequestsInfly;

            CheckCloseBalancerPipe(ctx);

            if (RequestsInfly == 0) {
                Reply(ctx);
            }
            return;
        }
    }

    if (!LocationsBackoff.HasMore()) {
        YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl PartitionsLocation retries exceeded",
            {"selfId", ctx.SelfID},
            {"response", record.DebugString()});
        return RaiseError(
            "Partition locations are not available",
            Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
            Ydb::StatusIds::UNAVAILABLE,
            ctx
        );
    }

    const auto delay = LocationsBackoff.Next();
    YDB_LOG_DEBUG_CTX(ctx, "DescribeTopicImpl Something wrong on location, retry",
        {"selfId", ctx.SelfID},
        {"iteration", LocationsBackoff.GetIteration()},
        {"delay", delay.ToString()},
        {"response", record.DebugString()});
    ctx.Schedule(delay, new TEvPQProxy::TEvRequestTablet(BalancerTabletId));
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

    RequestStartTime = TAppData::TimeProvider->Now();

    for (auto& pair : Tablets) {
        RequestTablet(pair.second, ctx);
    }

    if (RequestsInfly == 0) {
        Reply(ctx);
        return false;
    }

    TimeoutTimerActorId = CreateLongTimer(ctx, RequestTimeout,
        new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));
    return true;
}

} // namespace NKikimr::NGRpcProxy::V1

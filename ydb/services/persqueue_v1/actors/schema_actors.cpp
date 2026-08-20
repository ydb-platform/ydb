#include "schema_actors.h"

#include "persqueue_utils.h"

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

} // namespace NKikimr::NGRpcProxy::V1

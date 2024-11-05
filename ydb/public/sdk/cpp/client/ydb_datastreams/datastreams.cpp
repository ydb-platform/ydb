#include "datastreams.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb::NDataStreams::V1 {

    TPartitioningSettingsBuilder<TCreateStreamSettings> TCreateStreamSettings::BeginConfigurePartitioningSettings() {
        return { *this };
    }

    TPartitioningSettingsBuilder<TUpdateStreamSettings> TUpdateStreamSettings::BeginConfigurePartitioningSettings() {
        return { *this };
    }

    void SetPartitionSettings(const TPartitioningSettings& ps, ::Ydb::DataStreams::V1::PartitioningSettings* pt) {
        pt->set_max_active_partitions(ps.GetMaxActivePartitions());
        pt->set_min_active_partitions(ps.GetMinActivePartitions());

        ::Ydb::DataStreams::V1::AutoPartitioningStrategy strategy;
        switch (ps.GetAutoPartitioningSettings().GetStrategy()) {
            case EAutoPartitioningStrategy::Unspecified:
            case EAutoPartitioningStrategy::Disabled:
                strategy = ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED;
                break;
            case EAutoPartitioningStrategy::ScaleUp:
                strategy = ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP;
                break;
            case EAutoPartitioningStrategy::ScaleUpAndDown:
                strategy = ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN;
                break;
            case EAutoPartitioningStrategy::Paused:
                strategy = ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED;
                break;
        }

        pt->mutable_auto_partitioning_settings()->set_strategy(strategy);
        pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()
                ->mutable_stabilization_window()->set_seconds(ps.GetAutoPartitioningSettings().GetStabilizationWindow().Seconds());
        pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()
                ->set_up_utilization_percent(ps.GetAutoPartitioningSettings().GetUpUtilizationPercent());
        pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()
                ->set_down_utilization_percent(ps.GetAutoPartitioningSettings().GetDownUtilizationPercent());
    }

    class TDataStreamsClient::TImpl : public TClientImplCommon<TDataStreamsClient::TImpl> {
    public:
        TImpl(std::shared_ptr <TGRpcConnectionsImpl> &&connections, const TCommonClientSettings &settings)
                : TClientImplCommon(std::move(connections), settings) {}

        template<class TProtoResult, class TResultWrapper>
        auto MakeResultExtractor(NThreading::TPromise <TResultWrapper> promise) {
            return [promise = std::move(promise)]
                    (google::protobuf::Any *any, TPlainStatus status) mutable {
                std::unique_ptr <TProtoResult> result;
                if (any) {
                    result.reset(new TProtoResult);
                    any->UnpackTo(result.get());
                }

                promise.SetValue(
                        TResultWrapper(
                                TStatus(std::move(status)),
                                std::move(result)));
            };
        }

        template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TSettings, class TFillRequestFn, class TAsyncCall>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> CallImpl(const TSettings& settings, TAsyncCall grpcCall, TFillRequestFn fillRequest) {
            using TResultWrapper = TProtoResultWrapper<TProtoResult>;
            auto request = MakeOperationRequest<TProtoRequest>(settings);
            fillRequest(request);

            auto promise = NThreading::NewPromise<TResultWrapper>();
            auto future = promise.GetFuture();

            auto extractor = MakeResultExtractor<TProtoResult, TResultWrapper>(std::move(promise));

            Connections_->RunDeferred<TProtoService, TProtoRequest, TProtoResponse>(
                    std::move(request),
                    std::move(extractor),
                    grpcCall,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings));

            return future;

        }

        template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TSettings, class TAsyncCall>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> CallImpl(const TSettings& settings, TAsyncCall grpcCall) {
            return CallImpl<TProtoService, TProtoRequest, TProtoResponse, TProtoResult>(settings, grpcCall, [](TProtoRequest&) {});
        }

        TAsyncCreateStreamResult CreateStream(const TString &path, TCreateStreamSettings settings) {
            if (settings.RetentionPeriodHours_.Defined() && settings.RetentionStorageMegabytes_.Defined()) {
                return NThreading::MakeFuture(TProtoResultWrapper<Ydb::DataStreams::V1::CreateStreamResult>(
                    NYdb::TPlainStatus(NYdb::EStatus::BAD_REQUEST, "both retention types can not be set"),
                    std::make_unique<Ydb::DataStreams::V1::CreateStreamResult>()));
            }
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::CreateStreamRequest,
                    Ydb::DataStreams::V1::CreateStreamResponse,
                    Ydb::DataStreams::V1::CreateStreamResult>(settings,
                        &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncCreateStream,
                        [&](Ydb::DataStreams::V1::CreateStreamRequest& req) {
                            req.set_stream_name(path);
                            req.set_shard_count(settings.ShardCount_);
                            if (settings.RetentionStorageMegabytes_.Defined()) {
                                req.set_retention_storage_megabytes(*settings.RetentionStorageMegabytes_.Get());
                            } else if (settings.RetentionPeriodHours_.Defined()) {
                                req.set_retention_period_hours(*settings.RetentionPeriodHours_.Get());
                            } else {
                                req.set_retention_period_hours(24);
                            }
                            req.set_write_quota_kb_per_sec(settings.WriteQuotaKbPerSec_);
                            if (settings.StreamMode_.Defined()) {
                                req.mutable_stream_mode_details()->set_stream_mode(
                                        *settings.StreamMode_ == ESM_PROVISIONED ? Ydb::DataStreams::V1::StreamMode::PROVISIONED
                                                                                : Ydb::DataStreams::V1::StreamMode::ON_DEMAND);
                            }

                            if (settings.PartitioningSettings_.Defined()) {
                                SetPartitionSettings(*settings.PartitioningSettings_, req.mutable_partitioning_settings());
                            }
                        });
        }

        TAsyncListStreamsResult ListStreams(TListStreamsSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::ListStreamsRequest,
                    Ydb::DataStreams::V1::ListStreamsResponse,
                    Ydb::DataStreams::V1::ListStreamsResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListStreams,
                                                             [&](Ydb::DataStreams::V1::ListStreamsRequest& req) {
                                                                 req.set_exclusive_start_stream_name(settings.ExclusiveStartStreamName_);
                                                                 req.set_limit(settings.Limit_);
                                                                 req.set_recurse(settings.Recurse_);
                                                             });
        }

        TAsyncDescribeStreamResult DescribeStream(TDescribeStreamSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DescribeStreamRequest,
                    Ydb::DataStreams::V1::DescribeStreamResponse,
                    Ydb::DataStreams::V1::DescribeStreamResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream);
        }

        TAsyncListShardsResult ListShards(const TString &path,
            const Ydb::DataStreams::V1::ShardFilter& shardFilter,
            TListShardsSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::ListShardsRequest,
                    Ydb::DataStreams::V1::ListShardsResponse,
                    Ydb::DataStreams::V1::ListShardsResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListShards,
                    [&](Ydb::DataStreams::V1::ListShardsRequest& req) {
                        req.set_exclusive_start_shard_id(settings.ExclusiveStartShardId_);
                        req.set_max_results(settings.MaxResults_);
                        req.set_next_token(settings.NextToken_);
                        req.mutable_shard_filter()->CopyFrom(shardFilter);
                        req.set_stream_creation_timestamp(settings.StreamCreationTimestamp_);
                        req.set_stream_name(path);
                    });
        }

        TAsyncPutRecordsResult PutRecords(const TString& path, const std::vector<TDataRecord>& records, TPutRecordsSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::PutRecordsRequest,
                    Ydb::DataStreams::V1::PutRecordsResponse,
                    Ydb::DataStreams::V1::PutRecordsResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecords,
                                                            [&](Ydb::DataStreams::V1::PutRecordsRequest& req) {
                                                                req.set_stream_name(path);
                                                                for (const auto& record : records) {
                                                                    auto* protoRecord = req.add_records();
                                                                    protoRecord->set_partition_key(record.PartitionKey);
                                                                    protoRecord->set_data(record.Data);
                                                                    protoRecord->set_explicit_hash_key(record.ExplicitHashDecimal);
                                                                }
                                                            });
        }

        TAsyncGetRecordsResult GetRecords(const TString& shardIterator, TGetRecordsSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::GetRecordsRequest,
                    Ydb::DataStreams::V1::GetRecordsResponse,
                    Ydb::DataStreams::V1::GetRecordsResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncGetRecords,
                        [&](Ydb::DataStreams::V1::GetRecordsRequest& req) {
                            req.set_shard_iterator(shardIterator);
                            req.set_limit(settings.Limit_);
                        });
        }

        TAsyncGetShardIteratorResult GetShardIterator(const TString& path, const TString& shardId,
                                                      Ydb::DataStreams::V1::ShardIteratorType shardIteratorType,
                                                      TGetShardIteratorSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::GetShardIteratorRequest,
                    Ydb::DataStreams::V1::GetShardIteratorResponse,
                    Ydb::DataStreams::V1::GetShardIteratorResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncGetShardIterator,
                        [&](Ydb::DataStreams::V1::GetShardIteratorRequest& req) {
                            req.set_stream_name(path);
                            req.set_shard_id(shardId);
                            req.set_shard_iterator_type(shardIteratorType);
                            req.set_starting_sequence_number(settings.StartingSequenceNumber_);
                            req.set_timestamp(settings.Timestamp_);
                        });
        }

        /*TAsyncSubscribeToShardResult SubscribeToShard(TSubscribeToShardSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::SubscribeToShardRequest,
                    Ydb::DataStreams::V1::SubscribeToShardResponse,
                    Ydb::DataStreams::V1::SubscribeToShardResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncSubscribeToShard);
        }*/

        TAsyncDescribeLimitsResult DescribeLimits(TDescribeLimitsSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DescribeLimitsRequest,
                    Ydb::DataStreams::V1::DescribeLimitsResponse,
                    Ydb::DataStreams::V1::DescribeLimitsResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeLimits);
        }

        TAsyncDescribeStreamSummaryResult DescribeStreamSummary(const TString& path, TDescribeStreamSummarySettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DescribeStreamSummaryRequest,
                    Ydb::DataStreams::V1::DescribeStreamSummaryResponse,
                    Ydb::DataStreams::V1::DescribeStreamSummaryResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamSummary,
                                                                       [&](Ydb::DataStreams::V1::DescribeStreamSummaryRequest& req) {
                                                                           req.set_stream_name(path);
                                                                       });
        }

        TAsyncDecreaseStreamRetentionPeriodResult DecreaseStreamRetentionPeriod(const TString& path, TDecreaseStreamRetentionPeriodSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodRequest,
                    Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResponse,
                    Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDecreaseStreamRetentionPeriod,
                                                                               [&](Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodRequest& req) {
                                                                                   req.set_stream_name(path);
                                                                                   req.set_retention_period_hours(settings.RetentionPeriodHours_);
                                                                               });

        }

        TAsyncIncreaseStreamRetentionPeriodResult IncreaseStreamRetentionPeriod(const TString& path, TIncreaseStreamRetentionPeriodSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodRequest,
                    Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResponse,
                    Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncIncreaseStreamRetentionPeriod,
                                                                  [&](Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodRequest& req) {
                                                                      req.set_stream_name(path);
                                                                      req.set_retention_period_hours(settings.RetentionPeriodHours_);
                                                                  });

        }

        TAsyncUpdateShardCountResult UpdateShardCount(const TString& path, TUpdateShardCountSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::UpdateShardCountRequest,
                    Ydb::DataStreams::V1::UpdateShardCountResponse,
                    Ydb::DataStreams::V1::UpdateShardCountResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateShardCount,
                                                                  [&](Ydb::DataStreams::V1::UpdateShardCountRequest& req) {
                                                                      req.set_stream_name(path);
                                                                      req.set_target_shard_count(settings.TargetShardCount_);
                                                                  });
        }

        TAsyncUpdateStreamModeResult UpdateStreamMode(const TString& path, TUpdateStreamModeSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::UpdateStreamModeRequest,
                    Ydb::DataStreams::V1::UpdateStreamModeResponse,
                    Ydb::DataStreams::V1::UpdateStreamModeResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStreamMode,
                                                                  [&](Ydb::DataStreams::V1::UpdateStreamModeRequest& req) {
                                                                    req.set_stream_arn(path);

                                                                    req.mutable_stream_mode_details()->set_stream_mode(
                                                                        settings.StreamMode_ == ESM_PROVISIONED ? Ydb::DataStreams::V1::StreamMode::PROVISIONED
                                                                                                                : Ydb::DataStreams::V1::StreamMode::ON_DEMAND);
                                                                        });
        }

        TAsyncRegisterStreamConsumerResult RegisterStreamConsumer(const TString& path, const TString& consumer_name, TRegisterStreamConsumerSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::RegisterStreamConsumerRequest,
                    Ydb::DataStreams::V1::RegisterStreamConsumerResponse,
                    Ydb::DataStreams::V1::RegisterStreamConsumerResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncRegisterStreamConsumer,
                    [&](Ydb::DataStreams::V1::RegisterStreamConsumerRequest& req) {
                        req.set_stream_arn(path);
                        req.set_consumer_name(consumer_name);
                    });
        }

        TAsyncDeregisterStreamConsumerResult DeregisterStreamConsumer(const TString& path, const TString& consumer_name, TDeregisterStreamConsumerSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DeregisterStreamConsumerRequest,
                    Ydb::DataStreams::V1::DeregisterStreamConsumerResponse,
                    Ydb::DataStreams::V1::DeregisterStreamConsumerResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDeregisterStreamConsumer,
                    [&](Ydb::DataStreams::V1::DeregisterStreamConsumerRequest& req) {
                        req.set_stream_arn(path);
                        req.set_consumer_name(consumer_name);
                    });
        }

        TAsyncDescribeStreamConsumerResult DescribeStreamConsumer(TDescribeStreamConsumerSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DescribeStreamConsumerRequest,
                    Ydb::DataStreams::V1::DescribeStreamConsumerResponse,
                    Ydb::DataStreams::V1::DescribeStreamConsumerResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamConsumer);
        }

        TAsyncListStreamConsumersResult ListStreamConsumers(const TString& path, TListStreamConsumersSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::ListStreamConsumersRequest,
                    Ydb::DataStreams::V1::ListStreamConsumersResponse,
                    Ydb::DataStreams::V1::ListStreamConsumersResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListStreamConsumers, [&](Ydb::DataStreams::V1::ListStreamConsumersRequest& req) {
                        req.set_stream_arn(path);
                        req.set_next_token(settings.NextToken_);
                        req.set_max_results(settings.MaxResults_);
                    });
        }

        TAsyncAddTagsToStreamResult AddTagsToStream(TAddTagsToStreamSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::AddTagsToStreamRequest,
                    Ydb::DataStreams::V1::AddTagsToStreamResponse,
                    Ydb::DataStreams::V1::AddTagsToStreamResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncAddTagsToStream);
        }

        TAsyncDisableEnhancedMonitoringResult DisableEnhancedMonitoring(TDisableEnhancedMonitoringSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DisableEnhancedMonitoringRequest,
                    Ydb::DataStreams::V1::DisableEnhancedMonitoringResponse,
                    Ydb::DataStreams::V1::DisableEnhancedMonitoringResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDisableEnhancedMonitoring);
        }

        TAsyncEnableEnhancedMonitoringResult EnableEnhancedMonitoring(TEnableEnhancedMonitoringSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::EnableEnhancedMonitoringRequest,
                    Ydb::DataStreams::V1::EnableEnhancedMonitoringResponse,
                    Ydb::DataStreams::V1::EnableEnhancedMonitoringResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncEnableEnhancedMonitoring);
        }

        TAsyncListTagsForStreamResult ListTagsForStream(TListTagsForStreamSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::ListTagsForStreamRequest,
                    Ydb::DataStreams::V1::ListTagsForStreamResponse,
                    Ydb::DataStreams::V1::ListTagsForStreamResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListTagsForStream);
        }

        TAsyncMergeShardsResult MergeShards(TMergeShardsSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::MergeShardsRequest,
                    Ydb::DataStreams::V1::MergeShardsResponse,
                    Ydb::DataStreams::V1::MergeShardsResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncMergeShards);
        }

        TAsyncRemoveTagsFromStreamResult RemoveTagsFromStream(TRemoveTagsFromStreamSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::RemoveTagsFromStreamRequest,
                    Ydb::DataStreams::V1::RemoveTagsFromStreamResponse,
                    Ydb::DataStreams::V1::RemoveTagsFromStreamResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncRemoveTagsFromStream);
        }

        TAsyncSplitShardResult SplitShard(TSplitShardSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::SplitShardRequest,
                    Ydb::DataStreams::V1::SplitShardResponse,
                    Ydb::DataStreams::V1::SplitShardResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncSplitShard);
        }

        TAsyncStartStreamEncryptionResult StartStreamEncryption(TStartStreamEncryptionSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::StartStreamEncryptionRequest,
                    Ydb::DataStreams::V1::StartStreamEncryptionResponse,
                    Ydb::DataStreams::V1::StartStreamEncryptionResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncStartStreamEncryption);
        }

        TAsyncStopStreamEncryptionResult StopStreamEncryption(TStopStreamEncryptionSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::StopStreamEncryptionRequest,
                    Ydb::DataStreams::V1::StopStreamEncryptionResponse,
                    Ydb::DataStreams::V1::StopStreamEncryptionResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncStopStreamEncryption);
        }

        TAsyncUpdateStreamResult UpdateStream(const TString& streamName, TUpdateStreamSettings settings) {
            if (settings.RetentionPeriodHours_.Defined() && settings.RetentionStorageMegabytes_.Defined()) {
                return NThreading::MakeFuture(TProtoResultWrapper<Ydb::DataStreams::V1::UpdateStreamResult>(
                    NYdb::TPlainStatus(NYdb::EStatus::BAD_REQUEST, "both retention types can not be set"),
                    std::make_unique<Ydb::DataStreams::V1::UpdateStreamResult>()));
            }
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::UpdateStreamRequest,
                    Ydb::DataStreams::V1::UpdateStreamResponse,
                    Ydb::DataStreams::V1::UpdateStreamResult>(settings,
                        &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStream,
                        [&](Ydb::DataStreams::V1::UpdateStreamRequest& req) {
                            req.set_stream_name(streamName);
                            req.set_target_shard_count(settings.TargetShardCount_);
                            if (settings.RetentionPeriodHours_.Defined()) {
                                req.set_retention_period_hours(*settings.RetentionPeriodHours_.Get());
                            }
                            if (settings.RetentionStorageMegabytes_.Defined()) {
                                req.set_retention_storage_megabytes(*settings.RetentionStorageMegabytes_.Get());
                            }
                            req.set_write_quota_kb_per_sec(settings.WriteQuotaKbPerSec_);
                            if (settings.StreamMode_.Defined()) {
                                req.mutable_stream_mode_details()->set_stream_mode(
                                        *settings.StreamMode_ == ESM_PROVISIONED ? Ydb::DataStreams::V1::StreamMode::PROVISIONED
                                                                                : Ydb::DataStreams::V1::StreamMode::ON_DEMAND);
                            }

                            if (settings.PartitioningSettings_.Defined()) {
                                SetPartitionSettings(*settings.PartitioningSettings_, req.mutable_partitioning_settings());
                            }
                        });
        }

        TAsyncDeleteStreamResult DeleteStream(const TString &path, TDeleteStreamSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DeleteStreamRequest,
                    Ydb::DataStreams::V1::DeleteStreamResponse,
                    Ydb::DataStreams::V1::DeleteStreamResult>(settings,
                        &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDeleteStream,
                        [&](Ydb::DataStreams::V1::DeleteStreamRequest& req) {
                            req.set_stream_name(path);
                            req.set_enforce_consumer_deletion(settings.EnforceConsumerDeletion_);
                        });
        }

        TAsyncDescribeStreamResult DescribeStream(const TString &path, TDescribeStreamSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::DescribeStreamRequest,
                    Ydb::DataStreams::V1::DescribeStreamResponse,
                    Ydb::DataStreams::V1::DescribeStreamResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream,
                                                              [&](Ydb::DataStreams::V1::DescribeStreamRequest& req) {
                                                                  req.set_stream_name(path);
                                                                  req.set_exclusive_start_shard_id(settings.ExclusiveStartShardId_);
                                                                  req.set_limit(settings.Limit_);
                                                              });
        }

        TAsyncPutRecordResult PutRecord(const TString &path, const TDataRecord& record, TPutRecordSettings settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService,
                    Ydb::DataStreams::V1::PutRecordRequest,
                    Ydb::DataStreams::V1::PutRecordResponse,
                    Ydb::DataStreams::V1::PutRecordResult>(settings, &Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecord,
                                                                [&](Ydb::DataStreams::V1::PutRecordRequest& req) {
                                                                    req.set_stream_name(path);
                                                                    req.set_explicit_hash_key(record.ExplicitHashDecimal);
                                                                    req.set_partition_key(record.PartitionKey);
                                                                    req.set_data(record.Data);
                                                                });
        }

        template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> DoProtoRequest(const TProtoRequest& proto, TMethod method, const TProtoRequestSettings& settings) {
            return CallImpl<Ydb::DataStreams::V1::DataStreamsService, TProtoRequest, TProtoResponse, TProtoResult>(settings, method,
               [&](TProtoRequest& req) {
                    req.CopyFrom(proto);
               });
        }

    };

    TDataStreamsClient::TDataStreamsClient(const TDriver& driver, const TCommonClientSettings& settings)
            : Impl_(new TImpl(CreateInternalInterface(driver), settings))
    {}

    NThreading::TFuture<void> TDataStreamsClient::DiscoveryCompleted() {
        return Impl_->DiscoveryCompleted();
    }

    TAsyncCreateStreamResult TDataStreamsClient::CreateStream(const TString& path, TCreateStreamSettings settings) {
        return Impl_->CreateStream(path, settings);
    }

    TAsyncDeleteStreamResult TDataStreamsClient::DeleteStream(const TString& path, TDeleteStreamSettings settings) {
        return Impl_->DeleteStream(path, settings);
    }

    TAsyncDescribeStreamResult TDataStreamsClient::DescribeStream(const TString& path, TDescribeStreamSettings settings) {
        return Impl_->DescribeStream(path, settings);
    }

    TAsyncPutRecordResult TDataStreamsClient::PutRecord(const TString& path, const TDataRecord& record, TPutRecordSettings settings) {
        return Impl_->PutRecord(path, record, settings);
    }

    TAsyncListStreamsResult TDataStreamsClient::ListStreams(TListStreamsSettings settings) {
        return Impl_->ListStreams(settings);
    }

    TAsyncListShardsResult TDataStreamsClient::ListShards(const TString& path,
                                                          const Ydb::DataStreams::V1::ShardFilter& shardFilter,
                                                          TListShardsSettings settings) {
        return Impl_->ListShards(path, shardFilter, settings);
    }

    TAsyncPutRecordsResult TDataStreamsClient::PutRecords(const TString& path, const std::vector<TDataRecord>& records, TPutRecordsSettings settings) {
        return Impl_->PutRecords(path, records, settings);
    }

    TAsyncGetRecordsResult TDataStreamsClient::GetRecords(const TString& shardIterator, TGetRecordsSettings settings) {
        return Impl_->GetRecords(shardIterator, settings);
    }

    TAsyncGetShardIteratorResult TDataStreamsClient::GetShardIterator(const TString& path, const TString& shardId, Ydb::DataStreams::V1::ShardIteratorType shardIteratorType, TGetShardIteratorSettings settings) {
        return Impl_->GetShardIterator(path, shardId, shardIteratorType, settings);
    }

    /* TAsyncSubscribeToShardResult TDataStreamsClient::SubscribeToShard(TSubscribeToShardSettings settings) {
        return Impl_->SubscribeToShard(settings);
    } */

    TAsyncDescribeLimitsResult TDataStreamsClient::DescribeLimits(TDescribeLimitsSettings settings) {
        return Impl_->DescribeLimits(settings);
    }

    TAsyncDescribeStreamSummaryResult TDataStreamsClient::DescribeStreamSummary(const TString& path, TDescribeStreamSummarySettings settings) {
        return Impl_->DescribeStreamSummary(path, settings);
    }

    TAsyncDecreaseStreamRetentionPeriodResult TDataStreamsClient::DecreaseStreamRetentionPeriod(const TString& path, TDecreaseStreamRetentionPeriodSettings settings) {
        return Impl_->DecreaseStreamRetentionPeriod(path, settings);
    }

    TAsyncIncreaseStreamRetentionPeriodResult TDataStreamsClient::IncreaseStreamRetentionPeriod(const TString& path, TIncreaseStreamRetentionPeriodSettings settings) {
        return Impl_->IncreaseStreamRetentionPeriod(path, settings);
    }

    TAsyncUpdateShardCountResult TDataStreamsClient::UpdateShardCount(const TString& path, TUpdateShardCountSettings settings) {
        return Impl_->UpdateShardCount(path, settings);
    }

    TAsyncUpdateStreamModeResult TDataStreamsClient::UpdateStreamMode(const TString& path, TUpdateStreamModeSettings settings) {
        return Impl_->UpdateStreamMode(path, settings);
    }

    TAsyncRegisterStreamConsumerResult TDataStreamsClient::RegisterStreamConsumer(const TString& path, const TString& consumer_name, const TRegisterStreamConsumerSettings settings) {
        return Impl_->RegisterStreamConsumer(path, consumer_name, settings);
    }

    TAsyncDeregisterStreamConsumerResult TDataStreamsClient::DeregisterStreamConsumer(const TString& path, const TString& consumer_name, TDeregisterStreamConsumerSettings settings) {
        return Impl_->DeregisterStreamConsumer(path, consumer_name, settings);
    }

    TAsyncDescribeStreamConsumerResult TDataStreamsClient::DescribeStreamConsumer(TDescribeStreamConsumerSettings settings) {
        return Impl_->DescribeStreamConsumer(settings);
    }

    TAsyncListStreamConsumersResult TDataStreamsClient::ListStreamConsumers(const TString& path, TListStreamConsumersSettings settings) {
        return Impl_->ListStreamConsumers(path, settings);
    }

    TAsyncAddTagsToStreamResult TDataStreamsClient::AddTagsToStream(TAddTagsToStreamSettings settings) {
        return Impl_->AddTagsToStream(settings);
    }

    TAsyncDisableEnhancedMonitoringResult TDataStreamsClient::DisableEnhancedMonitoring(TDisableEnhancedMonitoringSettings settings) {
        return Impl_->DisableEnhancedMonitoring(settings);
    }

    TAsyncEnableEnhancedMonitoringResult TDataStreamsClient::EnableEnhancedMonitoring(TEnableEnhancedMonitoringSettings settings) {
        return Impl_->EnableEnhancedMonitoring(settings);
    }

    TAsyncListTagsForStreamResult TDataStreamsClient::ListTagsForStream(TListTagsForStreamSettings settings) {
        return Impl_->ListTagsForStream(settings);
    }

    TAsyncMergeShardsResult TDataStreamsClient::MergeShards(TMergeShardsSettings settings) {
        return Impl_->MergeShards(settings);
    }

    TAsyncRemoveTagsFromStreamResult TDataStreamsClient::RemoveTagsFromStream(TRemoveTagsFromStreamSettings settings) {
        return Impl_->RemoveTagsFromStream(settings);
    }

    TAsyncSplitShardResult TDataStreamsClient::SplitShard(TSplitShardSettings settings) {
        return Impl_->SplitShard(settings);
    }

    TAsyncStartStreamEncryptionResult TDataStreamsClient::StartStreamEncryption(TStartStreamEncryptionSettings settings) {
        return Impl_->StartStreamEncryption(settings);
    }

    TAsyncStopStreamEncryptionResult TDataStreamsClient::StopStreamEncryption(TStopStreamEncryptionSettings settings) {
        return Impl_->StopStreamEncryption(settings);
    }

    TAsyncUpdateStreamResult TDataStreamsClient::UpdateStream(const TString& streamName, TUpdateStreamSettings settings) {
        return Impl_->UpdateStream(streamName, settings);
    }

    template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
    NThreading::TFuture<TProtoResultWrapper<TProtoResult>> TDataStreamsClient::DoProtoRequest(const TProtoRequest& request, TMethod method, TProtoRequestSettings settings) {
        return Impl_->DoProtoRequest<TProtoRequest, TProtoResponse, TProtoResult, TMethod>(request, method, settings);
    }


    // Instantiate template protobuf methods
    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::PutRecordsResult>> TDataStreamsClient::DoProtoRequest
            <
                Ydb::DataStreams::V1::PutRecordsRequest,
                Ydb::DataStreams::V1::PutRecordsResponse,
                Ydb::DataStreams::V1::PutRecordsResult,
                decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecords)
            >(
                    const Ydb::DataStreams::V1::PutRecordsRequest& request,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecords) method,
                    TProtoRequestSettings settings
            );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::PutRecordResult>>TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::PutRecordRequest,
                    Ydb::DataStreams::V1::PutRecordResponse,
                    Ydb::DataStreams::V1::PutRecordResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecord)
                >(
                        const Ydb::DataStreams::V1::PutRecordRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecord) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::ListStreamsResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::ListStreamsRequest,
                    Ydb::DataStreams::V1::ListStreamsResponse,
                    Ydb::DataStreams::V1::ListStreamsResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListStreams)
                >(
                        const Ydb::DataStreams::V1::ListStreamsRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListStreams) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::CreateStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::CreateStreamRequest,
                    Ydb::DataStreams::V1::CreateStreamResponse,
                    Ydb::DataStreams::V1::CreateStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncCreateStream)
                >(
                        const Ydb::DataStreams::V1::CreateStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncCreateStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::UpdateStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::UpdateStreamRequest,
                    Ydb::DataStreams::V1::UpdateStreamResponse,
                    Ydb::DataStreams::V1::UpdateStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStream)
                >(
                        const Ydb::DataStreams::V1::UpdateStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DeleteStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DeleteStreamRequest,
                    Ydb::DataStreams::V1::DeleteStreamResponse,
                    Ydb::DataStreams::V1::DeleteStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDeleteStream)
                >(
                        const Ydb::DataStreams::V1::DeleteStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDeleteStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DescribeStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DescribeStreamRequest,
                    Ydb::DataStreams::V1::DescribeStreamResponse,
                    Ydb::DataStreams::V1::DescribeStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream)
                >(
                        const Ydb::DataStreams::V1::DescribeStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::ListShardsResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::ListShardsRequest,
                    Ydb::DataStreams::V1::ListShardsResponse,
                    Ydb::DataStreams::V1::ListShardsResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListShards)
                >(
                        const Ydb::DataStreams::V1::ListShardsRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListShards) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::GetRecordsResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::GetRecordsRequest,
                    Ydb::DataStreams::V1::GetRecordsResponse,
                    Ydb::DataStreams::V1::GetRecordsResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncGetRecords)
                >(
                        const Ydb::DataStreams::V1::GetRecordsRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncGetRecords) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::GetShardIteratorResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::GetShardIteratorRequest,
                    Ydb::DataStreams::V1::GetShardIteratorResponse,
                    Ydb::DataStreams::V1::GetShardIteratorResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncGetShardIterator)
                >(
                        const Ydb::DataStreams::V1::GetShardIteratorRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncGetShardIterator) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DescribeLimitsResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DescribeLimitsRequest,
                    Ydb::DataStreams::V1::DescribeLimitsResponse,
                    Ydb::DataStreams::V1::DescribeLimitsResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeLimits)
                >(
                        const Ydb::DataStreams::V1::DescribeLimitsRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeLimits) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodRequest,
                    Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResponse,
                    Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDecreaseStreamRetentionPeriod)
                >(
                        const Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDecreaseStreamRetentionPeriod) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodRequest,
                    Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResponse,
                    Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncIncreaseStreamRetentionPeriod)
                >(
                        const Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncIncreaseStreamRetentionPeriod) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::UpdateShardCountResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::UpdateShardCountRequest,
                    Ydb::DataStreams::V1::UpdateShardCountResponse,
                    Ydb::DataStreams::V1::UpdateShardCountResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateShardCount)
                >(
                        const Ydb::DataStreams::V1::UpdateShardCountRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateShardCount) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::UpdateStreamModeResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::UpdateStreamModeRequest,
                    Ydb::DataStreams::V1::UpdateStreamModeResponse,
                    Ydb::DataStreams::V1::UpdateStreamModeResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStreamMode)
                >(
                        const Ydb::DataStreams::V1::UpdateStreamModeRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStreamMode) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::RegisterStreamConsumerResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::RegisterStreamConsumerRequest,
                    Ydb::DataStreams::V1::RegisterStreamConsumerResponse,
                    Ydb::DataStreams::V1::RegisterStreamConsumerResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncRegisterStreamConsumer)
                >(
                        const Ydb::DataStreams::V1::RegisterStreamConsumerRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncRegisterStreamConsumer) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DeregisterStreamConsumerResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DeregisterStreamConsumerRequest,
                    Ydb::DataStreams::V1::DeregisterStreamConsumerResponse,
                    Ydb::DataStreams::V1::DeregisterStreamConsumerResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDeregisterStreamConsumer)
                >(
                        const Ydb::DataStreams::V1::DeregisterStreamConsumerRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDeregisterStreamConsumer) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DescribeStreamConsumerResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DescribeStreamConsumerRequest,
                    Ydb::DataStreams::V1::DescribeStreamConsumerResponse,
                    Ydb::DataStreams::V1::DescribeStreamConsumerResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamConsumer)
                >(
                        const Ydb::DataStreams::V1::DescribeStreamConsumerRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamConsumer) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::ListStreamConsumersResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::ListStreamConsumersRequest,
                    Ydb::DataStreams::V1::ListStreamConsumersResponse,
                    Ydb::DataStreams::V1::ListStreamConsumersResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListStreamConsumers)
                >(
                        const Ydb::DataStreams::V1::ListStreamConsumersRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListStreamConsumers) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::AddTagsToStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::AddTagsToStreamRequest,
                    Ydb::DataStreams::V1::AddTagsToStreamResponse,
                    Ydb::DataStreams::V1::AddTagsToStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncAddTagsToStream)
                >(
                        const Ydb::DataStreams::V1::AddTagsToStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncAddTagsToStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DisableEnhancedMonitoringResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::DisableEnhancedMonitoringRequest,
                    Ydb::DataStreams::V1::DisableEnhancedMonitoringResponse,
                    Ydb::DataStreams::V1::DisableEnhancedMonitoringResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDisableEnhancedMonitoring)
                >(
                        const Ydb::DataStreams::V1::DisableEnhancedMonitoringRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDisableEnhancedMonitoring) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::EnableEnhancedMonitoringResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::EnableEnhancedMonitoringRequest,
                    Ydb::DataStreams::V1::EnableEnhancedMonitoringResponse,
                    Ydb::DataStreams::V1::EnableEnhancedMonitoringResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncEnableEnhancedMonitoring)
                >(
                        const Ydb::DataStreams::V1::EnableEnhancedMonitoringRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncEnableEnhancedMonitoring) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::MergeShardsResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::MergeShardsRequest,
                    Ydb::DataStreams::V1::MergeShardsResponse,
                    Ydb::DataStreams::V1::MergeShardsResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncMergeShards)
                >(
                        const Ydb::DataStreams::V1::MergeShardsRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncMergeShards) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::ListTagsForStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::ListTagsForStreamRequest,
                    Ydb::DataStreams::V1::ListTagsForStreamResponse,
                    Ydb::DataStreams::V1::ListTagsForStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListTagsForStream)
                >(
                        const Ydb::DataStreams::V1::ListTagsForStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncListTagsForStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::RemoveTagsFromStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::RemoveTagsFromStreamRequest,
                    Ydb::DataStreams::V1::RemoveTagsFromStreamResponse,
                    Ydb::DataStreams::V1::RemoveTagsFromStreamResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncRemoveTagsFromStream)
                >(
                        const Ydb::DataStreams::V1::RemoveTagsFromStreamRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncRemoveTagsFromStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::SplitShardResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::SplitShardRequest,
                    Ydb::DataStreams::V1::SplitShardResponse,
                    Ydb::DataStreams::V1::SplitShardResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncSplitShard)
                >(
                        const Ydb::DataStreams::V1::SplitShardRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncSplitShard) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::StartStreamEncryptionResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::StartStreamEncryptionRequest,
                    Ydb::DataStreams::V1::StartStreamEncryptionResponse,
                    Ydb::DataStreams::V1::StartStreamEncryptionResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncStartStreamEncryption)
                >(
                        const Ydb::DataStreams::V1::StartStreamEncryptionRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncStartStreamEncryption) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::StopStreamEncryptionResult>> TDataStreamsClient::DoProtoRequest
                <
                    Ydb::DataStreams::V1::StopStreamEncryptionRequest,
                    Ydb::DataStreams::V1::StopStreamEncryptionResponse,
                    Ydb::DataStreams::V1::StopStreamEncryptionResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncStopStreamEncryption)
                >(
                        const Ydb::DataStreams::V1::StopStreamEncryptionRequest& request,
                        decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncStopStreamEncryption) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<Ydb::DataStreams::V1::DescribeStreamSummaryResult>> TDataStreamsClient::DoProtoRequest
            <
                    Ydb::DataStreams::V1::DescribeStreamSummaryRequest,
                    Ydb::DataStreams::V1::DescribeStreamSummaryResponse,
                    Ydb::DataStreams::V1::DescribeStreamSummaryResult,
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamSummary)
            >(
            const Ydb::DataStreams::V1::DescribeStreamSummaryRequest& request,
            decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamSummary) method,
            TProtoRequestSettings settings
    );
}

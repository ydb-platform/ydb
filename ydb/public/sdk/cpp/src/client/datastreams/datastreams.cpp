#include <ydb-cpp-sdk/client/datastreams/datastreams.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb-cpp-sdk/library/issue/yql_issue.h>
#include <src/library/issue/yql_issue_message.h>

#include <src/client/common_client/impl/client.h>

#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>

namespace NYdb::inline V3::NDataStreams::V1 {

    TPartitioningSettingsBuilder<TCreateStreamSettings> TCreateStreamSettings::BeginConfigurePartitioningSettings() {
        return { *this };
    }

    TPartitioningSettingsBuilder<TUpdateStreamSettings> TUpdateStreamSettings::BeginConfigurePartitioningSettings() {
        return { *this };
    }

    void SetPartitionSettings(const TPartitioningSettings& ps, ::NYdbProtos::DataStreams::V1::PartitioningSettings* pt) {
        pt->set_max_active_partitions(ps.GetMaxActivePartitions());
        pt->set_min_active_partitions(ps.GetMinActivePartitions());

        ::NYdbProtos::DataStreams::V1::AutoPartitioningStrategy strategy;
        switch (ps.GetAutoPartitioningSettings().GetStrategy()) {
            case EAutoPartitioningStrategy::Unspecified:
            case EAutoPartitioningStrategy::Disabled:
                strategy = ::NYdbProtos::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED;
                break;
            case EAutoPartitioningStrategy::ScaleUp:
                strategy = ::NYdbProtos::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP;
                break;
            case EAutoPartitioningStrategy::ScaleUpAndDown:
                strategy = ::NYdbProtos::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN;
                break;
            case EAutoPartitioningStrategy::Paused:
                strategy = ::NYdbProtos::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED;
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
                std::unique_ptr<TProtoResult> result;
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

        TAsyncCreateStreamResult CreateStream(const std::string &path, TCreateStreamSettings settings) {
            if (settings.RetentionPeriodHours_.has_value() && settings.RetentionStorageMegabytes_.has_value()) {
                return NThreading::MakeFuture(TProtoResultWrapper<NYdbProtos::DataStreams::V1::CreateStreamResult>(
                    NYdb::TPlainStatus(NYdb::EStatus::BAD_REQUEST, "both retention types can not be set"),
                    std::make_unique<NYdbProtos::DataStreams::V1::CreateStreamResult>()));
            }
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::CreateStreamRequest,
                    NYdbProtos::DataStreams::V1::CreateStreamResponse,
                    NYdbProtos::DataStreams::V1::CreateStreamResult>(settings,
                        &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncCreateStream,
                        [&](NYdbProtos::DataStreams::V1::CreateStreamRequest& req) {
                            req.set_stream_name(TStringType{path});
                            req.set_shard_count(settings.ShardCount_);
                            if (settings.RetentionStorageMegabytes_.has_value()) {
                                req.set_retention_storage_megabytes(*settings.RetentionStorageMegabytes_);
                            } else if (settings.RetentionPeriodHours_.has_value()) {
                                req.set_retention_period_hours(*settings.RetentionPeriodHours_);
                            } else {
                                req.set_retention_period_hours(24);
                            }
                            req.set_write_quota_kb_per_sec(settings.WriteQuotaKbPerSec_);
                            if (settings.StreamMode_.has_value()) {
                                req.mutable_stream_mode_details()->set_stream_mode(
                                        *settings.StreamMode_ == ESM_PROVISIONED ? NYdbProtos::DataStreams::V1::StreamMode::PROVISIONED
                                                                                : NYdbProtos::DataStreams::V1::StreamMode::ON_DEMAND);
                            }

                            if (settings.PartitioningSettings_.has_value()) {
                                SetPartitionSettings(*settings.PartitioningSettings_, req.mutable_partitioning_settings());
                            }
                        });
        }

        TAsyncListStreamsResult ListStreams(TListStreamsSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::ListStreamsRequest,
                    NYdbProtos::DataStreams::V1::ListStreamsResponse,
                    NYdbProtos::DataStreams::V1::ListStreamsResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListStreams,
                                                             [&](NYdbProtos::DataStreams::V1::ListStreamsRequest& req) {
                                                                 req.set_exclusive_start_stream_name(TStringType{settings.ExclusiveStartStreamName_});
                                                                 req.set_limit(settings.Limit_);
                                                                 req.set_recurse(settings.Recurse_);
                                                             });
        }

        TAsyncDescribeStreamResult DescribeStream(TDescribeStreamSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DescribeStreamRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream);
        }

        TAsyncListShardsResult ListShards(const std::string &path,
            const NYdbProtos::DataStreams::V1::ShardFilter& shardFilter,
            TListShardsSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::ListShardsRequest,
                    NYdbProtos::DataStreams::V1::ListShardsResponse,
                    NYdbProtos::DataStreams::V1::ListShardsResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListShards,
                    [&](NYdbProtos::DataStreams::V1::ListShardsRequest& req) {
                        req.set_exclusive_start_shard_id(TStringType{settings.ExclusiveStartShardId_});
                        req.set_max_results(settings.MaxResults_);
                        req.set_next_token(TStringType{settings.NextToken_});
                        req.mutable_shard_filter()->CopyFrom(shardFilter);
                        req.set_stream_creation_timestamp(settings.StreamCreationTimestamp_);
                        req.set_stream_name(TStringType{path});
                    });
        }

        TAsyncPutRecordsResult PutRecords(const std::string& path, const std::vector<TDataRecord>& records, TPutRecordsSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::PutRecordsRequest,
                    NYdbProtos::DataStreams::V1::PutRecordsResponse,
                    NYdbProtos::DataStreams::V1::PutRecordsResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecords,
                                                            [&](NYdbProtos::DataStreams::V1::PutRecordsRequest& req) {
                                                                req.set_stream_name(TStringType{path});
                                                                for (const auto& record : records) {
                                                                    auto* protoRecord = req.add_records();
                                                                    protoRecord->set_partition_key(TStringType{record.PartitionKey});
                                                                    protoRecord->set_data(TStringType{record.Data});
                                                                    protoRecord->set_explicit_hash_key(TStringType{record.ExplicitHashDecimal});
                                                                }
                                                            });
        }

        TAsyncGetRecordsResult GetRecords(const std::string& shardIterator, TGetRecordsSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::GetRecordsRequest,
                    NYdbProtos::DataStreams::V1::GetRecordsResponse,
                    NYdbProtos::DataStreams::V1::GetRecordsResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncGetRecords,
                        [&](NYdbProtos::DataStreams::V1::GetRecordsRequest& req) {
                            req.set_shard_iterator(TStringType{shardIterator});
                            req.set_limit(settings.Limit_);
                        });
        }

        TAsyncGetShardIteratorResult GetShardIterator(const std::string& path, const std::string& shardId,
                                                      NYdbProtos::DataStreams::V1::ShardIteratorType shardIteratorType,
                                                      TGetShardIteratorSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::GetShardIteratorRequest,
                    NYdbProtos::DataStreams::V1::GetShardIteratorResponse,
                    NYdbProtos::DataStreams::V1::GetShardIteratorResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncGetShardIterator,
                        [&](NYdbProtos::DataStreams::V1::GetShardIteratorRequest& req) {
                            req.set_stream_name(TStringType{path});
                            req.set_shard_id(TStringType{shardId});
                            req.set_shard_iterator_type(shardIteratorType);
                            req.set_starting_sequence_number(TStringType{settings.StartingSequenceNumber_});
                            req.set_timestamp(settings.Timestamp_);
                        });
        }

        /*TAsyncSubscribeToShardResult SubscribeToShard(TSubscribeToShardSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::SubscribeToShardRequest,
                    NYdbProtos::DataStreams::V1::SubscribeToShardResponse,
                    NYdbProtos::DataStreams::V1::SubscribeToShardResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncSubscribeToShard);
        }*/

        TAsyncDescribeLimitsResult DescribeLimits(TDescribeLimitsSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DescribeLimitsRequest,
                    NYdbProtos::DataStreams::V1::DescribeLimitsResponse,
                    NYdbProtos::DataStreams::V1::DescribeLimitsResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeLimits);
        }

        TAsyncDescribeStreamSummaryResult DescribeStreamSummary(const std::string& path, TDescribeStreamSummarySettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DescribeStreamSummaryRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamSummaryResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamSummaryResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamSummary,
                                                                       [&](NYdbProtos::DataStreams::V1::DescribeStreamSummaryRequest& req) {
                                                                           req.set_stream_name(TStringType{path});
                                                                       });
        }

        TAsyncDecreaseStreamRetentionPeriodResult DecreaseStreamRetentionPeriod(const std::string& path, TDecreaseStreamRetentionPeriodSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodRequest,
                    NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodResponse,
                    NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDecreaseStreamRetentionPeriod,
                                                                               [&](NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodRequest& req) {
                                                                                   req.set_stream_name(TStringType{path});
                                                                                   req.set_retention_period_hours(settings.RetentionPeriodHours_);
                                                                               });

        }

        TAsyncIncreaseStreamRetentionPeriodResult IncreaseStreamRetentionPeriod(const std::string& path, TIncreaseStreamRetentionPeriodSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodRequest,
                    NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodResponse,
                    NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncIncreaseStreamRetentionPeriod,
                                                                  [&](NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodRequest& req) {
                                                                      req.set_stream_name(TStringType{path});
                                                                      req.set_retention_period_hours(settings.RetentionPeriodHours_);
                                                                  });

        }

        TAsyncUpdateShardCountResult UpdateShardCount(const std::string& path, TUpdateShardCountSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::UpdateShardCountRequest,
                    NYdbProtos::DataStreams::V1::UpdateShardCountResponse,
                    NYdbProtos::DataStreams::V1::UpdateShardCountResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateShardCount,
                                                                  [&](NYdbProtos::DataStreams::V1::UpdateShardCountRequest& req) {
                                                                      req.set_stream_name(TStringType{path});
                                                                      req.set_target_shard_count(settings.TargetShardCount_);
                                                                  });
        }

        TAsyncUpdateStreamModeResult UpdateStreamMode(const std::string& path, TUpdateStreamModeSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::UpdateStreamModeRequest,
                    NYdbProtos::DataStreams::V1::UpdateStreamModeResponse,
                    NYdbProtos::DataStreams::V1::UpdateStreamModeResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStreamMode,
                                                                  [&](NYdbProtos::DataStreams::V1::UpdateStreamModeRequest& req) {
                                                                    req.set_stream_arn(TStringType{path});

                                                                    req.mutable_stream_mode_details()->set_stream_mode(
                                                                        settings.StreamMode_ == ESM_PROVISIONED ? NYdbProtos::DataStreams::V1::StreamMode::PROVISIONED
                                                                                                                : NYdbProtos::DataStreams::V1::StreamMode::ON_DEMAND);
                                                                        });
        }

        TAsyncRegisterStreamConsumerResult RegisterStreamConsumer(const std::string& path, const std::string& consumer_name, TRegisterStreamConsumerSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::RegisterStreamConsumerRequest,
                    NYdbProtos::DataStreams::V1::RegisterStreamConsumerResponse,
                    NYdbProtos::DataStreams::V1::RegisterStreamConsumerResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncRegisterStreamConsumer,
                    [&](NYdbProtos::DataStreams::V1::RegisterStreamConsumerRequest& req) {
                        req.set_stream_arn(TStringType{path});
                        req.set_consumer_name(TStringType{consumer_name});
                    });
        }

        TAsyncDeregisterStreamConsumerResult DeregisterStreamConsumer(const std::string& path, const std::string& consumer_name, TDeregisterStreamConsumerSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DeregisterStreamConsumerRequest,
                    NYdbProtos::DataStreams::V1::DeregisterStreamConsumerResponse,
                    NYdbProtos::DataStreams::V1::DeregisterStreamConsumerResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDeregisterStreamConsumer,
                    [&](NYdbProtos::DataStreams::V1::DeregisterStreamConsumerRequest& req) {
                        req.set_stream_arn(TStringType{path});
                        req.set_consumer_name(TStringType{consumer_name});
                    });
        }

        TAsyncDescribeStreamConsumerResult DescribeStreamConsumer(TDescribeStreamConsumerSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DescribeStreamConsumerRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamConsumerResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamConsumerResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamConsumer);
        }

        TAsyncListStreamConsumersResult ListStreamConsumers(const std::string& path, TListStreamConsumersSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::ListStreamConsumersRequest,
                    NYdbProtos::DataStreams::V1::ListStreamConsumersResponse,
                    NYdbProtos::DataStreams::V1::ListStreamConsumersResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListStreamConsumers, [&](NYdbProtos::DataStreams::V1::ListStreamConsumersRequest& req) {
                        req.set_stream_arn(TStringType{path});
                        req.set_next_token(TStringType{settings.NextToken_});
                        req.set_max_results(settings.MaxResults_);
                    });
        }

        TAsyncAddTagsToStreamResult AddTagsToStream(TAddTagsToStreamSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::AddTagsToStreamRequest,
                    NYdbProtos::DataStreams::V1::AddTagsToStreamResponse,
                    NYdbProtos::DataStreams::V1::AddTagsToStreamResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncAddTagsToStream);
        }

        TAsyncDisableEnhancedMonitoringResult DisableEnhancedMonitoring(TDisableEnhancedMonitoringSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringRequest,
                    NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringResponse,
                    NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDisableEnhancedMonitoring);
        }

        TAsyncEnableEnhancedMonitoringResult EnableEnhancedMonitoring(TEnableEnhancedMonitoringSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringRequest,
                    NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringResponse,
                    NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncEnableEnhancedMonitoring);
        }

        TAsyncListTagsForStreamResult ListTagsForStream(TListTagsForStreamSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::ListTagsForStreamRequest,
                    NYdbProtos::DataStreams::V1::ListTagsForStreamResponse,
                    NYdbProtos::DataStreams::V1::ListTagsForStreamResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListTagsForStream);
        }

        TAsyncMergeShardsResult MergeShards(TMergeShardsSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::MergeShardsRequest,
                    NYdbProtos::DataStreams::V1::MergeShardsResponse,
                    NYdbProtos::DataStreams::V1::MergeShardsResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncMergeShards);
        }

        TAsyncRemoveTagsFromStreamResult RemoveTagsFromStream(TRemoveTagsFromStreamSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::RemoveTagsFromStreamRequest,
                    NYdbProtos::DataStreams::V1::RemoveTagsFromStreamResponse,
                    NYdbProtos::DataStreams::V1::RemoveTagsFromStreamResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncRemoveTagsFromStream);
        }

        TAsyncSplitShardResult SplitShard(TSplitShardSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::SplitShardRequest,
                    NYdbProtos::DataStreams::V1::SplitShardResponse,
                    NYdbProtos::DataStreams::V1::SplitShardResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncSplitShard);
        }

        TAsyncStartStreamEncryptionResult StartStreamEncryption(TStartStreamEncryptionSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::StartStreamEncryptionRequest,
                    NYdbProtos::DataStreams::V1::StartStreamEncryptionResponse,
                    NYdbProtos::DataStreams::V1::StartStreamEncryptionResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncStartStreamEncryption);
        }

        TAsyncStopStreamEncryptionResult StopStreamEncryption(TStopStreamEncryptionSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::StopStreamEncryptionRequest,
                    NYdbProtos::DataStreams::V1::StopStreamEncryptionResponse,
                    NYdbProtos::DataStreams::V1::StopStreamEncryptionResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncStopStreamEncryption);
        }

        TAsyncUpdateStreamResult UpdateStream(const std::string& streamName, TUpdateStreamSettings settings) {
            if (settings.RetentionPeriodHours_.has_value() && settings.RetentionStorageMegabytes_.has_value()) {
                return NThreading::MakeFuture(TProtoResultWrapper<NYdbProtos::DataStreams::V1::UpdateStreamResult>(
                    NYdb::TPlainStatus(NYdb::EStatus::BAD_REQUEST, "both retention types can not be set"),
                    std::make_unique<NYdbProtos::DataStreams::V1::UpdateStreamResult>()));
            }
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::UpdateStreamRequest,
                    NYdbProtos::DataStreams::V1::UpdateStreamResponse,
                    NYdbProtos::DataStreams::V1::UpdateStreamResult>(settings,
                        &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStream,
                        [&](NYdbProtos::DataStreams::V1::UpdateStreamRequest& req) {
                            req.set_stream_name(TStringType{streamName});
                            req.set_target_shard_count(settings.TargetShardCount_);
                            if (settings.RetentionPeriodHours_.has_value()) {
                                req.set_retention_period_hours(*settings.RetentionPeriodHours_);
                            }
                            if (settings.RetentionStorageMegabytes_.has_value()) {
                                req.set_retention_storage_megabytes(*settings.RetentionStorageMegabytes_);
                            }
                            req.set_write_quota_kb_per_sec(settings.WriteQuotaKbPerSec_);
                            if (settings.StreamMode_.has_value()) {
                                req.mutable_stream_mode_details()->set_stream_mode(
                                        *settings.StreamMode_ == ESM_PROVISIONED ? NYdbProtos::DataStreams::V1::StreamMode::PROVISIONED
                                                                                : NYdbProtos::DataStreams::V1::StreamMode::ON_DEMAND);
                            }

                            if (settings.PartitioningSettings_.has_value()) {
                                SetPartitionSettings(*settings.PartitioningSettings_, req.mutable_partitioning_settings());
                            }
                        });
        }

        TAsyncDeleteStreamResult DeleteStream(const std::string &path, TDeleteStreamSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DeleteStreamRequest,
                    NYdbProtos::DataStreams::V1::DeleteStreamResponse,
                    NYdbProtos::DataStreams::V1::DeleteStreamResult>(settings,
                        &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDeleteStream,
                        [&](NYdbProtos::DataStreams::V1::DeleteStreamRequest& req) {
                            req.set_stream_name(TStringType{path});
                            req.set_enforce_consumer_deletion(settings.EnforceConsumerDeletion_);
                        });
        }

        TAsyncDescribeStreamResult DescribeStream(const std::string &path, TDescribeStreamSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::DescribeStreamRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream,
                                                              [&](NYdbProtos::DataStreams::V1::DescribeStreamRequest& req) {
                                                                  req.set_stream_name(TStringType{path});
                                                                  req.set_exclusive_start_shard_id(TStringType{settings.ExclusiveStartShardId_});
                                                                  req.set_limit(settings.Limit_);
                                                              });
        }

        TAsyncPutRecordResult PutRecord(const std::string &path, const TDataRecord& record, TPutRecordSettings settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService,
                    NYdbProtos::DataStreams::V1::PutRecordRequest,
                    NYdbProtos::DataStreams::V1::PutRecordResponse,
                    NYdbProtos::DataStreams::V1::PutRecordResult>(settings, &NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecord,
                                                                [&](NYdbProtos::DataStreams::V1::PutRecordRequest& req) {
                                                                    req.set_stream_name(TStringType{path});
                                                                    req.set_explicit_hash_key(TStringType{record.ExplicitHashDecimal});
                                                                    req.set_partition_key(TStringType{record.PartitionKey});
                                                                    req.set_data(TStringType{record.Data});
                                                                });
        }

        template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> DoProtoRequest(const TProtoRequest& proto, TMethod method, const TProtoRequestSettings& settings) {
            return CallImpl<NYdbProtos::DataStreams::V1::DataStreamsService, TProtoRequest, TProtoResponse, TProtoResult>(settings, method,
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

    TAsyncCreateStreamResult TDataStreamsClient::CreateStream(const std::string& path, TCreateStreamSettings settings) {
        return Impl_->CreateStream(path, settings);
    }

    TAsyncDeleteStreamResult TDataStreamsClient::DeleteStream(const std::string& path, TDeleteStreamSettings settings) {
        return Impl_->DeleteStream(path, settings);
    }

    TAsyncDescribeStreamResult TDataStreamsClient::DescribeStream(const std::string& path, TDescribeStreamSettings settings) {
        return Impl_->DescribeStream(path, settings);
    }

    TAsyncPutRecordResult TDataStreamsClient::PutRecord(const std::string& path, const TDataRecord& record, TPutRecordSettings settings) {
        return Impl_->PutRecord(path, record, settings);
    }

    TAsyncListStreamsResult TDataStreamsClient::ListStreams(TListStreamsSettings settings) {
        return Impl_->ListStreams(settings);
    }

    TAsyncListShardsResult TDataStreamsClient::ListShards(const std::string& path,
                                                          const NYdbProtos::DataStreams::V1::ShardFilter& shardFilter,
                                                          TListShardsSettings settings) {
        return Impl_->ListShards(path, shardFilter, settings);
    }

    TAsyncPutRecordsResult TDataStreamsClient::PutRecords(const std::string& path, const std::vector<TDataRecord>& records, TPutRecordsSettings settings) {
        return Impl_->PutRecords(path, records, settings);
    }

    TAsyncGetRecordsResult TDataStreamsClient::GetRecords(const std::string& shardIterator, TGetRecordsSettings settings) {
        return Impl_->GetRecords(shardIterator, settings);
    }

    TAsyncGetShardIteratorResult TDataStreamsClient::GetShardIterator(const std::string& path, const std::string& shardId, NYdbProtos::DataStreams::V1::ShardIteratorType shardIteratorType, TGetShardIteratorSettings settings) {
        return Impl_->GetShardIterator(path, shardId, shardIteratorType, settings);
    }

    /* TAsyncSubscribeToShardResult TDataStreamsClient::SubscribeToShard(TSubscribeToShardSettings settings) {
        return Impl_->SubscribeToShard(settings);
    } */

    TAsyncDescribeLimitsResult TDataStreamsClient::DescribeLimits(TDescribeLimitsSettings settings) {
        return Impl_->DescribeLimits(settings);
    }

    TAsyncDescribeStreamSummaryResult TDataStreamsClient::DescribeStreamSummary(const std::string& path, TDescribeStreamSummarySettings settings) {
        return Impl_->DescribeStreamSummary(path, settings);
    }

    TAsyncDecreaseStreamRetentionPeriodResult TDataStreamsClient::DecreaseStreamRetentionPeriod(const std::string& path, TDecreaseStreamRetentionPeriodSettings settings) {
        return Impl_->DecreaseStreamRetentionPeriod(path, settings);
    }

    TAsyncIncreaseStreamRetentionPeriodResult TDataStreamsClient::IncreaseStreamRetentionPeriod(const std::string& path, TIncreaseStreamRetentionPeriodSettings settings) {
        return Impl_->IncreaseStreamRetentionPeriod(path, settings);
    }

    TAsyncUpdateShardCountResult TDataStreamsClient::UpdateShardCount(const std::string& path, TUpdateShardCountSettings settings) {
        return Impl_->UpdateShardCount(path, settings);
    }

    TAsyncUpdateStreamModeResult TDataStreamsClient::UpdateStreamMode(const std::string& path, TUpdateStreamModeSettings settings) {
        return Impl_->UpdateStreamMode(path, settings);
    }

    TAsyncRegisterStreamConsumerResult TDataStreamsClient::RegisterStreamConsumer(const std::string& path, const std::string& consumer_name, const TRegisterStreamConsumerSettings settings) {
        return Impl_->RegisterStreamConsumer(path, consumer_name, settings);
    }

    TAsyncDeregisterStreamConsumerResult TDataStreamsClient::DeregisterStreamConsumer(const std::string& path, const std::string& consumer_name, TDeregisterStreamConsumerSettings settings) {
        return Impl_->DeregisterStreamConsumer(path, consumer_name, settings);
    }

    TAsyncDescribeStreamConsumerResult TDataStreamsClient::DescribeStreamConsumer(TDescribeStreamConsumerSettings settings) {
        return Impl_->DescribeStreamConsumer(settings);
    }

    TAsyncListStreamConsumersResult TDataStreamsClient::ListStreamConsumers(const std::string& path, TListStreamConsumersSettings settings) {
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

    TAsyncUpdateStreamResult TDataStreamsClient::UpdateStream(const std::string& streamName, TUpdateStreamSettings settings) {
        return Impl_->UpdateStream(streamName, settings);
    }

    template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
    NThreading::TFuture<TProtoResultWrapper<TProtoResult>> TDataStreamsClient::DoProtoRequest(const TProtoRequest& request, TMethod method, TProtoRequestSettings settings) {
        return Impl_->DoProtoRequest<TProtoRequest, TProtoResponse, TProtoResult, TMethod>(request, method, settings);
    }


    // Instantiate template protobuf methods
    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::PutRecordsResult>> TDataStreamsClient::DoProtoRequest
            <
                NYdbProtos::DataStreams::V1::PutRecordsRequest,
                NYdbProtos::DataStreams::V1::PutRecordsResponse,
                NYdbProtos::DataStreams::V1::PutRecordsResult,
                decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecords)
            >(
                    const NYdbProtos::DataStreams::V1::PutRecordsRequest& request,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecords) method,
                    TProtoRequestSettings settings
            );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::PutRecordResult>>TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::PutRecordRequest,
                    NYdbProtos::DataStreams::V1::PutRecordResponse,
                    NYdbProtos::DataStreams::V1::PutRecordResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecord)
                >(
                        const NYdbProtos::DataStreams::V1::PutRecordRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncPutRecord) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::ListStreamsResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::ListStreamsRequest,
                    NYdbProtos::DataStreams::V1::ListStreamsResponse,
                    NYdbProtos::DataStreams::V1::ListStreamsResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListStreams)
                >(
                        const NYdbProtos::DataStreams::V1::ListStreamsRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListStreams) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::CreateStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::CreateStreamRequest,
                    NYdbProtos::DataStreams::V1::CreateStreamResponse,
                    NYdbProtos::DataStreams::V1::CreateStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncCreateStream)
                >(
                        const NYdbProtos::DataStreams::V1::CreateStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncCreateStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::UpdateStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::UpdateStreamRequest,
                    NYdbProtos::DataStreams::V1::UpdateStreamResponse,
                    NYdbProtos::DataStreams::V1::UpdateStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStream)
                >(
                        const NYdbProtos::DataStreams::V1::UpdateStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DeleteStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DeleteStreamRequest,
                    NYdbProtos::DataStreams::V1::DeleteStreamResponse,
                    NYdbProtos::DataStreams::V1::DeleteStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDeleteStream)
                >(
                        const NYdbProtos::DataStreams::V1::DeleteStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDeleteStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DescribeStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DescribeStreamRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream)
                >(
                        const NYdbProtos::DataStreams::V1::DescribeStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::ListShardsResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::ListShardsRequest,
                    NYdbProtos::DataStreams::V1::ListShardsResponse,
                    NYdbProtos::DataStreams::V1::ListShardsResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListShards)
                >(
                        const NYdbProtos::DataStreams::V1::ListShardsRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListShards) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::GetRecordsResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::GetRecordsRequest,
                    NYdbProtos::DataStreams::V1::GetRecordsResponse,
                    NYdbProtos::DataStreams::V1::GetRecordsResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncGetRecords)
                >(
                        const NYdbProtos::DataStreams::V1::GetRecordsRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncGetRecords) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::GetShardIteratorResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::GetShardIteratorRequest,
                    NYdbProtos::DataStreams::V1::GetShardIteratorResponse,
                    NYdbProtos::DataStreams::V1::GetShardIteratorResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncGetShardIterator)
                >(
                        const NYdbProtos::DataStreams::V1::GetShardIteratorRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncGetShardIterator) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DescribeLimitsResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DescribeLimitsRequest,
                    NYdbProtos::DataStreams::V1::DescribeLimitsResponse,
                    NYdbProtos::DataStreams::V1::DescribeLimitsResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeLimits)
                >(
                        const NYdbProtos::DataStreams::V1::DescribeLimitsRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeLimits) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodRequest,
                    NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodResponse,
                    NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDecreaseStreamRetentionPeriod)
                >(
                        const NYdbProtos::DataStreams::V1::DecreaseStreamRetentionPeriodRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDecreaseStreamRetentionPeriod) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodRequest,
                    NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodResponse,
                    NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncIncreaseStreamRetentionPeriod)
                >(
                        const NYdbProtos::DataStreams::V1::IncreaseStreamRetentionPeriodRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncIncreaseStreamRetentionPeriod) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::UpdateShardCountResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::UpdateShardCountRequest,
                    NYdbProtos::DataStreams::V1::UpdateShardCountResponse,
                    NYdbProtos::DataStreams::V1::UpdateShardCountResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateShardCount)
                >(
                        const NYdbProtos::DataStreams::V1::UpdateShardCountRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateShardCount) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::UpdateStreamModeResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::UpdateStreamModeRequest,
                    NYdbProtos::DataStreams::V1::UpdateStreamModeResponse,
                    NYdbProtos::DataStreams::V1::UpdateStreamModeResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStreamMode)
                >(
                        const NYdbProtos::DataStreams::V1::UpdateStreamModeRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncUpdateStreamMode) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::RegisterStreamConsumerResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::RegisterStreamConsumerRequest,
                    NYdbProtos::DataStreams::V1::RegisterStreamConsumerResponse,
                    NYdbProtos::DataStreams::V1::RegisterStreamConsumerResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncRegisterStreamConsumer)
                >(
                        const NYdbProtos::DataStreams::V1::RegisterStreamConsumerRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncRegisterStreamConsumer) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DeregisterStreamConsumerResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DeregisterStreamConsumerRequest,
                    NYdbProtos::DataStreams::V1::DeregisterStreamConsumerResponse,
                    NYdbProtos::DataStreams::V1::DeregisterStreamConsumerResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDeregisterStreamConsumer)
                >(
                        const NYdbProtos::DataStreams::V1::DeregisterStreamConsumerRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDeregisterStreamConsumer) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DescribeStreamConsumerResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DescribeStreamConsumerRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamConsumerResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamConsumerResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamConsumer)
                >(
                        const NYdbProtos::DataStreams::V1::DescribeStreamConsumerRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamConsumer) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::ListStreamConsumersResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::ListStreamConsumersRequest,
                    NYdbProtos::DataStreams::V1::ListStreamConsumersResponse,
                    NYdbProtos::DataStreams::V1::ListStreamConsumersResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListStreamConsumers)
                >(
                        const NYdbProtos::DataStreams::V1::ListStreamConsumersRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListStreamConsumers) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::AddTagsToStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::AddTagsToStreamRequest,
                    NYdbProtos::DataStreams::V1::AddTagsToStreamResponse,
                    NYdbProtos::DataStreams::V1::AddTagsToStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncAddTagsToStream)
                >(
                        const NYdbProtos::DataStreams::V1::AddTagsToStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncAddTagsToStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringRequest,
                    NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringResponse,
                    NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDisableEnhancedMonitoring)
                >(
                        const NYdbProtos::DataStreams::V1::DisableEnhancedMonitoringRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDisableEnhancedMonitoring) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringRequest,
                    NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringResponse,
                    NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncEnableEnhancedMonitoring)
                >(
                        const NYdbProtos::DataStreams::V1::EnableEnhancedMonitoringRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncEnableEnhancedMonitoring) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::MergeShardsResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::MergeShardsRequest,
                    NYdbProtos::DataStreams::V1::MergeShardsResponse,
                    NYdbProtos::DataStreams::V1::MergeShardsResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncMergeShards)
                >(
                        const NYdbProtos::DataStreams::V1::MergeShardsRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncMergeShards) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::ListTagsForStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::ListTagsForStreamRequest,
                    NYdbProtos::DataStreams::V1::ListTagsForStreamResponse,
                    NYdbProtos::DataStreams::V1::ListTagsForStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListTagsForStream)
                >(
                        const NYdbProtos::DataStreams::V1::ListTagsForStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncListTagsForStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::RemoveTagsFromStreamResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::RemoveTagsFromStreamRequest,
                    NYdbProtos::DataStreams::V1::RemoveTagsFromStreamResponse,
                    NYdbProtos::DataStreams::V1::RemoveTagsFromStreamResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncRemoveTagsFromStream)
                >(
                        const NYdbProtos::DataStreams::V1::RemoveTagsFromStreamRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncRemoveTagsFromStream) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::SplitShardResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::SplitShardRequest,
                    NYdbProtos::DataStreams::V1::SplitShardResponse,
                    NYdbProtos::DataStreams::V1::SplitShardResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncSplitShard)
                >(
                        const NYdbProtos::DataStreams::V1::SplitShardRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncSplitShard) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::StartStreamEncryptionResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::StartStreamEncryptionRequest,
                    NYdbProtos::DataStreams::V1::StartStreamEncryptionResponse,
                    NYdbProtos::DataStreams::V1::StartStreamEncryptionResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncStartStreamEncryption)
                >(
                        const NYdbProtos::DataStreams::V1::StartStreamEncryptionRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncStartStreamEncryption) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::StopStreamEncryptionResult>> TDataStreamsClient::DoProtoRequest
                <
                    NYdbProtos::DataStreams::V1::StopStreamEncryptionRequest,
                    NYdbProtos::DataStreams::V1::StopStreamEncryptionResponse,
                    NYdbProtos::DataStreams::V1::StopStreamEncryptionResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncStopStreamEncryption)
                >(
                        const NYdbProtos::DataStreams::V1::StopStreamEncryptionRequest& request,
                        decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncStopStreamEncryption) method,
                        TProtoRequestSettings settings
                );

    template NThreading::TFuture<TProtoResultWrapper<NYdbProtos::DataStreams::V1::DescribeStreamSummaryResult>> TDataStreamsClient::DoProtoRequest
            <
                    NYdbProtos::DataStreams::V1::DescribeStreamSummaryRequest,
                    NYdbProtos::DataStreams::V1::DescribeStreamSummaryResponse,
                    NYdbProtos::DataStreams::V1::DescribeStreamSummaryResult,
                    decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamSummary)
            >(
            const NYdbProtos::DataStreams::V1::DescribeStreamSummaryRequest& request,
            decltype(&NYdbProtos::DataStreams::V1::DataStreamsService::Stub::AsyncDescribeStreamSummary) method,
            TProtoRequestSettings settings
    );
}

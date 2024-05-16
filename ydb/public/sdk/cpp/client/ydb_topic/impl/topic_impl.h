#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

namespace NYdb::NTopic {

struct TOffsetsRange {
    ui64 Start;
    ui64 End;
};

struct TPartitionOffsets {
    ui64 PartitionId;
    TVector<TOffsetsRange> Offsets;
};

struct TTopicOffsets {
    TString Path;
    TVector<TPartitionOffsets> Partitions;
};

struct TUpdateOffsetsInTransactionSettings : public TOperationRequestSettings<TUpdateOffsetsInTransactionSettings> {
    using TOperationRequestSettings<TUpdateOffsetsInTransactionSettings>::TOperationRequestSettings;
};

class TTopicClient::TImpl : public TClientImplCommon<TTopicClient::TImpl> {
public:
    // Constructor for main client.
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TTopicClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings(settings)
    {
    }

    template <class TSettings>
    static void ConvertConsumerToProto(const TConsumerSettings<TSettings>& settings, Ydb::Topic::Consumer& consumerProto) {
        consumerProto.set_name(settings.ConsumerName_);
        consumerProto.set_important(settings.Important_);
        consumerProto.mutable_read_from()->set_seconds(settings.ReadFrom_.Seconds());

        for (const auto& codec : settings.SupportedCodecs_) {
            consumerProto.mutable_supported_codecs()->add_codecs((static_cast<Ydb::Topic::Codec>(codec)));
        }
        for (auto& pair : settings.Attributes_) {
            (*consumerProto.mutable_attributes())[pair.first] = pair.second;
        }
    }

    static void ConvertAlterConsumerToProto(const TAlterConsumerSettings& settings, Ydb::Topic::AlterConsumer& consumerProto) {
        consumerProto.set_name(settings.ConsumerName_);
        if (settings.SetImportant_)
            consumerProto.set_set_important(*settings.SetImportant_);
        if (settings.SetReadFrom_)
            consumerProto.mutable_set_read_from()->set_seconds(settings.SetReadFrom_->Seconds());

        if (settings.SetSupportedCodecs_) {
            for (const auto& codec : *settings.SetSupportedCodecs_) {
                consumerProto.mutable_set_supported_codecs()->add_codecs((static_cast<Ydb::Topic::Codec>(codec)));
            }
        }

        for (auto& pair : settings.AlterAttributes_) {
            (*consumerProto.mutable_alter_attributes())[pair.first] = pair.second;
        }
    }


    static Ydb::Topic::CreateTopicRequest MakePropsCreateRequest(const TString& path, const TCreateTopicSettings& settings) {
        Ydb::Topic::CreateTopicRequest request = MakeOperationRequest<Ydb::Topic::CreateTopicRequest>(settings);
        request.set_path(path);

        request.mutable_partitioning_settings()->set_min_active_partitions(settings.PartitioningSettings_.GetMinActivePartitions());
        request.mutable_partitioning_settings()->set_partition_count_limit(settings.PartitioningSettings_.GetPartitionCountLimit());
        request.mutable_partitioning_settings()->set_max_active_partitions(settings.PartitioningSettings_.GetMaxActivePartitions());
        request.mutable_partitioning_settings()->mutable_autoscaling_settings()->set_strategy(static_cast<Ydb::Topic::AutoscalingStrategy>(settings.PartitioningSettings_.GetAutoscalingSettings().GetStrategy()));
        request.mutable_partitioning_settings()->mutable_autoscaling_settings()->mutable_partition_write_speed()->mutable_threshold_time()->set_seconds(settings.PartitioningSettings_.GetAutoscalingSettings().GetThresholdTime().Seconds());
        request.mutable_partitioning_settings()->mutable_autoscaling_settings()->mutable_partition_write_speed()->set_scale_up_threshold_percent(settings.PartitioningSettings_.GetAutoscalingSettings().GetScaleUpThresholdPercent());
        request.mutable_partitioning_settings()->mutable_autoscaling_settings()->mutable_partition_write_speed()->set_scale_down_threshold_percent(settings.PartitioningSettings_.GetAutoscalingSettings().GetScaleDownThresholdPercent());

        request.mutable_retention_period()->set_seconds(settings.RetentionPeriod_.Seconds());

        for (const auto& codec : settings.SupportedCodecs_) {
            request.mutable_supported_codecs()->add_codecs((static_cast<Ydb::Topic::Codec>(codec)));
        }
        request.set_partition_write_speed_bytes_per_second(settings.PartitionWriteSpeedBytesPerSecond_);
        request.set_partition_write_burst_bytes(settings.PartitionWriteBurstBytes_);
        request.set_retention_storage_mb(settings.RetentionStorageMb_);
        request.set_metering_mode(TProtoAccessor::GetProto(settings.MeteringMode_));

        for (auto& pair : settings.Attributes_) {
            (*request.mutable_attributes())[pair.first] = pair.second;
        }

        for (const auto& consumer : settings.Consumers_) {
            Ydb::Topic::Consumer& consumerProto = *request.add_consumers();
            ConvertConsumerToProto(consumer, consumerProto);
        }

        return request;
    }

    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings) {
        auto request = MakePropsCreateRequest(path, settings);

        return RunSimple<Ydb::Topic::V1::TopicService, Ydb::Topic::CreateTopicRequest, Ydb::Topic::CreateTopicResponse>(
            std::move(request),
            &Ydb::Topic::V1::TopicService::Stub::AsyncCreateTopic,
            TRpcRequestSettings::Make(settings));
    }


    static Ydb::Topic::AlterTopicRequest MakePropsAlterRequest(const TString& path, const TAlterTopicSettings& settings) {
        Ydb::Topic::AlterTopicRequest request = MakeOperationRequest<Ydb::Topic::AlterTopicRequest>(settings);
        request.set_path(path);

        if (settings.AlterPartitioningSettings_) {
            if (settings.AlterPartitioningSettings_->MinActivePartitions_) {
                request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(*settings.AlterPartitioningSettings_->MinActivePartitions_);
            }
            if (settings.AlterPartitioningSettings_->MaxActivePartitions_) {
                request.mutable_alter_partitioning_settings()->set_set_max_active_partitions(*settings.AlterPartitioningSettings_->MaxActivePartitions_);
            }
            if (settings.AlterPartitioningSettings_->AutoscalingSettings_) {
                if (settings.AlterPartitioningSettings_->AutoscalingSettings_->Strategy_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_autoscaling_settings()->set_set_strategy(static_cast<Ydb::Topic::AutoscalingStrategy>(*settings.AlterPartitioningSettings_->AutoscalingSettings_->Strategy_));
                }
                if (settings.AlterPartitioningSettings_->AutoscalingSettings_->ScaleDownThresholdPercent_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_autoscaling_settings()->mutable_set_partition_write_speed()->set_set_scale_down_threshold_percent(*settings.AlterPartitioningSettings_->AutoscalingSettings_->ScaleDownThresholdPercent_);
                }
                if (settings.AlterPartitioningSettings_->AutoscalingSettings_->ScaleUpThresholdPercent_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_autoscaling_settings()->mutable_set_partition_write_speed()->set_set_scale_up_threshold_percent(*settings.AlterPartitioningSettings_->AutoscalingSettings_->ScaleUpThresholdPercent_);
                }
                if (settings.AlterPartitioningSettings_->AutoscalingSettings_->ThresholdTime_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_autoscaling_settings()->mutable_set_partition_write_speed()->mutable_set_threshold_time()->set_seconds(settings.AlterPartitioningSettings_->AutoscalingSettings_->ThresholdTime_->Seconds());
                }
            }
        }
        if (settings.SetRetentionPeriod_) {
            request.mutable_set_retention_period()->set_seconds(settings.SetRetentionPeriod_->Seconds());
        }
        if (settings.SetSupportedCodecs_) {
            for (const auto& codec : *settings.SetSupportedCodecs_) {
                request.mutable_set_supported_codecs()->add_codecs((static_cast<Ydb::Topic::Codec>(codec)));
            }
        }
        if (settings.SetPartitionWriteSpeedBytesPerSecond_) {
            request.set_set_partition_write_speed_bytes_per_second(*settings.SetPartitionWriteSpeedBytesPerSecond_);
        }
        if (settings.SetPartitionWriteBurstBytes_) {
            request.set_set_partition_write_burst_bytes(*settings.SetPartitionWriteBurstBytes_);
        }
        if (settings.SetRetentionStorageMb_) {
            request.set_set_retention_storage_mb(*settings.SetRetentionStorageMb_);
        }
        if (settings.SetMeteringMode_) {
            request.set_set_metering_mode(TProtoAccessor::GetProto(*settings.SetMeteringMode_));
        }

        for (auto& pair : settings.AlterAttributes_) {
            (*request.mutable_alter_attributes())[pair.first] = pair.second;
        }

        for (const auto& consumer : settings.AddConsumers_) {
            Ydb::Topic::Consumer& consumerProto = *request.add_add_consumers();
            ConvertConsumerToProto(consumer, consumerProto);
        }

        for (const auto& consumer : settings.DropConsumers_) {
            request.add_drop_consumers(consumer);
        }

        for (const auto& consumer : settings.AlterConsumers_) {
            Ydb::Topic::AlterConsumer& consumerProto = *request.add_alter_consumers();
            ConvertAlterConsumerToProto(consumer, consumerProto);
        }

        return request;
    }


    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& settings) {
        auto request = MakePropsAlterRequest(path, settings);

        return RunSimple<Ydb::Topic::V1::TopicService, Ydb::Topic::AlterTopicRequest, Ydb::Topic::AlterTopicResponse>(
            std::move(request),
            &Ydb::Topic::V1::TopicService::Stub::AsyncAlterTopic,
            TRpcRequestSettings::Make(settings));
    }


    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Topic::DropTopicRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::Topic::V1::TopicService, Ydb::Topic::DropTopicRequest, Ydb::Topic::DropTopicResponse>(
            std::move(request),
            &Ydb::Topic::V1::TopicService::Stub::AsyncDropTopic,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Topic::DescribeTopicRequest>(settings);
        request.set_path(path);

        if (settings.IncludeStats_) {
            request.set_include_stats(true);
        }

        if (settings.IncludeLocation_) {
            request.set_include_location(true);
        }

        auto promise = NThreading::NewPromise<TDescribeTopicResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Topic::DescribeTopicResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeTopicResult val(TStatus(std::move(status)), std::move(result));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Topic::V1::TopicService, Ydb::Topic::DescribeTopicRequest, Ydb::Topic::DescribeTopicResponse>(
            std::move(request),
            extractor,
            &Ydb::Topic::V1::TopicService::Stub::AsyncDescribeTopic,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Topic::DescribeConsumerRequest>(settings);
        request.set_path(path);
        request.set_consumer(consumer);

        if (settings.IncludeStats_) {
            request.set_include_stats(true);
        }

        if (settings.IncludeLocation_) {
            request.set_include_location(true);
        }

        auto promise = NThreading::NewPromise<TDescribeConsumerResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Topic::DescribeConsumerResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeConsumerResult val(TStatus(std::move(status)), std::move(result));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Topic::V1::TopicService, Ydb::Topic::DescribeConsumerRequest, Ydb::Topic::DescribeConsumerResponse>(
            std::move(request),
            extractor,
            &Ydb::Topic::V1::TopicService::Stub::AsyncDescribeConsumer,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const TDescribePartitionSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Topic::DescribePartitionRequest>(settings);
        request.set_path(path);
        request.set_partition_id(partitionId);

        if (settings.IncludeStats_) {
            request.set_include_stats(true);
        }

        if (settings.IncludeLocation_) {
            request.set_include_location(true);
        }

        auto promise = NThreading::NewPromise<TDescribePartitionResult>();

        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Topic::DescribePartitionResult result;
            if (any) {
                any->UnpackTo(&result);
            }

            TDescribePartitionResult val(TStatus(std::move(status)), std::move(result));
            promise.SetValue(std::move(val));
        };

        Connections_->RunDeferred<Ydb::Topic::V1::TopicService, Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            std::move(request),
            extractor,
            &Ydb::Topic::V1::TopicService::Stub::AsyncDescribePartition,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
        const TCommitOffsetSettings& settings) {
        Ydb::Topic::CommitOffsetRequest request = MakeOperationRequest<Ydb::Topic::CommitOffsetRequest>(settings);
        request.set_path(path);
        request.set_partition_id(partitionId);
        request.set_consumer(consumerName);
        request.set_offset(offset);

        return RunSimple<Ydb::Topic::V1::TopicService, Ydb::Topic::CommitOffsetRequest, Ydb::Topic::CommitOffsetResponse>(
            std::move(request),
            &Ydb::Topic::V1::TopicService::Stub::AsyncCommitOffset,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus UpdateOffsetsInTransaction(const NTable::TTransaction& tx,
                                            const TVector<TTopicOffsets>& topics,
                                            const TString& consumerName,
                                            const TUpdateOffsetsInTransactionSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Topic::UpdateOffsetsInTransactionRequest>(settings);

        request.mutable_tx()->set_id(tx.GetId());
        request.mutable_tx()->set_session(tx.GetSession().GetId());

        for (auto& t : topics) {
            auto* topic = request.mutable_topics()->Add();
            topic->set_path(t.Path);

            for (auto& p : t.Partitions) {
                auto* partition = topic->mutable_partitions()->Add();
                partition->set_partition_id(p.PartitionId);

                for (auto& r : p.Offsets) {
                    auto *range = partition->mutable_partition_offsets()->Add();
                    range->set_start(r.Start);
                    range->set_end(r.End);
                }
            }
        }

        request.set_consumer(consumerName);

        return RunSimple<Ydb::Topic::V1::TopicService, Ydb::Topic::UpdateOffsetsInTransactionRequest, Ydb::Topic::UpdateOffsetsInTransactionResponse>(
            std::move(request),
            &Ydb::Topic::V1::TopicService::Stub::AsyncUpdateOffsetsInTransaction,
            TRpcRequestSettings::Make(settings)
        );
    }

    // Runtime API.
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings);
    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleWriteSession(const TWriteSessionSettings& settings);
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings);

    using IReadSessionConnectionProcessorFactory =
        ISessionConnectionProcessorFactory<Ydb::Topic::StreamReadMessage::FromClient,
                                           Ydb::Topic::StreamReadMessage::FromServer>;

    std::shared_ptr<IReadSessionConnectionProcessorFactory> CreateReadSessionConnectionProcessorFactory();

    using IWriteSessionConnectionProcessorFactory =
        ISessionConnectionProcessorFactory<Ydb::Topic::StreamWriteMessage::FromClient,
                                           Ydb::Topic::StreamWriteMessage::FromServer>;

    std::shared_ptr<IWriteSessionConnectionProcessorFactory> CreateWriteSessionConnectionProcessorFactory();

    NYdbGrpc::IQueueClientContextPtr CreateContext() {
        return Connections_->CreateContext();
    }

private:
    const TTopicClientSettings Settings;
    TAdaptiveLock Lock;
};

} // namespace NYdb::NTopic

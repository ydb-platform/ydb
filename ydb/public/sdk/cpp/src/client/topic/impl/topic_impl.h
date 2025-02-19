#pragma once

#include "transaction.h"

#include <src/client/topic/impl/common.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <src/client/common_client/impl/client.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

namespace NYdb::inline V3::NTopic {
struct TOffsetsRange {
    ui64 Start;
    ui64 End;
};

struct TPartitionOffsets {
    ui64 PartitionId;
    std::vector<TOffsetsRange> Offsets;
};

struct TTopicOffsets {
    std::string Path;
    std::vector<TPartitionOffsets> Partitions;
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

    static void ConvertAlterConsumerToProto(const TAlterConsumerSettings& settings, NYdbProtos::Topic::AlterConsumer& consumerProto) {
        consumerProto.set_name(TStringType{settings.ConsumerName_});
        if (settings.SetImportant_)
            consumerProto.set_set_important(*settings.SetImportant_);
        if (settings.SetReadFrom_)
            consumerProto.mutable_set_read_from()->set_seconds(settings.SetReadFrom_->Seconds());

        if (settings.SetSupportedCodecs_) {
            for (const auto& codec : *settings.SetSupportedCodecs_) {
                consumerProto.mutable_set_supported_codecs()->add_codecs((static_cast<NYdbProtos::Topic::Codec>(codec)));
            }
        }

        for (auto& pair : settings.AlterAttributes_) {
            (*consumerProto.mutable_alter_attributes())[pair.first] = pair.second;
        }
    }


    static NYdbProtos::Topic::CreateTopicRequest MakePropsCreateRequest(const std::string& path, const TCreateTopicSettings& settings) {
        NYdbProtos::Topic::CreateTopicRequest request = MakeOperationRequest<NYdbProtos::Topic::CreateTopicRequest>(settings);
        request.set_path(TStringType{path});
        settings.SerializeTo(request);
        return request;
    }

    TAsyncStatus CreateTopic(const std::string& path, const TCreateTopicSettings& settings) {
        auto request = MakePropsCreateRequest(path, settings);

        return RunSimple<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::CreateTopicRequest, NYdbProtos::Topic::CreateTopicResponse>(
            std::move(request),
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncCreateTopic,
            TRpcRequestSettings::Make(settings));
    }


    static NYdbProtos::Topic::AlterTopicRequest MakePropsAlterRequest(const std::string& path, const TAlterTopicSettings& settings) {
        NYdbProtos::Topic::AlterTopicRequest request = MakeOperationRequest<NYdbProtos::Topic::AlterTopicRequest>(settings);
        request.set_path(TStringType{path});

        if (settings.AlterPartitioningSettings_) {
            if (settings.AlterPartitioningSettings_->MinActivePartitions_) {
                request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(*settings.AlterPartitioningSettings_->MinActivePartitions_);
            }
            if (settings.AlterPartitioningSettings_->MaxActivePartitions_) {
                request.mutable_alter_partitioning_settings()->set_set_max_active_partitions(*settings.AlterPartitioningSettings_->MaxActivePartitions_);
            }
            if (settings.AlterPartitioningSettings_->AutoPartitioningSettings_) {
                if (settings.AlterPartitioningSettings_->AutoPartitioningSettings_->Strategy_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_auto_partitioning_settings()->set_set_strategy(static_cast<NYdbProtos::Topic::AutoPartitioningStrategy>(*settings.AlterPartitioningSettings_->AutoPartitioningSettings_->Strategy_));
                }
                if (settings.AlterPartitioningSettings_->AutoPartitioningSettings_->DownUtilizationPercent_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_auto_partitioning_settings()->mutable_set_partition_write_speed()->set_set_down_utilization_percent(*settings.AlterPartitioningSettings_->AutoPartitioningSettings_->DownUtilizationPercent_);
                }
                if (settings.AlterPartitioningSettings_->AutoPartitioningSettings_->UpUtilizationPercent_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_auto_partitioning_settings()->mutable_set_partition_write_speed()->set_set_up_utilization_percent(*settings.AlterPartitioningSettings_->AutoPartitioningSettings_->UpUtilizationPercent_);
                }
                if (settings.AlterPartitioningSettings_->AutoPartitioningSettings_->StabilizationWindow_) {
                    request.mutable_alter_partitioning_settings()->mutable_alter_auto_partitioning_settings()->mutable_set_partition_write_speed()->mutable_set_stabilization_window()->set_seconds(settings.AlterPartitioningSettings_->AutoPartitioningSettings_->StabilizationWindow_->Seconds());
                }
            }
        }
        if (settings.SetRetentionPeriod_) {
            request.mutable_set_retention_period()->set_seconds(settings.SetRetentionPeriod_->Seconds());
        }
        if (settings.SetSupportedCodecs_) {
            for (const auto& codec : *settings.SetSupportedCodecs_) {
                request.mutable_set_supported_codecs()->add_codecs((static_cast<NYdbProtos::Topic::Codec>(codec)));
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
            consumer.SerializeTo(*request.add_add_consumers());
        }

        for (const auto& consumer : settings.DropConsumers_) {
            request.add_drop_consumers(TStringType{consumer});
        }

        for (const auto& consumer : settings.AlterConsumers_) {
            NYdbProtos::Topic::AlterConsumer& consumerProto = *request.add_alter_consumers();
            ConvertAlterConsumerToProto(consumer, consumerProto);
        }

        return request;
    }


    TAsyncStatus AlterTopic(const std::string& path, const TAlterTopicSettings& settings) {
        auto request = MakePropsAlterRequest(path, settings);

        return RunSimple<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::AlterTopicRequest, NYdbProtos::Topic::AlterTopicResponse>(
            std::move(request),
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncAlterTopic,
            TRpcRequestSettings::Make(settings));
    }


    TAsyncStatus DropTopic(const std::string& path, const TDropTopicSettings& settings) {
        auto request = MakeOperationRequest<NYdbProtos::Topic::DropTopicRequest>(settings);
        request.set_path(TStringType{path});

        return RunSimple<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::DropTopicRequest, NYdbProtos::Topic::DropTopicResponse>(
            std::move(request),
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncDropTopic,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribeTopicResult DescribeTopic(const std::string& path, const TDescribeTopicSettings& settings) {
        auto request = MakeOperationRequest<NYdbProtos::Topic::DescribeTopicRequest>(settings);
        request.set_path(TStringType{path});

        if (settings.IncludeStats_) {
            request.set_include_stats(true);
        }

        if (settings.IncludeLocation_) {
            request.set_include_location(true);
        }

        auto promise = NThreading::NewPromise<TDescribeTopicResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                NYdbProtos::Topic::DescribeTopicResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeTopicResult val(TStatus(std::move(status)), std::move(result));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::DescribeTopicRequest, NYdbProtos::Topic::DescribeTopicResponse>(
            std::move(request),
            extractor,
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncDescribeTopic,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribeConsumerResult DescribeConsumer(const std::string& path, const std::string& consumer, const TDescribeConsumerSettings& settings) {
        auto request = MakeOperationRequest<NYdbProtos::Topic::DescribeConsumerRequest>(settings);
        request.set_path(TStringType{path});
        request.set_consumer(TStringType{consumer});

        if (settings.IncludeStats_) {
            request.set_include_stats(true);
        }

        if (settings.IncludeLocation_) {
            request.set_include_location(true);
        }

        auto promise = NThreading::NewPromise<TDescribeConsumerResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                NYdbProtos::Topic::DescribeConsumerResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeConsumerResult val(TStatus(std::move(status)), std::move(result));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::DescribeConsumerRequest, NYdbProtos::Topic::DescribeConsumerResponse>(
            std::move(request),
            extractor,
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncDescribeConsumer,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribePartitionResult DescribePartition(const std::string& path, i64 partitionId, const TDescribePartitionSettings& settings) {
        auto request = MakeOperationRequest<NYdbProtos::Topic::DescribePartitionRequest>(settings);
        request.set_path(TStringType{path});
        request.set_partition_id(partitionId);

        if (settings.IncludeStats_) {
            request.set_include_stats(true);
        }

        if (settings.IncludeLocation_) {
            request.set_include_location(true);
        }

        auto promise = NThreading::NewPromise<TDescribePartitionResult>();

        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            NYdbProtos::Topic::DescribePartitionResult result;
            if (any) {
                any->UnpackTo(&result);
            }

            TDescribePartitionResult val(TStatus(std::move(status)), std::move(result));
            promise.SetValue(std::move(val));
        };

        Connections_->RunDeferred<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::DescribePartitionRequest, NYdbProtos::Topic::DescribePartitionResponse>(
            std::move(request),
            extractor,
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncDescribePartition,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus CommitOffset(const std::string& path, ui64 partitionId, const std::string& consumerName, ui64 offset,
        const TCommitOffsetSettings& settings) {
        NYdbProtos::Topic::CommitOffsetRequest request = MakeOperationRequest<NYdbProtos::Topic::CommitOffsetRequest>(settings);
        request.set_path(TStringType{path});
        request.set_partition_id(partitionId);
        request.set_consumer(TStringType{consumerName});
        request.set_offset(offset);

        return RunSimple<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::CommitOffsetRequest, NYdbProtos::Topic::CommitOffsetResponse>(
            std::move(request),
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncCommitOffset,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus UpdateOffsetsInTransaction(const TTransactionId& tx,
                                            const std::vector<TTopicOffsets>& topics,
                                            const std::string& consumerName,
                                            const TUpdateOffsetsInTransactionSettings& settings)
    {
        auto request = MakeOperationRequest<NYdbProtos::Topic::UpdateOffsetsInTransactionRequest>(settings);

        request.mutable_tx()->set_id(TStringType{GetTxId(tx)});
        request.mutable_tx()->set_session(TStringType{GetSessionId(tx)});

        for (auto& t : topics) {
            auto* topic = request.mutable_topics()->Add();
            topic->set_path(TStringType{t.Path});

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

        request.set_consumer(TStringType{consumerName});

        return RunSimple<NYdbProtos::Topic::V1::TopicService, NYdbProtos::Topic::UpdateOffsetsInTransactionRequest, NYdbProtos::Topic::UpdateOffsetsInTransactionResponse>(
            std::move(request),
            &NYdbProtos::Topic::V1::TopicService::Stub::AsyncUpdateOffsetsInTransaction,
            TRpcRequestSettings::Make(settings)
        );
    }

    // Runtime API.
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings);
    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleWriteSession(const TWriteSessionSettings& settings);
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings);

    using IReadSessionConnectionProcessorFactory =
        ISessionConnectionProcessorFactory<NYdbProtos::Topic::StreamReadMessage::FromClient,
                                           NYdbProtos::Topic::StreamReadMessage::FromServer>;

    std::shared_ptr<IReadSessionConnectionProcessorFactory> CreateReadSessionConnectionProcessorFactory();

    using IWriteSessionConnectionProcessorFactory =
        ISessionConnectionProcessorFactory<NYdbProtos::Topic::StreamWriteMessage::FromClient,
                                           NYdbProtos::Topic::StreamWriteMessage::FromServer>;

    std::shared_ptr<IWriteSessionConnectionProcessorFactory> CreateWriteSessionConnectionProcessorFactory();

    NYdbGrpc::IQueueClientContextPtr CreateContext() {
        return Connections_->CreateContext();
    }

private:
    const TTopicClientSettings Settings;
    TAdaptiveLock Lock;
};

} // namespace NYdb::NTopic

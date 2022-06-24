#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <ydb/public/api/grpc/draft/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb::NTopic {

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

        request.mutable_retention_period()->set_seconds(settings.RetentionPeriod_.Seconds());

        for (const auto& codec : settings.SupportedCodecs_) {
            request.mutable_supported_codecs()->add_codecs((static_cast<Ydb::Topic::Codec>(codec)));
        }
        request.set_partition_write_speed_bytes_per_second(settings.PartitionWriteSpeedBytesPerSecond_);
        request.set_partition_write_burst_bytes(settings.PartitionWriteBurstBytes_);
        request.set_retention_storage_mb(settings.RetentionStorageMb_);

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
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);
    }


    static Ydb::Topic::AlterTopicRequest MakePropsAlterRequest(const TString& path, const TAlterTopicSettings& settings) {
        Ydb::Topic::AlterTopicRequest request = MakeOperationRequest<Ydb::Topic::AlterTopicRequest>(settings);
        request.set_path(path);

        if (settings.AlterPartitioningSettings_) {
            request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(settings.AlterPartitioningSettings_->GetMinActivePartitions());
            request.mutable_alter_partitioning_settings()->set_set_partition_count_limit(settings.AlterPartitioningSettings_->GetPartitionCountLimit());
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
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);
    }


    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Topic::DropTopicRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::Topic::V1::TopicService, Ydb::Topic::DropTopicRequest, Ydb::Topic::DropTopicResponse>(
            std::move(request),
            &Ydb::Topic::V1::TopicService::Stub::AsyncDropTopic,
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);
    }

    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Topic::DescribeTopicRequest>(settings);
        request.set_path(path);

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
            TRpcRequestSettings::Make(settings),
            settings.ClientTimeout_);

        return promise.GetFuture();
    }

private:
    const TTopicClientSettings Settings;
};

} // namespace NYdb::NTopic

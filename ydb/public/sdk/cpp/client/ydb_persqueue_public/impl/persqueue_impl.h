#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/aliases.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H


namespace NYdb::NPersQueue {

class TPersQueueClient::TImpl : public TClientImplCommon<TPersQueueClient::TImpl> {
public:
    // Constructor for main client.
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TPersQueueClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings(settings)
    {
    }

    // Constructor for subclients with endpoints discovered by cluster discovery.
    // Async discovery mode is used because this client is created inside SDK threads.
    // See YDB-1231 and YDB-1232.
    TImpl(const TString& clusterEndpoint, std::shared_ptr<TGRpcConnectionsImpl> connections, const TPersQueueClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings.Database_, clusterEndpoint, EDiscoveryMode::Async, settings.SslCredentials_, settings.CredentialsProviderFactory_)
        , Settings(settings)
        , CustomEndpoint(clusterEndpoint)
    {
    }

    template<class TReadRule>
    static void ConvertToProtoReadRule(const TReadRule& readRule, Ydb::PersQueue::V1::TopicSettings::ReadRule& rrProps) {
        rrProps.set_consumer_name(readRule.ConsumerName_);
        rrProps.set_important(readRule.Important_);
        rrProps.set_starting_message_timestamp_ms(readRule.StartingMessageTimestamp_.MilliSeconds());
        rrProps.set_version(readRule.Version_);
        rrProps.set_supported_format(static_cast<Ydb::PersQueue::V1::TopicSettings::Format>(readRule.SupportedFormat_));
        for (const auto& codec : readRule.SupportedCodecs_) {
            rrProps.add_supported_codecs((static_cast<Ydb::PersQueue::V1::Codec>(codec)));
        }
        rrProps.set_service_type(readRule.ServiceType_);
    }

    template <class TRequest, class TSettings>
    static TRequest MakePropsCreateOrAlterRequest(const TString& path, const TSettings& settings) {
        TRequest request = MakeOperationRequest<TRequest>(settings);
        request.set_path(path);

        Ydb::PersQueue::V1::TopicSettings& props = *request.mutable_settings();

        props.set_partitions_count(settings.PartitionsCount_);

        bool autoPartitioningSettingsDefined = false;
        if (settings.MaxPartitionsCount_.Defined()) {
            props.mutable_auto_partitioning_settings()->set_max_active_partitions(settings.PartitionsCount_);
            autoPartitioningSettingsDefined = true;
        }
        if (settings.AutoPartitioningStrategy_.Defined()) {
            props.mutable_auto_partitioning_settings()->set_strategy(*settings.AutoPartitioningStrategy_);
            autoPartitioningSettingsDefined = true;
        }
        if (settings.DownUtilizationPercent_.Defined()) {
            props.mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_down_utilization_percent(*settings.DownUtilizationPercent_);
            autoPartitioningSettingsDefined = true;
        }
        if (settings.UpUtilizationPercent_.Defined()) {
            props.mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_up_utilization_percent(*settings.UpUtilizationPercent_);
            autoPartitioningSettingsDefined = true;
        }
        if (settings.StabilizationWindow_.Defined()) {
            props.mutable_auto_partitioning_settings()->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds((*settings.StabilizationWindow_).Seconds());
            autoPartitioningSettingsDefined = true;
        }

        if (!autoPartitioningSettingsDefined) {
            props.set_partitions_count(settings.PartitionsCount_);
        } else {
            props.mutable_auto_partitioning_settings()->set_min_active_partitions(settings.PartitionsCount_);
        }


        props.set_retention_period_ms(settings.RetentionPeriod_.MilliSeconds());
        props.set_supported_format(static_cast<Ydb::PersQueue::V1::TopicSettings::Format>(settings.SupportedFormat_));
        for (const auto& codec : settings.SupportedCodecs_) {
            props.add_supported_codecs((static_cast<Ydb::PersQueue::V1::Codec>(codec)));
        }
        props.set_max_partition_storage_size(settings.MaxPartitionStorageSize_);
        props.set_max_partition_write_speed(settings.MaxPartitionWriteSpeed_);
        props.set_max_partition_write_burst(settings.MaxPartitionWriteBurst_);
        props.set_client_write_disabled(settings.ClientWriteDisabled_);
        (*props.mutable_attributes())["_allow_unauthenticated_read"] = settings.AllowUnauthenticatedRead_ ? "true" : "false";
        (*props.mutable_attributes())["_allow_unauthenticated_write"] = settings.AllowUnauthenticatedWrite_ ? "true" : "false";
        if (settings.PartitionsPerTablet_) (*props.mutable_attributes())["_partitions_per_tablet"] = ToString(*settings.PartitionsPerTablet_);
        if (settings.AbcId_) (*props.mutable_attributes())["_abc_id"] = ToString(*settings.AbcId_);
        if (settings.AbcSlug_) (*props.mutable_attributes())["_abc_slug"] = *settings.AbcSlug_;
        if (settings.FederationAccount_) (*props.mutable_attributes())["_federation_account"] = ToString(*settings.FederationAccount_);

        for (const auto& readRule : settings.ReadRules_) {
            Ydb::PersQueue::V1::TopicSettings::ReadRule& rrProps = *props.add_read_rules();
            ConvertToProtoReadRule(readRule, rrProps);
        }

        if (settings.RemoteMirrorRule_) {
            auto rmr = props.mutable_remote_mirror_rule();
            rmr->set_endpoint(settings.RemoteMirrorRule_.GetRef().Endpoint_);
            rmr->set_topic_path(settings.RemoteMirrorRule_.GetRef().TopicPath_);
            rmr->set_consumer_name(settings.RemoteMirrorRule_.GetRef().ConsumerName_);
            rmr->set_starting_message_timestamp_ms(settings.RemoteMirrorRule_.GetRef().StartingMessageTimestamp_.MilliSeconds());
            const auto& credentials = settings.RemoteMirrorRule_.GetRef().Credentials_;
            switch (credentials.GetMode()) {
                case TCredentials::EMode::OAUTH_TOKEN: {
                    rmr->mutable_credentials()->set_oauth_token(credentials.GetOauthToken());
                    break;
                }
                case TCredentials::EMode::JWT_PARAMS: {
                    rmr->mutable_credentials()->set_jwt_params(credentials.GetJwtParams());
                    break;
                }
                case TCredentials::EMode::IAM: {
                    rmr->mutable_credentials()->mutable_iam()->set_endpoint(credentials.GetIamEndpoint());
                    rmr->mutable_credentials()->mutable_iam()->set_service_account_key(credentials.GetIamServiceAccountKey());
                    break;
                }
                case TCredentials::EMode::NOT_SET: {
                    break;
                }
                default: {
                    ythrow yexception() << "unsupported credentials type for remote mirror rule";
                }
            }
            rmr->set_database(settings.RemoteMirrorRule_.GetRef().Database_);
        }
        return request;
    }

    void ProvideCodec(ECodec codecId, THolder<ICodec>&& codecImpl) {
        with_lock(Lock) {
            if (ProvidedCodecs->contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " already provided";
            }
            (*ProvidedCodecs)[codecId] = std::move(codecImpl);
        }
    }

    void OverrideCodec(ECodec codecId, THolder<ICodec>&& codecImpl) {
        with_lock(Lock) {
            (*ProvidedCodecs)[codecId] = std::move(codecImpl);
        }
    }

    const ICodec* GetCodecImplOrThrow(ECodec codecId) const {
        with_lock(Lock) {
            if (!ProvidedCodecs->contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " not provided";
            }
            return ProvidedCodecs->at(codecId).Get();
        }
    }

    std::shared_ptr<std::unordered_map<ECodec, THolder<ICodec>>> GetProvidedCodecs() const {
        return ProvidedCodecs;
    }

    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings) {
        auto request = MakePropsCreateOrAlterRequest<Ydb::PersQueue::V1::CreateTopicRequest>(path,
            settings.PartitionsPerTablet_ ? settings : TCreateTopicSettings(settings).PartitionsPerTablet(2));

        return RunSimple<Ydb::PersQueue::V1::PersQueueService, Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(
            std::move(request),
            &Ydb::PersQueue::V1::PersQueueService::Stub::AsyncCreateTopic,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& settings) {
        auto request = MakePropsCreateOrAlterRequest<Ydb::PersQueue::V1::AlterTopicRequest>(path, settings);

        return RunSimple<Ydb::PersQueue::V1::PersQueueService, Ydb::PersQueue::V1::AlterTopicRequest, Ydb::PersQueue::V1::AlterTopicResponse>(
            std::move(request),
            &Ydb::PersQueue::V1::PersQueueService::Stub::AsyncAlterTopic,
            TRpcRequestSettings::Make(settings));
    }


    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings) {
        auto request = MakeOperationRequest<Ydb::PersQueue::V1::DropTopicRequest>(settings);
        request.set_path(path);

        return RunSimple<Ydb::PersQueue::V1::PersQueueService, Ydb::PersQueue::V1::DropTopicRequest, Ydb::PersQueue::V1::DropTopicResponse>(
            std::move(request),
            &Ydb::PersQueue::V1::PersQueueService::Stub::AsyncDropTopic,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AddReadRule(const TString& path, const TAddReadRuleSettings& settings) {
        auto request = MakeOperationRequest<Ydb::PersQueue::V1::AddReadRuleRequest>(settings);
        request.set_path(path);
        ConvertToProtoReadRule(settings.ReadRule_, *request.mutable_read_rule());
        return RunSimple<Ydb::PersQueue::V1::PersQueueService, Ydb::PersQueue::V1::AddReadRuleRequest, Ydb::PersQueue::V1::AddReadRuleResponse>(
                std::move(request),
                &Ydb::PersQueue::V1::PersQueueService::Stub::AsyncAddReadRule,
                TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemoveReadRule(const TString& path, const TRemoveReadRuleSettings& settings) {
        auto request = MakeOperationRequest<Ydb::PersQueue::V1::RemoveReadRuleRequest>(settings);
        request.set_path(path);

        request.set_consumer_name(settings.ConsumerName_);
        return RunSimple<Ydb::PersQueue::V1::PersQueueService, Ydb::PersQueue::V1::RemoveReadRuleRequest, Ydb::PersQueue::V1::RemoveReadRuleResponse>(
                std::move(request),
                &Ydb::PersQueue::V1::PersQueueService::Stub::AsyncRemoveReadRule,
                TRpcRequestSettings::Make(settings));
    }


    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) {
        auto request = MakeOperationRequest<Ydb::PersQueue::V1::DescribeTopicRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TDescribeTopicResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::PersQueue::V1::DescribeTopicResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeTopicResult val(TStatus(std::move(status)), result);
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::PersQueue::V1::PersQueueService, Ydb::PersQueue::V1::DescribeTopicRequest, Ydb::PersQueue::V1::DescribeTopicResponse>(
            std::move(request),
            extractor,
            &Ydb::PersQueue::V1::PersQueueService::Stub::AsyncDescribeTopic,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    // Runtime API.
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings);
    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleWriteSession(const TWriteSessionSettings& settings);
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings);

    std::shared_ptr<TImpl> GetClientForEndpoint(const TString& clusterEndoint);

    using IReadSessionConnectionProcessorFactory = ISessionConnectionProcessorFactory<Ydb::PersQueue::V1::MigrationStreamingReadClientMessage, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>;

    std::shared_ptr<IReadSessionConnectionProcessorFactory> CreateReadSessionConnectionProcessorFactory();

    using IWriteSessionConnectionProcessorFactory = ISessionConnectionProcessorFactory<
            Ydb::PersQueue::V1::StreamingWriteClientMessage,
            Ydb::PersQueue::V1::StreamingWriteServerMessage>;

    std::shared_ptr<IWriteSessionConnectionProcessorFactory> CreateWriteSessionConnectionProcessorFactory();

    NYdbGrpc::IQueueClientContextPtr CreateContext() {
        return Connections_->CreateContext();
    }

private:
    const TPersQueueClientSettings Settings;
    const TString CustomEndpoint;
    TAdaptiveLock Lock;
    std::shared_ptr<std::unordered_map<ECodec, THolder<ICodec>>> ProvidedCodecs = std::make_shared<std::unordered_map<ECodec, THolder<ICodec>>>();
    THashMap<TString, std::shared_ptr<TImpl>> Subclients; // Endpoint -> Subclient.
};

}  // namespace NYdb::NPersQueue

#pragma once

#include <ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>

namespace NYdb::NDataStreams::V1 {

    template<class TProtoResult>
    class TProtoResultWrapper : public NYdb::TStatus {
        friend class TDataStreamsClient;

    private:
        TProtoResultWrapper(
                NYdb::TStatus&& status,
                std::unique_ptr<TProtoResult> result)
                : TStatus(std::move(status))
                , Result(std::move(result))
        { }

    public:
        const TProtoResult& GetResult() const {
            Y_ABORT_UNLESS(Result, "Uninitialized result");
            return *Result;
        }

    private:
        std::unique_ptr<TProtoResult> Result;
    };

    enum EStreamMode {
        ESM_PROVISIONED = 1,
        ESM_ON_DEMAND = 2,
    };

    using TCreateStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::CreateStreamResult>;
    using TDeleteStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::DeleteStreamResult>;
    using TDescribeStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::DescribeStreamResult>;
    using TPutRecordResult = TProtoResultWrapper<Ydb::DataStreams::V1::PutRecordResult>;
    using TRegisterStreamConsumerResult = TProtoResultWrapper<Ydb::DataStreams::V1::RegisterStreamConsumerResult>;
    using TDeregisterStreamConsumerResult = TProtoResultWrapper<Ydb::DataStreams::V1::DeregisterStreamConsumerResult>;
    using TDescribeStreamConsumerResult = TProtoResultWrapper<Ydb::DataStreams::V1::DescribeStreamConsumerResult>;
    using TListStreamsResult = TProtoResultWrapper<Ydb::DataStreams::V1::ListStreamsResult>;
    using TListShardsResult = TProtoResultWrapper<Ydb::DataStreams::V1::ListShardsResult>;
    using TPutRecordsResult = TProtoResultWrapper<Ydb::DataStreams::V1::PutRecordsResult>;
    using TGetRecordsResult = TProtoResultWrapper<Ydb::DataStreams::V1::GetRecordsResult>;
    using TGetShardIteratorResult = TProtoResultWrapper<Ydb::DataStreams::V1::GetShardIteratorResult>;
    // using TSubscribeToShardResult = TProtoResultWrapper<Ydb::DataStreams::V1::SubscribeToShardResult>;
    using TDescribeLimitsResult = TProtoResultWrapper<Ydb::DataStreams::V1::DescribeLimitsResult>;
    using TDescribeStreamSummaryResult = TProtoResultWrapper<Ydb::DataStreams::V1::DescribeStreamSummaryResult>;
    using TDecreaseStreamRetentionPeriodResult = TProtoResultWrapper<Ydb::DataStreams::V1::DecreaseStreamRetentionPeriodResult>;
    using TIncreaseStreamRetentionPeriodResult = TProtoResultWrapper<Ydb::DataStreams::V1::IncreaseStreamRetentionPeriodResult>;
    using TUpdateShardCountResult = TProtoResultWrapper<Ydb::DataStreams::V1::UpdateShardCountResult>;
    using TUpdateStreamModeResult = TProtoResultWrapper<Ydb::DataStreams::V1::UpdateStreamModeResult>;
    using TListStreamConsumersResult = TProtoResultWrapper<Ydb::DataStreams::V1::ListStreamConsumersResult>;
    using TAddTagsToStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::AddTagsToStreamResult>;
    using TDisableEnhancedMonitoringResult = TProtoResultWrapper<Ydb::DataStreams::V1::DisableEnhancedMonitoringResult>;
    using TEnableEnhancedMonitoringResult = TProtoResultWrapper<Ydb::DataStreams::V1::EnableEnhancedMonitoringResult>;
    using TListTagsForStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::ListTagsForStreamResult>;
    using TMergeShardsResult = TProtoResultWrapper<Ydb::DataStreams::V1::MergeShardsResult>;
    using TRemoveTagsFromStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::RemoveTagsFromStreamResult>;
    using TSplitShardResult = TProtoResultWrapper<Ydb::DataStreams::V1::SplitShardResult>;
    using TStartStreamEncryptionResult = TProtoResultWrapper<Ydb::DataStreams::V1::StartStreamEncryptionResult>;
    using TStopStreamEncryptionResult = TProtoResultWrapper<Ydb::DataStreams::V1::StopStreamEncryptionResult>;
    using TUpdateStreamResult = TProtoResultWrapper<Ydb::DataStreams::V1::UpdateStreamResult>;

    using TAsyncCreateStreamResult = NThreading::TFuture<TCreateStreamResult>;
    using TAsyncDeleteStreamResult = NThreading::TFuture<TDeleteStreamResult>;
    using TAsyncDescribeStreamResult = NThreading::TFuture<TDescribeStreamResult>;
    using TAsyncPutRecordResult = NThreading::TFuture<TPutRecordResult>;
    using TAsyncRegisterStreamConsumerResult = NThreading::TFuture<TRegisterStreamConsumerResult>;
    using TAsyncDeregisterStreamConsumerResult = NThreading::TFuture<TDeregisterStreamConsumerResult>;
    using TAsyncDescribeStreamConsumerResult = NThreading::TFuture<TDescribeStreamConsumerResult>;
    using TAsyncListStreamsResult = NThreading::TFuture<TListStreamsResult>;
    using TAsyncListShardsResult = NThreading::TFuture<TListShardsResult>;
    using TAsyncPutRecordsResult = NThreading::TFuture<TPutRecordsResult>;
    using TAsyncGetRecordsResult = NThreading::TFuture<TGetRecordsResult>;
    using TAsyncGetShardIteratorResult = NThreading::TFuture<TGetShardIteratorResult>;
    // using TAsyncSubscribeToShardResult = NThreading::TFuture<TSubscribeToShardResult>;
    using TAsyncDescribeLimitsResult = NThreading::TFuture<TDescribeLimitsResult>;
    using TAsyncDescribeStreamSummaryResult = NThreading::TFuture<TDescribeStreamSummaryResult>;
    using TAsyncDecreaseStreamRetentionPeriodResult = NThreading::TFuture<TDecreaseStreamRetentionPeriodResult>;
    using TAsyncIncreaseStreamRetentionPeriodResult = NThreading::TFuture<TIncreaseStreamRetentionPeriodResult>;
    using TAsyncUpdateShardCountResult = NThreading::TFuture<TUpdateShardCountResult>;
    using TAsyncUpdateStreamModeResult = NThreading::TFuture<TUpdateStreamModeResult>;
    using TAsyncListStreamConsumersResult = NThreading::TFuture<TListStreamConsumersResult>;
    using TAsyncAddTagsToStreamResult = NThreading::TFuture<TAddTagsToStreamResult>;
    using TAsyncDisableEnhancedMonitoringResult = NThreading::TFuture<TDisableEnhancedMonitoringResult>;
    using TAsyncEnableEnhancedMonitoringResult = NThreading::TFuture<TEnableEnhancedMonitoringResult>;
    using TAsyncListTagsForStreamResult = NThreading::TFuture<TListTagsForStreamResult>;
    using TAsyncMergeShardsResult = NThreading::TFuture<TMergeShardsResult>;
    using TAsyncRemoveTagsFromStreamResult = NThreading::TFuture<TRemoveTagsFromStreamResult>;
    using TAsyncSplitShardResult = NThreading::TFuture<TSplitShardResult>;
    using TAsyncStartStreamEncryptionResult = NThreading::TFuture<TStartStreamEncryptionResult>;
    using TAsyncStopStreamEncryptionResult = NThreading::TFuture<TStopStreamEncryptionResult>;
    using TAsyncUpdateStreamResult = NThreading::TFuture<TUpdateStreamResult>;

    struct TDataRecord {
        std::string Data;
        std::string PartitionKey;
        std::string ExplicitHashDecimal;
    };

    struct TCreateStreamSettings : public NYdb::TOperationRequestSettings<TCreateStreamSettings> {
        FLUENT_SETTING(uint32_t, ShardCount);
        FLUENT_SETTING_OPTIONAL(uint32_t, RetentionPeriodHours);
        FLUENT_SETTING_OPTIONAL(uint32_t, RetentionStorageMegabytes);
        FLUENT_SETTING(uint64_t, WriteQuotaKbPerSec);
        FLUENT_SETTING_OPTIONAL(EStreamMode, StreamMode);
    };
    struct TListStreamsSettings : public NYdb::TOperationRequestSettings<TListStreamsSettings> {
        FLUENT_SETTING(uint32_t, Limit);
        FLUENT_SETTING(std::string, ExclusiveStartStreamName);
        FLUENT_SETTING_DEFAULT(bool, Recurse, true);
    };
    struct TDeleteStreamSettings : public NYdb::TOperationRequestSettings<TDeleteStreamSettings> {
        FLUENT_SETTING_DEFAULT(bool, EnforceConsumerDeletion, false);
    };
    struct TDescribeStreamSettings : public NYdb::TOperationRequestSettings<TDescribeStreamSettings> {
        FLUENT_SETTING(uint32_t, Limit);
        FLUENT_SETTING(std::string, ExclusiveStartShardId);
    };
    struct TListShardsSettings : public NYdb::TOperationRequestSettings<TListShardsSettings> {
        FLUENT_SETTING(std::string, ExclusiveStartShardId);
        FLUENT_SETTING(uint32_t, MaxResults);
        FLUENT_SETTING(std::string, NextToken);
        FLUENT_SETTING(uint64_t, StreamCreationTimestamp);
    };
    struct TGetRecordsSettings : public NYdb::TOperationRequestSettings<TGetRecordsSettings> {
        FLUENT_SETTING_DEFAULT(uint32_t, Limit, 10000);
    };
    struct TGetShardIteratorSettings : public NYdb::TOperationRequestSettings<TGetShardIteratorSettings> {
        FLUENT_SETTING(std::string, StartingSequenceNumber);
        FLUENT_SETTING(uint64_t, Timestamp);
    };
    struct TSubscribeToShardSettings : public NYdb::TOperationRequestSettings<TSubscribeToShardSettings> {};
    struct TDescribeLimitsSettings : public NYdb::TOperationRequestSettings<TDescribeLimitsSettings> {};
    struct TDescribeStreamSummarySettings : public NYdb::TOperationRequestSettings<TDescribeStreamSummarySettings> {};
    struct TDecreaseStreamRetentionPeriodSettings : public NYdb::TOperationRequestSettings<TDecreaseStreamRetentionPeriodSettings> {
        FLUENT_SETTING(uint32_t, RetentionPeriodHours);
    };
    struct TIncreaseStreamRetentionPeriodSettings : public NYdb::TOperationRequestSettings<TIncreaseStreamRetentionPeriodSettings> {
        FLUENT_SETTING(uint32_t, RetentionPeriodHours);
    };
    struct TUpdateShardCountSettings : public NYdb::TOperationRequestSettings<TUpdateShardCountSettings> {
        FLUENT_SETTING(uint32_t, TargetShardCount);
    };
    struct TUpdateStreamModeSettings : public NYdb::TOperationRequestSettings<TUpdateStreamModeSettings> {
        FLUENT_SETTING_DEFAULT(EStreamMode, StreamMode, ESM_PROVISIONED);
    };
    struct TUpdateStreamSettings : public NYdb::TOperationRequestSettings<TUpdateStreamSettings> {
        FLUENT_SETTING(uint32_t, TargetShardCount);
        FLUENT_SETTING_OPTIONAL(uint32_t, RetentionPeriodHours);
        FLUENT_SETTING_OPTIONAL(uint32_t, RetentionStorageMegabytes);
        FLUENT_SETTING(uint64_t, WriteQuotaKbPerSec);
        FLUENT_SETTING_OPTIONAL(EStreamMode, StreamMode);

    };
    struct TPutRecordSettings : public NYdb::TOperationRequestSettings<TPutRecordSettings> {};
    struct TPutRecordsSettings : public NYdb::TOperationRequestSettings<TPutRecordsSettings> {};
    struct TRegisterStreamConsumerSettings : public NYdb::TOperationRequestSettings<TRegisterStreamConsumerSettings> {};
    struct TDeregisterStreamConsumerSettings : public NYdb::TOperationRequestSettings<TDeregisterStreamConsumerSettings> {};
    struct TDescribeStreamConsumerSettings : public NYdb::TOperationRequestSettings<TDescribeStreamConsumerSettings> {};
    struct TListStreamConsumersSettings : public NYdb::TOperationRequestSettings<TListStreamConsumersSettings> {
        FLUENT_SETTING(uint32_t, MaxResults);
        FLUENT_SETTING(std::string, NextToken);
    };
    struct TAddTagsToStreamSettings : public NYdb::TOperationRequestSettings<TAddTagsToStreamSettings> {};
    struct TDisableEnhancedMonitoringSettings : public NYdb::TOperationRequestSettings<TDisableEnhancedMonitoringSettings> {};
    struct TEnableEnhancedMonitoringSettings : public NYdb::TOperationRequestSettings<TEnableEnhancedMonitoringSettings> {};
    struct TListTagsForStreamSettings : public NYdb::TOperationRequestSettings<TListTagsForStreamSettings> {};
    struct TMergeShardsSettings : public NYdb::TOperationRequestSettings<TMergeShardsSettings> {};
    struct TRemoveTagsFromStreamSettings : public NYdb::TOperationRequestSettings<TRemoveTagsFromStreamSettings> {};
    struct TSplitShardSettings : public NYdb::TOperationRequestSettings<TSplitShardSettings> {};
    struct TStartStreamEncryptionSettings : public NYdb::TOperationRequestSettings<TStartStreamEncryptionSettings> {};
    struct TStopStreamEncryptionSettings : public NYdb::TOperationRequestSettings<TStopStreamEncryptionSettings> {};
    struct TProtoRequestSettings : public NYdb::TOperationRequestSettings<TProtoRequestSettings> {};

    class TDataStreamsClient {
        class TImpl;

    public:
        TDataStreamsClient(const NYdb::TDriver& driver, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings());

        TAsyncCreateStreamResult CreateStream(const std::string& path, TCreateStreamSettings settings = TCreateStreamSettings());
        TAsyncDeleteStreamResult DeleteStream(const std::string& path, TDeleteStreamSettings settings = TDeleteStreamSettings());
        TAsyncDescribeStreamResult DescribeStream(const std::string& path, TDescribeStreamSettings settings = TDescribeStreamSettings());
        TAsyncPutRecordResult PutRecord(const std::string& path, const TDataRecord& record, TPutRecordSettings settings = TPutRecordSettings());
        TAsyncListStreamsResult ListStreams(TListStreamsSettings settings = TListStreamsSettings());
        TAsyncListShardsResult ListShards(const std::string& path, const Ydb::DataStreams::V1::ShardFilter& shardFilter, TListShardsSettings settings = TListShardsSettings());
        TAsyncPutRecordsResult PutRecords(const std::string& path, const std::vector<TDataRecord>& records, TPutRecordsSettings settings = TPutRecordsSettings());
        TAsyncGetRecordsResult GetRecords(const std::string& shardIterator, TGetRecordsSettings settings = TGetRecordsSettings());
        TAsyncGetShardIteratorResult GetShardIterator(const std::string& path, const std::string& shardId, Ydb::DataStreams::V1::ShardIteratorType shardIteratorTypeStr,
                                                      TGetShardIteratorSettings settings = TGetShardIteratorSettings());
        // TAsyncSubscribeToShardResult SubscribeToShard(TSubscribeToShardSettings settings = TSubscribeToShardSettings());
        TAsyncDescribeLimitsResult DescribeLimits(TDescribeLimitsSettings settings = TDescribeLimitsSettings());
        TAsyncDescribeStreamSummaryResult DescribeStreamSummary(const std::string& path, TDescribeStreamSummarySettings settings = TDescribeStreamSummarySettings());
        TAsyncDecreaseStreamRetentionPeriodResult DecreaseStreamRetentionPeriod(const std::string& path, TDecreaseStreamRetentionPeriodSettings settings = TDecreaseStreamRetentionPeriodSettings());
        TAsyncIncreaseStreamRetentionPeriodResult IncreaseStreamRetentionPeriod(const std::string& path, TIncreaseStreamRetentionPeriodSettings settings = TIncreaseStreamRetentionPeriodSettings());
        TAsyncUpdateShardCountResult UpdateShardCount(const std::string& path, TUpdateShardCountSettings settings = TUpdateShardCountSettings());
        TAsyncUpdateStreamModeResult UpdateStreamMode(const std::string& path, TUpdateStreamModeSettings settings = TUpdateStreamModeSettings());
        TAsyncRegisterStreamConsumerResult RegisterStreamConsumer(const std::string& path, const std::string& consumer_name, TRegisterStreamConsumerSettings settings = TRegisterStreamConsumerSettings());
        TAsyncDeregisterStreamConsumerResult DeregisterStreamConsumer(const std::string& path, const std::string& consumer_name, TDeregisterStreamConsumerSettings settings = TDeregisterStreamConsumerSettings());
        TAsyncDescribeStreamConsumerResult DescribeStreamConsumer(TDescribeStreamConsumerSettings settings = TDescribeStreamConsumerSettings());
        TAsyncListStreamConsumersResult ListStreamConsumers(const std::string& path, TListStreamConsumersSettings settings = TListStreamConsumersSettings());
        TAsyncAddTagsToStreamResult AddTagsToStream(TAddTagsToStreamSettings settings = TAddTagsToStreamSettings());
        TAsyncDisableEnhancedMonitoringResult DisableEnhancedMonitoring(TDisableEnhancedMonitoringSettings settings = TDisableEnhancedMonitoringSettings());
        TAsyncEnableEnhancedMonitoringResult EnableEnhancedMonitoring(TEnableEnhancedMonitoringSettings settings = TEnableEnhancedMonitoringSettings());
        TAsyncListTagsForStreamResult ListTagsForStream(TListTagsForStreamSettings settings = TListTagsForStreamSettings());
        TAsyncMergeShardsResult MergeShards(TMergeShardsSettings settings = TMergeShardsSettings());
        TAsyncRemoveTagsFromStreamResult RemoveTagsFromStream(TRemoveTagsFromStreamSettings settings = TRemoveTagsFromStreamSettings());
        TAsyncSplitShardResult SplitShard(TSplitShardSettings settings = TSplitShardSettings());
        TAsyncStartStreamEncryptionResult StartStreamEncryption(TStartStreamEncryptionSettings settings = TStartStreamEncryptionSettings());
        TAsyncStopStreamEncryptionResult StopStreamEncryption(TStopStreamEncryptionSettings settings = TStopStreamEncryptionSettings());
        TAsyncUpdateStreamResult UpdateStream(const std::string& streamName, TUpdateStreamSettings settings = TUpdateStreamSettings());

        template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> DoProtoRequest(const TProtoRequest& request, TMethod method, TProtoRequestSettings settings = TProtoRequestSettings());

        NThreading::TFuture<void> DiscoveryCompleted();

    private:
        std::shared_ptr<TImpl> Impl_;
    };

}

#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

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
        TString Data;
        TString PartitionKey;
        TString ExplicitHashDecimal;
    };

    enum class EAutoPartitioningStrategy: ui32 {
        Unspecified = 0,
        Disabled = 1,
        ScaleUp = 2,
        ScaleUpAndDown = 3,
        Paused = 4
    };

    struct TCreateStreamSettings;
    struct TUpdateStreamSettings;


    template<typename TSettings>
    struct TPartitioningSettingsBuilder;
    template<typename TSettings>
    struct TAutoPartitioningSettingsBuilder;

    struct TAutoPartitioningSettings {
    friend struct TAutoPartitioningSettingsBuilder<TCreateStreamSettings>;
    friend struct TAutoPartitioningSettingsBuilder<TUpdateStreamSettings>;
    public:
        TAutoPartitioningSettings()
            : Strategy_(EAutoPartitioningStrategy::Disabled)
            , StabilizationWindow_(TDuration::Seconds(0))
            , DownUtilizationPercent_(0)
            , UpUtilizationPercent_(0) {
        }
        TAutoPartitioningSettings(const Ydb::DataStreams::V1::AutoPartitioningSettings& settings);
        TAutoPartitioningSettings(EAutoPartitioningStrategy strategy, TDuration stabilizationWindow, ui64 downUtilizationPercent, ui64 upUtilizationPercent)
            : Strategy_(strategy)
            , StabilizationWindow_(stabilizationWindow)
            , DownUtilizationPercent_(downUtilizationPercent)
            , UpUtilizationPercent_(upUtilizationPercent) {}

        EAutoPartitioningStrategy GetStrategy() const { return Strategy_; };
        TDuration GetStabilizationWindow() const { return StabilizationWindow_; };
        ui32 GetDownUtilizationPercent() const { return DownUtilizationPercent_; };
        ui32 GetUpUtilizationPercent() const { return UpUtilizationPercent_; };
    private:
        EAutoPartitioningStrategy Strategy_;
        TDuration StabilizationWindow_;
        ui32 DownUtilizationPercent_;
        ui32 UpUtilizationPercent_;
    };


    class TPartitioningSettings {
        using TSelf = TPartitioningSettings;
        friend struct TPartitioningSettingsBuilder<TCreateStreamSettings>;
        friend struct TPartitioningSettingsBuilder<TUpdateStreamSettings>;
    public:
        TPartitioningSettings() : MinActivePartitions_(0), MaxActivePartitions_(0), AutoPartitioningSettings_(){}
        TPartitioningSettings(const Ydb::DataStreams::V1::PartitioningSettings& settings);
        TPartitioningSettings(ui64 minActivePartitions, ui64 maxActivePartitions, TAutoPartitioningSettings autoscalingSettings = {})
            : MinActivePartitions_(minActivePartitions)
            , MaxActivePartitions_(maxActivePartitions)
            , AutoPartitioningSettings_(autoscalingSettings) {
        }

        ui64 GetMinActivePartitions() const { return MinActivePartitions_; };
        ui64 GetMaxActivePartitions() const { return MaxActivePartitions_; };
        TAutoPartitioningSettings GetAutoPartitioningSettings() const { return AutoPartitioningSettings_; };
    private:
        ui64 MinActivePartitions_;
        ui64 MaxActivePartitions_;
        TAutoPartitioningSettings AutoPartitioningSettings_;
    };

    struct TCreateStreamSettings : public NYdb::TOperationRequestSettings<TCreateStreamSettings> {
        FLUENT_SETTING(ui32, ShardCount);
        FLUENT_SETTING_OPTIONAL(ui32, RetentionPeriodHours);
        FLUENT_SETTING_OPTIONAL(ui32, RetentionStorageMegabytes);
        FLUENT_SETTING(ui64, WriteQuotaKbPerSec);
        FLUENT_SETTING_OPTIONAL(EStreamMode, StreamMode);

        FLUENT_SETTING_OPTIONAL(TPartitioningSettings, PartitioningSettings);
        TPartitioningSettingsBuilder<TCreateStreamSettings> BeginConfigurePartitioningSettings();
    };
    template<typename TSettings>
    struct TAutoPartitioningSettingsBuilder {
        using TSelf = TAutoPartitioningSettingsBuilder<TSettings>;
    public:
        TAutoPartitioningSettingsBuilder(TPartitioningSettingsBuilder<TSettings>& parent, TAutoPartitioningSettings& settings): Parent_(parent), Settings_(settings) {}

        TSelf Strategy(EAutoPartitioningStrategy value) {
            Settings_.Strategy_ = value;
            return *this;
        }

        TSelf StabilizationWindow(TDuration value) {
            Settings_.StabilizationWindow_ = value;
            return *this;
        }

        TSelf DownUtilizationPercent(ui32 value) {
            Settings_.DownUtilizationPercent_ = value;
            return *this;
        }

        TSelf UpUtilizationPercent(ui32 value) {
            Settings_.UpUtilizationPercent_ = value;
            return *this;
        }

        TPartitioningSettingsBuilder<TSettings>& EndConfigureAutoPartitioningSettings() {
            return Parent_;
        }

    private:
        TPartitioningSettingsBuilder<TSettings>& Parent_;
        TAutoPartitioningSettings& Settings_;
    };

    template<typename TSettings>
    struct TPartitioningSettingsBuilder {
        using TSelf = TPartitioningSettingsBuilder;
    public:
        TPartitioningSettingsBuilder(TSettings& parent): Parent_(parent) {}

        TSelf MinActivePartitions(ui64 value) {
            if (!Parent_.PartitioningSettings_.Defined()) {
                Parent_.PartitioningSettings_.ConstructInPlace();
            }
            (*Parent_.PartitioningSettings_).MinActivePartitions_ = value;
            return *this;
        }

        TSelf MaxActivePartitions(ui64 value) {
            if (!Parent_.PartitioningSettings_.Defined()) {
                Parent_.PartitioningSettings_.ConstructInPlace();
            }
            (*Parent_.PartitioningSettings_).MaxActivePartitions_ = value;
            return *this;
        }

        TAutoPartitioningSettingsBuilder<TSettings> BeginConfigureAutoPartitioningSettings() {
            if (!Parent_.PartitioningSettings_.Defined()) {
                Parent_.PartitioningSettings_.ConstructInPlace();
            }
            return {*this, (*Parent_.PartitioningSettings_).AutoPartitioningSettings_};
        }

        TSettings& EndConfigurePartitioningSettings() {
            return Parent_;
        }

    private:
        TSettings& Parent_;
    };

    struct TListStreamsSettings : public NYdb::TOperationRequestSettings<TListStreamsSettings> {
        FLUENT_SETTING(ui32, Limit);
        FLUENT_SETTING(TString, ExclusiveStartStreamName);
        FLUENT_SETTING_DEFAULT(bool, Recurse, true);
    };
    struct TDeleteStreamSettings : public NYdb::TOperationRequestSettings<TDeleteStreamSettings> {
        FLUENT_SETTING_DEFAULT(bool, EnforceConsumerDeletion, false);
    };
    struct TDescribeStreamSettings : public NYdb::TOperationRequestSettings<TDescribeStreamSettings> {
        FLUENT_SETTING(ui32, Limit);
        FLUENT_SETTING(TString, ExclusiveStartShardId);
    };
    struct TListShardsSettings : public NYdb::TOperationRequestSettings<TListShardsSettings> {
        FLUENT_SETTING(TString, ExclusiveStartShardId);
        FLUENT_SETTING(ui32, MaxResults);
        FLUENT_SETTING(TString, NextToken);
        FLUENT_SETTING(ui64, StreamCreationTimestamp);
    };
    struct TGetRecordsSettings : public NYdb::TOperationRequestSettings<TGetRecordsSettings> {
        FLUENT_SETTING_DEFAULT(ui32, Limit, 10000);
    };
    struct TGetShardIteratorSettings : public NYdb::TOperationRequestSettings<TGetShardIteratorSettings> {
        FLUENT_SETTING(TString, StartingSequenceNumber);
        FLUENT_SETTING(ui64, Timestamp);
    };
    struct TSubscribeToShardSettings : public NYdb::TOperationRequestSettings<TSubscribeToShardSettings> {};
    struct TDescribeLimitsSettings : public NYdb::TOperationRequestSettings<TDescribeLimitsSettings> {};
    struct TDescribeStreamSummarySettings : public NYdb::TOperationRequestSettings<TDescribeStreamSummarySettings> {};
    struct TDecreaseStreamRetentionPeriodSettings : public NYdb::TOperationRequestSettings<TDecreaseStreamRetentionPeriodSettings> {
        FLUENT_SETTING(ui32, RetentionPeriodHours);
    };
    struct TIncreaseStreamRetentionPeriodSettings : public NYdb::TOperationRequestSettings<TIncreaseStreamRetentionPeriodSettings> {
        FLUENT_SETTING(ui32, RetentionPeriodHours);
    };
    struct TUpdateShardCountSettings : public NYdb::TOperationRequestSettings<TUpdateShardCountSettings> {
        FLUENT_SETTING(ui32, TargetShardCount);
    };
    struct TUpdateStreamModeSettings : public NYdb::TOperationRequestSettings<TUpdateStreamModeSettings> {
        FLUENT_SETTING_DEFAULT(EStreamMode, StreamMode, ESM_PROVISIONED);
    };
    struct TUpdateStreamSettings : public NYdb::TOperationRequestSettings<TUpdateStreamSettings> {
        FLUENT_SETTING(ui32, TargetShardCount);
        FLUENT_SETTING_OPTIONAL(ui32, RetentionPeriodHours);
        FLUENT_SETTING_OPTIONAL(ui32, RetentionStorageMegabytes);
        FLUENT_SETTING(ui64, WriteQuotaKbPerSec);
        FLUENT_SETTING_OPTIONAL(EStreamMode, StreamMode);

        FLUENT_SETTING_OPTIONAL(TPartitioningSettings, PartitioningSettings);
        TPartitioningSettingsBuilder<TUpdateStreamSettings> BeginConfigurePartitioningSettings();
    };
    struct TPutRecordSettings : public NYdb::TOperationRequestSettings<TPutRecordSettings> {};
    struct TPutRecordsSettings : public NYdb::TOperationRequestSettings<TPutRecordsSettings> {};
    struct TRegisterStreamConsumerSettings : public NYdb::TOperationRequestSettings<TRegisterStreamConsumerSettings> {};
    struct TDeregisterStreamConsumerSettings : public NYdb::TOperationRequestSettings<TDeregisterStreamConsumerSettings> {};
    struct TDescribeStreamConsumerSettings : public NYdb::TOperationRequestSettings<TDescribeStreamConsumerSettings> {};
    struct TListStreamConsumersSettings : public NYdb::TOperationRequestSettings<TListStreamConsumersSettings> {
        FLUENT_SETTING(ui32, MaxResults);
        FLUENT_SETTING(TString, NextToken);
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

        TAsyncCreateStreamResult CreateStream(const TString& path, TCreateStreamSettings settings = TCreateStreamSettings());
        TAsyncDeleteStreamResult DeleteStream(const TString& path, TDeleteStreamSettings settings = TDeleteStreamSettings());
        TAsyncDescribeStreamResult DescribeStream(const TString& path, TDescribeStreamSettings settings = TDescribeStreamSettings());
        TAsyncPutRecordResult PutRecord(const TString& path, const TDataRecord& record, TPutRecordSettings settings = TPutRecordSettings());
        TAsyncListStreamsResult ListStreams(TListStreamsSettings settings = TListStreamsSettings());
        TAsyncListShardsResult ListShards(const TString& path, const Ydb::DataStreams::V1::ShardFilter& shardFilter, TListShardsSettings settings = TListShardsSettings());
        TAsyncPutRecordsResult PutRecords(const TString& path, const std::vector<TDataRecord>& records, TPutRecordsSettings settings = TPutRecordsSettings());
        TAsyncGetRecordsResult GetRecords(const TString& shardIterator, TGetRecordsSettings settings = TGetRecordsSettings());
        TAsyncGetShardIteratorResult GetShardIterator(const TString& path, const TString& shardId, Ydb::DataStreams::V1::ShardIteratorType shardIteratorTypeStr,
                                                      TGetShardIteratorSettings settings = TGetShardIteratorSettings());
        // TAsyncSubscribeToShardResult SubscribeToShard(TSubscribeToShardSettings settings = TSubscribeToShardSettings());
        TAsyncDescribeLimitsResult DescribeLimits(TDescribeLimitsSettings settings = TDescribeLimitsSettings());
        TAsyncDescribeStreamSummaryResult DescribeStreamSummary(const TString& path, TDescribeStreamSummarySettings settings = TDescribeStreamSummarySettings());
        TAsyncDecreaseStreamRetentionPeriodResult DecreaseStreamRetentionPeriod(const TString& path, TDecreaseStreamRetentionPeriodSettings settings = TDecreaseStreamRetentionPeriodSettings());
        TAsyncIncreaseStreamRetentionPeriodResult IncreaseStreamRetentionPeriod(const TString& path, TIncreaseStreamRetentionPeriodSettings settings = TIncreaseStreamRetentionPeriodSettings());
        TAsyncUpdateShardCountResult UpdateShardCount(const TString& path, TUpdateShardCountSettings settings = TUpdateShardCountSettings());
        TAsyncUpdateStreamModeResult UpdateStreamMode(const TString& path, TUpdateStreamModeSettings settings = TUpdateStreamModeSettings());
        TAsyncRegisterStreamConsumerResult RegisterStreamConsumer(const TString& path, const TString& consumer_name, TRegisterStreamConsumerSettings settings = TRegisterStreamConsumerSettings());
        TAsyncDeregisterStreamConsumerResult DeregisterStreamConsumer(const TString& path, const TString& consumer_name, TDeregisterStreamConsumerSettings settings = TDeregisterStreamConsumerSettings());
        TAsyncDescribeStreamConsumerResult DescribeStreamConsumer(TDescribeStreamConsumerSettings settings = TDescribeStreamConsumerSettings());
        TAsyncListStreamConsumersResult ListStreamConsumers(const TString& path, TListStreamConsumersSettings settings = TListStreamConsumersSettings());
        TAsyncAddTagsToStreamResult AddTagsToStream(TAddTagsToStreamSettings settings = TAddTagsToStreamSettings());
        TAsyncDisableEnhancedMonitoringResult DisableEnhancedMonitoring(TDisableEnhancedMonitoringSettings settings = TDisableEnhancedMonitoringSettings());
        TAsyncEnableEnhancedMonitoringResult EnableEnhancedMonitoring(TEnableEnhancedMonitoringSettings settings = TEnableEnhancedMonitoringSettings());
        TAsyncListTagsForStreamResult ListTagsForStream(TListTagsForStreamSettings settings = TListTagsForStreamSettings());
        TAsyncMergeShardsResult MergeShards(TMergeShardsSettings settings = TMergeShardsSettings());
        TAsyncRemoveTagsFromStreamResult RemoveTagsFromStream(TRemoveTagsFromStreamSettings settings = TRemoveTagsFromStreamSettings());
        TAsyncSplitShardResult SplitShard(TSplitShardSettings settings = TSplitShardSettings());
        TAsyncStartStreamEncryptionResult StartStreamEncryption(TStartStreamEncryptionSettings settings = TStartStreamEncryptionSettings());
        TAsyncStopStreamEncryptionResult StopStreamEncryption(TStopStreamEncryptionSettings settings = TStopStreamEncryptionSettings());
        TAsyncUpdateStreamResult UpdateStream(const TString& streamName, TUpdateStreamSettings settings = TUpdateStreamSettings());

        template<class TProtoRequest, class TProtoResponse, class TProtoResult, class TMethod>
        NThreading::TFuture<TProtoResultWrapper<TProtoResult>> DoProtoRequest(const TProtoRequest& request, TMethod method, TProtoRequestSettings settings = TProtoRequestSettings());

        NThreading::TFuture<void> DiscoveryCompleted();

    private:
        std::shared_ptr<TImpl> Impl_;
    };

}

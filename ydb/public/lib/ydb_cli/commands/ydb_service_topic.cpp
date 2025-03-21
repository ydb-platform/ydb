#include <openssl/sha.h>

#include "ydb_service_topic.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/topic/topic_read.h>
#include <ydb/public/lib/ydb_cli/topic/topic_write.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/set.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/string/vector.h>
#include <util/string/join.h>

#define TIMESTAMP_FORMAT_OPTION_DESCRIPTION "Timestamp may be specified in unix time format (seconds from 1970.01.01) or in ISO-8601 format (like 2020-07-10T15:00:00Z)"

namespace NYdb::NConsoleClient {
    namespace {
        THashMap<NYdb::NTopic::ECodec, TString> CodecsDescriptions = {
            {NYdb::NTopic::ECodec::RAW, "Raw codec. No data compression."},
            {NYdb::NTopic::ECodec::GZIP, "GZIP codec. Data is compressed with GZIP compression algorithm."},
            {NYdb::NTopic::ECodec::LZOP, "LZOP codec. Data is compressed with LZOP compression algorithm."},
            {NYdb::NTopic::ECodec::ZSTD, "ZSTD codec. Data is compressed with ZSTD compression algorithm."},
        };

        THashMap<TString, NYdb::NTopic::ECodec> ExistingCodecs = {
            std::pair<TString, NYdb::NTopic::ECodec>("raw", NYdb::NTopic::ECodec::RAW),
            std::pair<TString, NYdb::NTopic::ECodec>("gzip", NYdb::NTopic::ECodec::GZIP),
            std::pair<TString, NYdb::NTopic::ECodec>("lzop", NYdb::NTopic::ECodec::LZOP),
            std::pair<TString, NYdb::NTopic::ECodec>("zstd", NYdb::NTopic::ECodec::ZSTD),
        };

        THashMap<TString, NTopic::EMeteringMode> ExistingMeteringModes = {
            std::pair<TString, NTopic::EMeteringMode>("request-units", NTopic::EMeteringMode::RequestUnits),
            std::pair<TString, NTopic::EMeteringMode>("reserved-capacity", NTopic::EMeteringMode::ReservedCapacity),
        };

        THashMap<NTopic::EMeteringMode, TString> MeteringModesDescriptions = {
            std::pair<NTopic::EMeteringMode, TString>(NTopic::EMeteringMode::ReservedCapacity, "Throughput and storage limits on hourly basis, write operations."),
            std::pair<NTopic::EMeteringMode, TString>(NTopic::EMeteringMode::RequestUnits, "Read/write operations valued in request units, storage usage on hourly basis."),
        };

        THashMap<TString, NTopic::EAutoPartitioningStrategy> AutoPartitioningStrategies = {
            std::pair<TString, NTopic::EAutoPartitioningStrategy>("disabled", NTopic::EAutoPartitioningStrategy::Disabled),
            std::pair<TString, NTopic::EAutoPartitioningStrategy>("up", NTopic::EAutoPartitioningStrategy::ScaleUp),
            std::pair<TString, NTopic::EAutoPartitioningStrategy>("up-and-down", NTopic::EAutoPartitioningStrategy::ScaleUpAndDown),
            std::pair<TString, NTopic::EAutoPartitioningStrategy>("paused", NTopic::EAutoPartitioningStrategy::Paused),
        };

        THashMap<NTopic::EAutoPartitioningStrategy, TString> AutoscaleStrategiesDescriptions = {
            std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::Disabled, "Automatic scaling of the number of partitions is disabled"),
            std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::ScaleUp, "The number of partitions can increase under high load, but cannot decrease"),
            std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::ScaleUpAndDown, "The number of partitions can increase under high load and decrease under low load"),
            std::pair<NTopic::EAutoPartitioningStrategy, TString>(NTopic::EAutoPartitioningStrategy::Paused, "Automatic scaling of the number of partitions is paused"),
        };

        THashMap<ETopicMetadataField, TString> TopicMetadataFieldsDescriptions = {
            {ETopicMetadataField::Body, "Message data"},
            {ETopicMetadataField::WriteTime, "Message write time, a UNIX timestamp the message was written to server."},
            {ETopicMetadataField::CreateTime, "Message creation time, a UNIX timestamp provided by the publishing client."},
            {ETopicMetadataField::MessageGroupID, "Message group id. All messages with the same message group id are guaranteed to be read in FIFO order."},
            {ETopicMetadataField::Offset, "Message offset. Offset orders messages in each partition."},
            {ETopicMetadataField::SeqNo, "Message sequence number, used for message deduplication when publishing."},
            {ETopicMetadataField::Meta, "Message additional metadata."},
        };

        const TVector<ETopicMetadataField> AllTopicMetadataFields = {
            ETopicMetadataField::Body,
            ETopicMetadataField::WriteTime,
            ETopicMetadataField::CreateTime,
            ETopicMetadataField::MessageGroupID,
            ETopicMetadataField::Offset,
            ETopicMetadataField::SeqNo,
            ETopicMetadataField::Meta,
        };

        const THashMap<TString, ETopicMetadataField> TopicMetadataFieldsMap = {
            {"body", ETopicMetadataField::Body},
            {"write_time", ETopicMetadataField::WriteTime},
            {"create_time", ETopicMetadataField::CreateTime},
            {"message_group_id", ETopicMetadataField::MessageGroupID},
            {"offset", ETopicMetadataField::Offset},
            {"seq_no", ETopicMetadataField::SeqNo},
            {"meta", ETopicMetadataField::Meta},
        };

        THashMap<ETransformBody, TString> TransformBodyDescriptions = {
            {ETransformBody::None, "No conversions, binary data on the client is exactly the same as it is in the topic message."},
            {ETransformBody::Base64, "Message on the client is a base64-encoded representation of the topic message."},
        };

        constexpr TDuration DefaultIdleTimeout = TDuration::Seconds(1);

        bool IsStreamingFormat(EMessagingFormat format) {
            return format == EMessagingFormat::NewlineDelimited || format == EMessagingFormat::Concatenated;
        }
    } // namespace

    std::function<void(const TString& opt)> TimestampOptionHandler(TMaybe<TInstant>* destination) {
        return [destination](const TString& opt) {
            ui64 seconds = 0;
            if (TryFromString(opt, seconds)) { // unix time
                destination->ConstructInPlace(TInstant::Seconds(seconds));
                return;
            }

            TInstant time;
            if (TInstant::TryParseIso8601(opt, time)) {
                destination->ConstructInPlace(time);
                return;
            }

            TStringBuilder err;
            err << "failed to parse \"" << opt << "\" as a timestamp. It must be either unix time format or ISO-8601 format";
            throw std::runtime_error(err);
        };
    }

    TString PrepareAllowedCodecsDescription(const TString& descriptionPrefix, const TVector<NTopic::ECodec>& codecs) {
        TStringStream description;
        description << descriptionPrefix << ". Available codecs: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& codec : codecs) {
            auto findResult = CodecsDescriptions.find(codec);
            Y_ABORT_UNLESS(findResult != CodecsDescriptions.end(),
                        "Couldn't find description for %s codec", (TStringBuilder() << codec).c_str());
            description << "\n  " << colors.BoldColor() << codec << colors.OldColor()
                        << "\n    " << findResult->second;
        }

        return description.Str();
    }

    namespace {
        NTopic::ECodec ParseCodec(const TString& codecStr, const TVector<NTopic::ECodec>& allowedCodecs) {
            auto exists = ExistingCodecs.find(to_lower(codecStr));
            if (exists == ExistingCodecs.end()) {
                throw TMisuseException() << "Codec " << codecStr << " is not available for this command";
            }

            if (std::find(allowedCodecs.begin(), allowedCodecs.end(), exists->second) == allowedCodecs.end()) {
                throw TMisuseException() << "Codec " << codecStr << " is not available for this command";
            }

            return exists->second;
        }
    }

    void TCommandWithSupportedCodecs::AddAllowedCodecs(TClientCommand::TConfig& config, const TVector<NYdb::NTopic::ECodec>& supportedCodecs) {
        TString description = PrepareAllowedCodecsDescription("Comma-separated list of supported codecs", supportedCodecs);
        config.Opts->AddLongOption("supported-codecs", description)
            .RequiredArgument("STRING")
            .StoreResult(&SupportedCodecsStr_);
        AllowedCodecs_ = supportedCodecs;
    }

    void TCommandWithSupportedCodecs::ParseCodecs() {
        TVector<NTopic::ECodec> parsedCodecs;
        TVector<TString> split = SplitString(SupportedCodecsStr_, ",");
        for (const TString& codecStr : split) {
            SupportedCodecs_.push_back(::NYdb::NConsoleClient::ParseCodec(codecStr, AllowedCodecs_));
        }
    }

    const TVector<NTopic::ECodec> TCommandWithSupportedCodecs::GetCodecs() {
        return SupportedCodecs_;
    }

    void TCommandWithMeteringMode::AddAllowedMeteringModes(TClientCommand::TConfig& config) {
        TStringStream description;
        description << "Topic metering for serverless databases pricing. Available metering modes: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& mode: ExistingMeteringModes) {
            auto findResult = MeteringModesDescriptions.find(mode.second);
            Y_ABORT_UNLESS(findResult != MeteringModesDescriptions.end(),
                     "Couldn't find description for %s metering mode", (TStringBuilder() << mode.second).c_str());
            description << "\n  " << colors.BoldColor() << mode.first << colors.OldColor()
                        << "\n    " << findResult->second;
            if (mode.second == NTopic::EMeteringMode::RequestUnits) {
                description << colors.CyanColor() << " (default)" << colors.OldColor();
            }
        }
        config.Opts->AddLongOption("metering-mode", description.Str())
            .Optional()
            .StoreResult(&MeteringModeStr_);
    }

    void TCommandWithMeteringMode::ParseMeteringMode() {
        if (MeteringModeStr_.empty()) {
            return;
        }

        TString toLowerMeteringMode = to_lower(MeteringModeStr_);
        if (toLowerMeteringMode == "reserved-capacity") {
            MeteringMode_ = NTopic::EMeteringMode::ReservedCapacity;
            return;
        }
        if (toLowerMeteringMode == "request-units") {
            MeteringMode_ = NTopic::EMeteringMode::RequestUnits;
            return;
        }

        throw TMisuseException() << "Metering mode " << MeteringModeStr_ << " is not available for this command";
    }

    NTopic::EMeteringMode TCommandWithMeteringMode::GetMeteringMode() const {
        return MeteringMode_;
    }

    void TCommandWithAutoPartitioning::AddAutoPartitioning(TClientCommand::TConfig& config, bool isAlter) {
        TStringStream description;
        description << "A strategy to automatically change the number of partitions depending on the load. Available strategies: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& strategy: AutoPartitioningStrategies) {
            auto findResult = AutoscaleStrategiesDescriptions.find(strategy.second);
            Y_ABORT_UNLESS(findResult != AutoscaleStrategiesDescriptions.end(),
                     "Couldn't find description for %s autoscale strategy", (TStringBuilder() << strategy.second).c_str());
            description << "\n  " << colors.BoldColor() << strategy.first << colors.OldColor()
                        << "\n    " << findResult->second;
        }

        if (isAlter) {
            config.Opts->AddLongOption("auto-partitioning-strategy", description.Str())
                .Optional()
                .StoreResult(&AutoPartitioningStrategyStr_);
            config.Opts->AddLongOption("auto-partitioning-stabilization-window-seconds", "Duration in seconds of high or low load before automatically scale the number of partitions")
                .Optional()
                .StoreResult(&ScaleThresholdTime_);
            config.Opts->AddLongOption("auto-partitioning-up-utilization-percent", "The load percentage at which the number of partitions will increase")
                .Optional()
                .StoreResult(&ScaleUpThresholdPercent_);
            config.Opts->AddLongOption("auto-partitioning-down-utilization-percent", "The load percentage at which the number of partitions will decrease")
                .Optional()
                .StoreResult(&ScaleDownThresholdPercent_);
        } else {
            config.Opts->AddLongOption("auto-partitioning-strategy", description.Str())
                .Optional()
                .DefaultValue("disabled")
                .StoreResult(&AutoPartitioningStrategyStr_);
            config.Opts->AddLongOption("auto-partitioning-stabilization-window-seconds", "Duration in seconds of high or low load before automatically scale the number of partitions")
                .Optional()
                .DefaultValue(300)
                .StoreResult(&ScaleThresholdTime_);
            config.Opts->AddLongOption("auto-partitioning-up-utilization-percent", "The load percentage at which the number of partitions will increase")
                .Optional()
                .DefaultValue(90)
                .StoreResult(&ScaleUpThresholdPercent_);
            config.Opts->AddLongOption("auto-partitioning-down-utilization-percent", "The load percentage at which the number of partitions will decrease")
                .Optional()
                .DefaultValue(30)
                .StoreResult(&ScaleDownThresholdPercent_);
        }
    }

    void TCommandWithAutoPartitioning::ParseAutoPartitioningStrategy() {
        if (AutoPartitioningStrategyStr_.empty()) {
            return;
        }

        TString toLowerStrategy = to_lower(AutoPartitioningStrategyStr_);
        auto strategyIt = AutoPartitioningStrategies.find(toLowerStrategy);
        if (strategyIt.IsEnd()) {
            throw TMisuseException() << "Auto partitioning strategy " << AutoPartitioningStrategyStr_ << " is not available for this command";
        } else {
            AutoPartitioningStrategy_ = strategyIt->second;
        }
    }

    TMaybe<NTopic::EAutoPartitioningStrategy> TCommandWithAutoPartitioning::GetAutoPartitioningStrategy() const {
        return AutoPartitioningStrategy_;
    }

    TMaybe<ui32> TCommandWithAutoPartitioning::GetAutoPartitioningStabilizationWindowSeconds() const {
        return ScaleThresholdTime_;
    }

    TMaybe<ui32> TCommandWithAutoPartitioning::GetAutoPartitioningUpUtilizationPercent() const {
        return ScaleUpThresholdPercent_;
    }

    TMaybe<ui32> TCommandWithAutoPartitioning::GetAutoPartitioninDownUtilizationPercent() const {
        return ScaleDownThresholdPercent_;
    }

    TCommandTopic::TCommandTopic()
        : TClientCommandTree("topic", {}, "TopicService operations") {
        AddCommand(std::make_unique<TCommandTopicCreate>());
        AddCommand(std::make_unique<TCommandTopicAlter>());
        AddCommand(std::make_unique<TCommandTopicDrop>());
        AddCommand(std::make_unique<TCommandTopicConsumer>());
        AddCommand(std::make_unique<TCommandTopicRead>());
        AddCommand(std::make_unique<TCommandTopicWrite>());
    }

    TCommandTopicCreate::TCommandTopicCreate()
        : TYdbCommand("create", {}, "Create topic command") {
    }

    void TCommandTopicCreate::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("partitions-count", "Initial and minimum number of partitions for topic")
            .Optional()
            .StoreResult(&MinActivePartitions_)
            .DefaultValue(1);
        config.Opts->AddLongOption("retention-period-hours", "Duration in hours for which data in topic is stored")
            .DefaultValue(24)
            .Optional()
            .StoreResult(&RetentionPeriodHours_);
        config.Opts->AddLongOption("partition-write-speed-kbps", "Partition write speed in kilobytes per second")
            .DefaultValue(1024)
            .Optional()
            .StoreResult(&PartitionWriteSpeedKbps_);
        config.Opts->AddLongOption("retention-storage-mb", "Storage retention in megabytes")
            .DefaultValue(0)
            .Optional()
            .StoreResult(&RetentionStorageMb_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
        AddAllowedCodecs(config, AllowedCodecs);
        AddAllowedMeteringModes(config);

        config.Opts->AddLongOption("auto-partitioning-max-partitions-count", "Maximum number of partitions for topic")
            .Optional()
            .StoreResult(&MaxActivePartitions_);
        AddAutoPartitioning(config, false);

        config.Opts->AddLongOption("partitions-per-tablet", "Partitions per PQ tablet")
            .Optional()
            .Hidden()
            .StoreResult(&PartitionsPerTablet_);
    }

    void TCommandTopicCreate::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseCodecs();
        ParseMeteringMode();
        ParseAutoPartitioningStrategy();
    }

    int TCommandTopicCreate::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto settings = NYdb::NTopic::TCreateTopicSettings();

        auto autoscaleSettings = NTopic::TAutoPartitioningSettings(
        GetAutoPartitioningStrategy() ? *GetAutoPartitioningStrategy() : NTopic::EAutoPartitioningStrategy::Disabled,
        GetAutoPartitioningStabilizationWindowSeconds() ? TDuration::Seconds(*GetAutoPartitioningStabilizationWindowSeconds()) : TDuration::Seconds(0),
        GetAutoPartitioninDownUtilizationPercent() ? *GetAutoPartitioninDownUtilizationPercent() : 0,
        GetAutoPartitioningUpUtilizationPercent() ? *GetAutoPartitioningUpUtilizationPercent() : 0);

        ui32 finalMaxActivePartitions = MaxActivePartitions_.Defined() ? *MaxActivePartitions_
                                      : autoscaleSettings.GetStrategy() != NTopic::EAutoPartitioningStrategy::Disabled ? MinActivePartitions_ + 50
                                      : MinActivePartitions_;

        settings.PartitioningSettings(MinActivePartitions_, finalMaxActivePartitions, autoscaleSettings);
        settings.PartitionWriteBurstBytes(PartitionWriteSpeedKbps_ * 1_KB);
        settings.PartitionWriteSpeedBytesPerSecond(PartitionWriteSpeedKbps_ * 1_KB);

        auto codecs = GetCodecs();
        if (!codecs.empty()) {
            settings.SetSupportedCodecs(codecs);
        }

        if (GetMeteringMode() != NTopic::EMeteringMode::Unspecified) {
            settings.MeteringMode(GetMeteringMode());
        }

        settings.RetentionPeriod(TDuration::Hours(RetentionPeriodHours_));
        settings.RetentionStorageMb(RetentionStorageMb_);

        if (PartitionsPerTablet_.Defined()) {
            settings.AddAttribute("_partitions_per_tablet", ToString(*PartitionsPerTablet_));
        }

        auto status = topicClient.CreateTopic(TopicName, settings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicAlter::TCommandTopicAlter()
        : TYdbCommand("alter", {}, "Alter topic command") {
    }

    void TCommandTopicAlter::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("partitions-count", "Initial and minimum number of partitions for topic")
            .Optional()
            .StoreResult(&MinActivePartitions_);
        config.Opts->AddLongOption("retention-period-hours", "Duration for which data in topic is stored")
            .Optional()
            .StoreResult(&RetentionPeriodHours_);
        config.Opts->AddLongOption("partition-write-speed-kbps", "Partition write speed in kilobytes per second")
            .Optional()
            .StoreResult(&PartitionWriteSpeedKbps_);
        config.Opts->AddLongOption("retention-storage-mb", "Storage retention in megabytes")
            .Optional()
            .StoreResult(&RetentionStorageMb_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
        AddAllowedCodecs(config, AllowedCodecs);
        AddAllowedMeteringModes(config);

        config.Opts->AddLongOption("auto-partitioning-max-partitions-count", "Maximum number of partitions for topic")
            .Optional()
            .StoreResult(&MaxActivePartitions_);
        AddAutoPartitioning(config, true);
    }

    void TCommandTopicAlter::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseCodecs();
        ParseMeteringMode();
        ParseAutoPartitioningStrategy();
    }

    NYdb::NTopic::TAlterTopicSettings TCommandTopicAlter::PrepareAlterSettings(
        NYdb::NTopic::TDescribeTopicResult& describeResult) {
        auto settings = NYdb::NTopic::TAlterTopicSettings();
        auto& partitioningSettings = settings.BeginAlterPartitioningSettings();
        auto& originPartitioningSettings = describeResult.GetTopicDescription().GetPartitioningSettings();

        if (MinActivePartitions_.Defined() && (*MinActivePartitions_ != originPartitioningSettings.GetMinActivePartitions())) {
            partitioningSettings.MinActivePartitions(*MinActivePartitions_);
        }

        if (MaxActivePartitions_.Defined() && (*MaxActivePartitions_ != originPartitioningSettings.GetMaxActivePartitions())) {
            partitioningSettings.MaxActivePartitions(*MaxActivePartitions_);
        }

        auto& autoPartitioningSettings = partitioningSettings.BeginAlterAutoPartitioningSettings();
        const auto& originalAutoPartitioningSettings = originPartitioningSettings.GetAutoPartitioningSettings();

        if (GetAutoPartitioningStabilizationWindowSeconds().Defined() && *GetAutoPartitioningStabilizationWindowSeconds() != originalAutoPartitioningSettings.GetStabilizationWindow().Seconds()) {
            autoPartitioningSettings.StabilizationWindow(TDuration::Seconds(*GetAutoPartitioningStabilizationWindowSeconds()));
        }

        if (GetAutoPartitioningStrategy().Defined() && *GetAutoPartitioningStrategy() != originalAutoPartitioningSettings.GetStrategy()) {
            autoPartitioningSettings.Strategy(*GetAutoPartitioningStrategy());
        }

        if (GetAutoPartitioninDownUtilizationPercent().Defined() && *GetAutoPartitioninDownUtilizationPercent() != originalAutoPartitioningSettings.GetDownUtilizationPercent()) {
            autoPartitioningSettings.DownUtilizationPercent(*GetAutoPartitioninDownUtilizationPercent());
        }

        if (GetAutoPartitioningUpUtilizationPercent().Defined() && *GetAutoPartitioningUpUtilizationPercent() != originalAutoPartitioningSettings.GetUpUtilizationPercent()) {
            autoPartitioningSettings.UpUtilizationPercent(*GetAutoPartitioningUpUtilizationPercent());
        }

        auto codecs = GetCodecs();
        if (!codecs.empty()) {
            settings.SetSupportedCodecs(codecs);
        }

        if (RetentionPeriodHours_.Defined() && describeResult.GetTopicDescription().GetRetentionPeriod() != TDuration::Hours(*RetentionPeriodHours_)) {
            settings.SetRetentionPeriod(TDuration::Hours(*RetentionPeriodHours_));
        }

        if (PartitionWriteSpeedKbps_.Defined() && describeResult.GetTopicDescription().GetPartitionWriteSpeedBytesPerSecond() / 1_KB != *PartitionWriteSpeedKbps_) {
            settings.SetPartitionWriteSpeedBytesPerSecond(*PartitionWriteSpeedKbps_ * 1_KB);
            settings.SetPartitionWriteBurstBytes(*PartitionWriteSpeedKbps_ * 1_KB);
        }

        if (GetMeteringMode() != NTopic::EMeteringMode::Unspecified && GetMeteringMode() != describeResult.GetTopicDescription().GetMeteringMode()) {
            settings.SetMeteringMode(GetMeteringMode());
        }

        if (RetentionStorageMb_.Defined() && describeResult.GetTopicDescription().GetRetentionStorageMb() != *RetentionStorageMb_) {
            settings.SetRetentionStorageMb(*RetentionStorageMb_);
        }

        return settings;
    }

    int TCommandTopicAlter::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto topicDescription = topicClient.DescribeTopic(TopicName, {}).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(topicDescription);

        auto describeResult = topicClient.DescribeTopic(TopicName).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(describeResult);

        auto settings = PrepareAlterSettings(describeResult);
        auto result = topicClient.AlterTopic(TopicName, settings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        return EXIT_SUCCESS;
    }

    TCommandTopicDrop::TCommandTopicDrop()
        : TYdbCommand("drop", {}, "Drop topic command") {
    }

    void TCommandTopicDrop::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
    }

    void TCommandTopicDrop::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
    }

    int TCommandTopicDrop::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NTopic::TTopicClient topicClient(driver);

        auto topicDescription = topicClient.DescribeTopic(TopicName, {}).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(topicDescription);

        auto settings = NYdb::NTopic::TDropTopicSettings();
        TStatus status = topicClient.DropTopic(TopicName, settings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicConsumer::TCommandTopicConsumer()
        : TClientCommandTree("consumer", {}, "Consumer operations") {
        AddCommand(std::make_unique<TCommandTopicConsumerAdd>());
        AddCommand(std::make_unique<TCommandTopicConsumerDrop>());
        AddCommand(std::make_unique<TCommandTopicConsumerDescribe>());
        AddCommand(std::make_unique<TCommandTopicConsumerOffset>());
    }

    TCommandTopicConsumerOffset::TCommandTopicConsumerOffset()
        : TClientCommandTree("offset", {}, "Consumer offset operations") {
        AddCommand(std::make_unique<TCommandTopicConsumerCommitOffset>());
    }


    TCommandTopicConsumerAdd::TCommandTopicConsumerAdd()
        : TYdbCommand("add", {}, "Consumer add operation") {
    }

    void TCommandTopicConsumerAdd::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("consumer", "New consumer for topic")
            .Required()
            .StoreResult(&ConsumerName_);
        config.Opts->AddLongOption("starting-message-timestamp", "'Written_at' timestamp from which read is allowed. " TIMESTAMP_FORMAT_OPTION_DESCRIPTION)
            .RequiredArgument("TIMESTAMP")
            .Optional()
            .Handler1T<TString>(TimestampOptionHandler(&StartingMessageTimestamp_));
        config.Opts->AddLongOption("important", "Is consumer important")
            .Optional()
            .DefaultValue(false)
            .StoreResult(&IsImportant_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
        AddAllowedCodecs(config, AllowedCodecs);
    }

    void TCommandTopicConsumerAdd::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseCodecs();
        ParseTopicName(config, 0);
    }

    int TCommandTopicConsumerAdd::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NTopic::TTopicClient topicClient(driver);

        auto topicDescription = topicClient.DescribeTopic(TopicName, {}).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(topicDescription);

        NYdb::NTopic::TAlterTopicSettings readRuleSettings = NYdb::NTopic::TAlterTopicSettings();
        NYdb::NTopic::TConsumerSettings<NYdb::NTopic::TAlterTopicSettings> consumerSettings(readRuleSettings);
        consumerSettings.ConsumerName(ConsumerName_);
        if (StartingMessageTimestamp_.Defined()) {
            consumerSettings.ReadFrom(*StartingMessageTimestamp_);
        }

        auto codecs = GetCodecs();
        if (!codecs.empty()) {
            consumerSettings.SetSupportedCodecs(codecs);
        }
        consumerSettings.SetImportant(IsImportant_);

        readRuleSettings.AppendAddConsumers(consumerSettings);

        TStatus status = topicClient.AlterTopic(TopicName, readRuleSettings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicConsumerDrop::TCommandTopicConsumerDrop()
        : TYdbCommand("drop", {}, "Consumer drop operation") {
    }

    void TCommandTopicConsumerDrop::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("consumer", "Consumer which will be dropped")
            .Required()
            .StoreResult(&ConsumerName_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
    }

    void TCommandTopicConsumerDrop::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
    }

    int TCommandTopicConsumerDrop::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto topicDescription = topicClient.DescribeTopic(TopicName, {}).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(topicDescription);

        auto consumers = topicDescription.GetTopicDescription().GetConsumers();
        if (!std::any_of(consumers.begin(), consumers.end(), [&](const auto& consumer) { return consumer.GetConsumerName() == ConsumerName_; }))
        {
            throw TMisuseException() << "Topic '" << TopicName << "' doesn't have a consumer '" << ConsumerName_ << "'.\n";
            return EXIT_FAILURE;
        }

        NYdb::NTopic::TAlterTopicSettings removeReadRuleSettings = NYdb::NTopic::TAlterTopicSettings();
        removeReadRuleSettings.AppendDropConsumers(ConsumerName_);

        TStatus status = topicClient.AlterTopic(TopicName, removeReadRuleSettings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicConsumerDescribe::TCommandTopicConsumerDescribe()
        : TYdbCommand("describe", {}, "Consumer describe operation") {
    }

    void TCommandTopicConsumerDescribe::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("consumer", "Consumer to describe")
            .Required()
            .StoreResult(&ConsumerName_);
        config.Opts->AddLongOption("partition-stats", "Show partition statistics")
            .StoreTrue(&ShowPartitionStats_);
        config.Opts->SetFreeArgsNum(1);
        AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::ProtoJsonBase64 });
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
    }

    void TCommandTopicConsumerDescribe::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseOutputFormats();
        ParseTopicName(config, 0);
    }

    int TCommandTopicConsumerDescribe::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto consumerDescription = topicClient.DescribeConsumer(TopicName, ConsumerName_, NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(ShowPartitionStats_)).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(consumerDescription);

        return PrintDescription(this, OutputFormat, consumerDescription.GetConsumerDescription(), &TCommandTopicConsumerDescribe::PrintPrettyResult);
    }

    int TCommandTopicConsumerDescribe::PrintPrettyResult(const NYdb::NTopic::TConsumerDescription& description) const {
        return PrintPrettyDescribeConsumerResult(description, ShowPartitionStats_);
    }

    TCommandTopicConsumerCommitOffset::TCommandTopicConsumerCommitOffset()
        : TYdbCommand("commit", {}, "Commit offset for consumer") {
    }

    void TCommandTopicConsumerCommitOffset::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("consumer", "Consumer which offset will be changed")
            .Required()
            .StoreResult(&ConsumerName_);

        config.Opts->AddLongOption("partition", "Partition which offset will be changed")
            .Required()
            .StoreResult(&PartitionId_);

        config.Opts->AddLongOption("offset", "Partition offset will be set for desired consumer")
            .Required()
            .StoreResult(&Offset_);

        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");
    }

    void TCommandTopicConsumerCommitOffset::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
    }

    int TCommandTopicConsumerCommitOffset::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto topicDescription = topicClient.DescribeTopic(TopicName, {}).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(topicDescription);

        auto consumers = topicDescription.GetTopicDescription().GetConsumers();
        if (!std::any_of(consumers.begin(), consumers.end(), [&](const auto& consumer) { return consumer.GetConsumerName() == ConsumerName_; }))
        {
            throw TMisuseException() << "Topic '" << TopicName << "' doesn't have a consumer '" << ConsumerName_ << "'.\n";
            return EXIT_FAILURE;
        }

        TStatus status = topicClient.CommitOffset(TopicName, PartitionId_, ConsumerName_, Offset_).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(status);
        return EXIT_SUCCESS;
    }

    void TCommandWithTransformBody::AddTransform(TClientCommand::TConfig& config) {
        TStringStream description;
        description << "Conversion between a message data in the topic and the client filesystem/terminal. Available options: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& iter : TransformBodyDescriptions) {
            description << "\n  " << colors.BoldColor() << iter.first << colors.OldColor() << "\n    " << iter.second;
        }

        config.Opts->AddLongOption("transform", description.Str())
            .Optional()
            .DefaultValue("none")
            .StoreResult(&TransformStr_);
    }

    void TCommandWithTransformBody::ParseTransform() {
        if (TransformStr_.empty()) {
            return;
        }

        TString val = TransformStr_;
        if (val == (TStringBuilder() << ETransformBody::None)) {
            return;
        }
        if (val == (TStringBuilder() << ETransformBody::Base64)) {
            Transform_ = ETransformBody::Base64;
            return;
        }

        throw TMisuseException() << "Transform " << TransformStr_ << " not found in available \"transform\" values";
    }

    ETransformBody TCommandWithTransformBody::GetTransform() const {
        return Transform_;
    }

    TCommandTopicRead::TCommandTopicRead()
        : TYdbCommand("read", {}, "Read from a topic to the client filesystem or terminal") {
    }

    void TCommandTopicRead::AddAllowedMetadataFields(TConfig& config) {
        TStringStream description;
        description << "Comma-separated list of message fields to print in Pretty format. If not specified, all fields are printed. Available fields: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& iter : TopicMetadataFieldsDescriptions) {
            description << "\n  " << colors.BoldColor() << iter.first << colors.OldColor() << "\n    " << iter.second;
        }

        config.Opts->AddLongOption("metadata-fields", description.Str())
            .Optional()
            .StoreResult(&WithMetadataFields_);
    }

    void TCommandTopicRead::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");

        AddMessagingFormats(config, {
                               EMessagingFormat::SingleMessage,
                               EMessagingFormat::Pretty,
                               EMessagingFormat::NewlineDelimited,
                               EMessagingFormat::Concatenated,
                           });

        // TODO(shmel1k@): improve help.
        config.Opts->AddLongOption('c', "consumer", "Consumer name. If not set, then you need to specify partitions through --partition-ids to read without consumer")
            .Optional()
            .StoreResult(&Consumer_);

        config.Opts->AddLongOption('f', "file", "File to write data to. In not specified, data is written to the standard output.")
            .Optional()
            .StoreResult(&File_);
        config.Opts->AddLongOption("idle-timeout", "Max wait duration for new messages. Topic is considered empty if no new messages arrive within this period.")
            .Optional()
            .DefaultValue(DefaultIdleTimeout)
            .StoreResult(&IdleTimeout_);
        config.Opts->AddLongOption("commit", "Commit messages after successful read")
            .Optional()
            .DefaultValue(false)
            .StoreResult(&Commit_);
        config.Opts->AddLongOption("limit", "Limit on message count to read, 0 - unlimited. "
                                            "If avobe 0, processing stops when either topic is empty, or the specified limit reached. "
                                            "Must be above 0 for pretty output format."
                                            "\nDefault is 10 for pretty format, unlimited for streaming formats.")
            .Optional()
            .StoreResult(&Limit_);
        config.Opts->AddLongOption('w', "wait", "Wait indefinitely for a first message received. If not specified, command exits on empty topic returning no data to the output.")
            .Optional()
            .NoArgument()
            .StoreValue(&Wait_, true);
        config.Opts->AddLongOption("timestamp", "'Written_at' timestamp from which messages will be read. If not specified, messages are read from the last commit point for the chosen consumer. " TIMESTAMP_FORMAT_OPTION_DESCRIPTION)
            .RequiredArgument("TIMESTAMP")
            .Optional()
            .Handler1T<TString>(TimestampOptionHandler(&Timestamp_));
        config.Opts->AddLongOption("partition-ids", "Comma separated list of partition ids to read from. If not specified, messages are read from all partitions. E.g. \"--partition-ids 0,1,10\"")
            .Optional()
            .SplitHandler(&PartitionIds_, ',');

        AddAllowedMetadataFields(config);
        AddTransform(config);
    }

    void TCommandTopicRead::ParseMetadataFields() {
        MetadataFields_ = AllTopicMetadataFields;

        // TODO(shmel1k@): discuss: disable all fields?
        if (WithMetadataFields_ == "all") {
            return;
        }

        TVector<TString> split = SplitString(WithMetadataFields_, ",");
        if (split.empty()) {
            return;
        }

        TSet<ETopicMetadataField> set;
        for (const auto& field : split) {
            auto f = TopicMetadataFieldsMap.find(field);
            if (f == TopicMetadataFieldsMap.end()) {
                throw TMisuseException() << "Field " << field << " not found in available fields"; // TODO(shmel1k@): improve message.
            }
            set.insert(f->second);
        }

        TVector<ETopicMetadataField> result;
        result.reserve(set.size());
        // NOTE(shmel1k@): preserving the order from AllMetadataFields
        for (const auto metadataField : set) {
            auto f = std::find(AllTopicMetadataFields.begin(), AllTopicMetadataFields.end(), metadataField);
            if (f == AllTopicMetadataFields.end()) {
                continue;
            }
            result.push_back(metadataField);
        }

        MetadataFields_ = result;
    }

    void TCommandTopicRead::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseMessagingFormats();
        ParseMetadataFields();
        ParseTransform();
    }

    NTopic::TReadSessionSettings TCommandTopicRead::PrepareReadSessionSettings() {
        NTopic::TReadSessionSettings settings;
        settings.AutoPartitioningSupport(true);
        if (Consumer_) {
            settings.ConsumerName(Consumer_);
        } else {
            settings.WithoutConsumer();
        }
        // settings.ReadAll(); // TODO(shmel1k@): change to read only original?
        if (Timestamp_.Defined()) {
            settings.ReadFromTimestamp(*Timestamp_);
        }

        // TODO(shmel1k@): partition can be added here.
        NTopic::TTopicReadSettings readSettings;
        readSettings.Path(TopicName);
        for (ui64 id : PartitionIds_) {
            readSettings.AppendPartitionIds(id);
        }

        settings.AppendTopics(std::move(readSettings));

        // This check was added for the static analyzer.
        Y_ABORT_UNLESS(!settings.EventHandlers_.DataReceivedHandler_);

        return settings;
    }

    void TCommandTopicRead::ValidateConfig() {
        // TODO(shmel1k@): add more formats.
        if (!IsStreamingFormat(MessagingFormat) && (Limit_.Defined() && (Limit_ <= 0 || Limit_ > 500))) {
            throw TMisuseException() << "OutputFormat " << MessagingFormat << " is not compatible with "
                                     << "limit less and equal '0' or more than '500': '" << *Limit_ << "' was given";
        }

        // validate partitions ids are specified, if no consumer is provided. no-consumer mode will be used. 
        if (!Consumer_ && !PartitionIds_) {
            throw TMisuseException() << "Please specify either --consumer or --partition-ids to read without consumer";
        }
    }

    int TCommandTopicRead::Run(TConfig& config) {
        ValidateConfig();

        auto driver =
            std::make_unique<TDriver>(CreateDriver(config, std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)).Release())));
        NTopic::TTopicClient topicClient(*driver);

        auto readSession = topicClient.CreateReadSession(PrepareReadSessionSettings());

        {
            TTopicReader reader = TTopicReader(std::move(readSession), TTopicReaderSettings(
                                                                           Limit_,
                                                                           Commit_,
                                                                           Wait_,
                                                                           MessagingFormat,
                                                                           MetadataFields_,
                                                                           GetTransform(),
                                                                           IdleTimeout_));

            reader.Init();

            int status = 0;
            if (File_.Defined()) {
                TFileOutput out(*File_);
                status = reader.Run(out);
                reader.Close(out);
            } else {
                status = reader.Run(Cout);
                reader.Close(Cout);
            }
            if (status) {
                return status;
            }
        }

        driver->Stop(true);

        return EXIT_SUCCESS;
    }

    void TCommandWithCodec::AddAllowedCodecs(TClientCommand::TConfig& config, const TVector<NTopic::ECodec>& allowedCodecs) {
        TString description = PrepareAllowedCodecsDescription("Client-side compression algorithm. When read, data will be uncompressed transparently with a codec used on write", allowedCodecs);
        config.Opts->AddLongOption("codec", description)
            .Optional()
            .DefaultValue("RAW")
            .StoreResult(&CodecStr_);
        AllowedCodecs_ = allowedCodecs;
    }

    void TCommandWithCodec::ParseCodec() {
        if (CodecStr_.empty()) {
            return;
        }

        Codec_ = ::NYdb::NConsoleClient::ParseCodec(CodecStr_, AllowedCodecs_);
    }

    TMaybe<NTopic::ECodec> TCommandWithCodec::GetCodec() const {
        return Codec_;
    }

    TCommandTopicWrite::TCommandTopicWrite()
        : TYdbCommand("write", {}, "Write to topic command") {
    }

    void TCommandTopicWrite::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic path");

        AddMessagingFormats(config, {
                                    EMessagingFormat::NewlineDelimited,
                                    EMessagingFormat::SingleMessage,
                                    //      EDataFormat::JsonRawStreamConcat,
                                    //      EDataFormat::JsonRawArray,
                                });
        AddAllowedCodecs(config, AllowedCodecs);

        // TODO(shmel1k@): improve help.
        config.Opts->AddLongOption('d', "delimiter", "Delimiter to split messages")
            .Optional()
            .StoreResult(&Delimiter_);
        config.Opts->AddLongOption('f', "file", "File to read data from")
            .Optional()
            .StoreResult(&File_);
        config.Opts->AddLongOption("message-group-id", "Message group identifier. If not set, all messages from input will get the same identifier based on hex string\nrepresentation of 3 random bytes")
            .Optional()
            .StoreResult(&MessageGroupId_);

        AddTransform(config);
    }

    void TCommandTopicWrite::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseMessagingFormats();
        ParseTransform();
        ParseCodec();

        if (Delimiter_.Defined() && MessagingFormat != EMessagingFormat::SingleMessage) {
            throw TMisuseException() << "Both mutually exclusive options \"delimiter\"(\"--delimiter\", \"-d\" "
                                     << "and \"input format\"(\"--input-format\") were provided.";
        }
    }

    NTopic::TWriteSessionSettings TCommandTopicWrite::PrepareWriteSessionSettings() {
        NTopic::TWriteSessionSettings settings;
        if (auto codec = GetCodec(); codec.Defined()) {
            settings.Codec(*codec);
        }
        settings.Path(TopicName);

        if (!MessageGroupId_.Defined()) {
            const TString rnd = ToString(TInstant::Now().NanoSeconds());
            SHA_CTX ctx;
            SHA1_Init(&ctx);
            SHA1_Update(&ctx, rnd.data(), rnd.size());
            unsigned char sha1[SHA_DIGEST_LENGTH];
            SHA1_Final(sha1, &ctx);

            TString hex = HexEncode(TString(reinterpret_cast<const char*>(sha1), SHA_DIGEST_LENGTH));
            hex.to_lower();
            MessageGroupId_ = TString(hex.begin(), hex.begin() + 6);
        }

        settings.MessageGroupId(*MessageGroupId_);
        settings.ProducerId(*MessageGroupId_);

        // This check was added for the static analyzer.
        Y_ABORT_UNLESS(!settings.EventHandlers_.AcksHandler_);

        return settings;
    }

    int TCommandTopicWrite::Run(TConfig& config) {
        SetInterruptHandlers();

        auto driver =
            std::make_unique<TDriver>(CreateDriver(config, std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", TClientCommand::TConfig::VerbosityLevelToELogPriority(config.VerbosityLevel)).Release())));
        NTopic::TTopicClient topicClient(*driver);

        {
            auto writeSession = NTopic::TTopicClient(*driver).CreateWriteSession(std::move(PrepareWriteSessionSettings()));
            auto writer =
                TTopicWriter(writeSession, std::move(TTopicWriterParams(MessagingFormat, Delimiter_, MessageSizeLimit_, BatchDuration_,
                                                                        BatchSize_, BatchMessagesCount_, GetTransform())));

            if (int status = writer.Init(); status) {
                return status;
            }

            int status = 0;
            if (File_.Defined()) {
                TFileInput input(*File_);
                status = writer.Run(input);
            } else {
                status = writer.Run(Cin);
            }
            if (status) {
                return status;
            }

            if (!writer.Close()) {
                Cerr << "Failed to close session" << Endl;
                return EXIT_FAILURE;
            }
        }

        driver->Stop(true);
        return EXIT_SUCCESS;
    }

} // namespace NYdb::NConsoleClient

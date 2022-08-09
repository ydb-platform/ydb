#include "ydb_service_topic.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <util/stream/str.h>
#include <util/string/vector.h>

namespace NYdb::NConsoleClient {
    namespace {
        THashMap<NYdb::NTopic::ECodec, TString> CodecsDescriptions = {
            {NYdb::NTopic::ECodec::RAW, "Raw codec. No data compression"},
            {NYdb::NTopic::ECodec::GZIP, "GZIP codec. Data is compressed with GZIP compression algorithm"},
            {NYdb::NTopic::ECodec::LZOP, "LZOP codec. Data is compressed with LZOP compression algorithm"},
            {NYdb::NTopic::ECodec::ZSTD, "ZSTD codec. Data is compressed with ZSTD compression algorithm"},
        };

        THashMap<TString, NYdb::NTopic::ECodec> ExistingCodecs = {
            std::pair<TString, NYdb::NTopic::ECodec>("raw", NYdb::NTopic::ECodec::RAW),
            std::pair<TString, NYdb::NTopic::ECodec>("gzip", NYdb::NTopic::ECodec::GZIP),
            std::pair<TString, NYdb::NTopic::ECodec>("lzop", NYdb::NTopic::ECodec::LZOP),
            std::pair<TString, NYdb::NTopic::ECodec>("zstd", NYdb::NTopic::ECodec::ZSTD),
        };
    } // namespace

    void TCommandWithSupportedCodecs::AddAllowedCodecs(TClientCommand::TConfig& config, const TVector<NTopic::ECodec>& supportedCodecs) {
        TStringStream description;
        description << "Comma-separated list of supported codecs. Available codecs: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& codec : supportedCodecs) {
            auto findResult = CodecsDescriptions.find(codec);
            Y_VERIFY(findResult != CodecsDescriptions.end(),
                     "Couldn't find description for %s codec", (TStringBuilder() << codec).c_str());
            description << "\n  " << colors.BoldColor() << codec << colors.OldColor()
                        << "\n    " << findResult->second;
        }
        config.Opts->AddLongOption("supported-codecs", description.Str())
            .RequiredArgument("STRING")
            .StoreResult(&SupportedCodecsStr_);
        AllowedCodecs_ = supportedCodecs;
    }

    void TCommandWithSupportedCodecs::ParseCodecs() {
        if (SupportedCodecsStr_.empty()) {
            return;
        }

        TVector<NTopic::ECodec> parsedCodecs;
        TVector<TString> split = SplitString(SupportedCodecsStr_, ",");
        for (const TString& codecStr : split) {
            auto exists = ExistingCodecs.find(to_lower(codecStr));
            if (exists == ExistingCodecs.end()) {
                throw TMisuseException() << "Supported codec " << codecStr << " is not available for this command";
            }

            if (std::find(AllowedCodecs_.begin(), AllowedCodecs_.end(), exists->second) == AllowedCodecs_.end()) {
                throw TMisuseException() << "Supported codec " << codecStr << " is not available for this command";
            }

            SupportedCodecs_.push_back(exists->second);
        }
    }

    const TVector<NTopic::ECodec> TCommandWithSupportedCodecs::GetCodecs() {
        return SupportedCodecs_;
    }

    TCommandTopic::TCommandTopic()
        : TClientCommandTree("topic", {}, "TopicService operations") {
        AddCommand(std::make_unique<TCommandTopicCreate>());
        AddCommand(std::make_unique<TCommandTopicAlter>());
        AddCommand(std::make_unique<TCommandTopicDrop>());
        AddCommand(std::make_unique<TCommandTopicConsumer>());
    }

    TCommandTopicCreate::TCommandTopicCreate()
        : TYdbCommand("create", {}, "Create topic command") {
    }

    void TCommandTopicCreate::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("partitions-count", "Total partitions count for topic")
            .DefaultValue(1)
            .StoreResult(&PartitionsCount_);
        config.Opts->AddLongOption("retention-period-hours", "Duration in hours for which data in topic is stored")
            .DefaultValue(18)
            .Optional()
            .StoreResult(&RetentionPeriodHours_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "New topic path");
        AddAllowedCodecs(config, AllowedCodecs);
    }

    void TCommandTopicCreate::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseCodecs();
    }

    int TCommandTopicCreate::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient persQueueClient(driver);

        auto settings = NYdb::NTopic::TCreateTopicSettings();
        settings.PartitioningSettings(PartitionsCount_, PartitionsCount_);
        settings.PartitionWriteBurstBytes(1024 * 1024);
        settings.PartitionWriteSpeedBytesPerSecond(1024 * 1024);
        const auto codecs = GetCodecs();
        if (!codecs.empty()) {
            settings.SetSupportedCodecs(codecs);
        } else {
            settings.SetSupportedCodecs(AllowedCodecs);
        }
        settings.RetentionPeriod(TDuration::Hours(RetentionPeriodHours_));

        auto status = persQueueClient.CreateTopic(TopicName, settings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicAlter::TCommandTopicAlter()
        : TYdbCommand("alter", {}, "Alter topic command") {
    }

    void TCommandTopicAlter::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("partitions-count", "Total partitions count for topic")
            .StoreResult(&PartitionsCount_);
        config.Opts->AddLongOption("retention-period-hours", "Duration for which data in topic is stored")
            .Optional()
            .StoreResult(&RetentionPeriodHours_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic to alter");
        AddAllowedCodecs(config, AllowedCodecs);
    }

    void TCommandTopicAlter::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseCodecs();
    }

    NYdb::NTopic::TAlterTopicSettings TCommandTopicAlter::PrepareAlterSettings(
        NYdb::NTopic::TDescribeTopicResult& describeResult) {
        auto settings = NYdb::NTopic::TAlterTopicSettings();

        if (PartitionsCount_.Defined() && (*PartitionsCount_ != describeResult.GetTopicDescription().GetTotalPartitionsCount())) {
            settings.AlterPartitioningSettings(*PartitionsCount_, *PartitionsCount_);
        }

        auto codecs = GetCodecs();
        if (!codecs.empty()) {
            settings.SetSupportedCodecs(codecs);
        }

        if (RetentionPeriodHours_.Defined() && describeResult.GetTopicDescription().GetRetentionPeriod() != TDuration::Hours(*RetentionPeriodHours_)) {
            settings.SetRetentionPeriod(TDuration::Hours(*RetentionPeriodHours_));
        }

        return settings;
    }

    int TCommandTopicAlter::Run(TConfig& config) {
        if (!PartitionsCount_.Defined() && GetCodecs().empty() && !RetentionPeriodHours_.Defined()) {
            return EXIT_SUCCESS;
        }

        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto describeResult = topicClient.DescribeTopic(TopicName).GetValueSync();
        ThrowOnError(describeResult);

        auto settings = PrepareAlterSettings(describeResult);
        auto result = topicClient.AlterTopic(TopicName, settings).GetValueSync();
        ThrowOnError(result);
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
        SetFreeArgTitle(0, "<topic-path>", "Topic which will be dropped");
    }

    int TCommandTopicDrop::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NTopic::TTopicClient topicClient(driver);

        auto settings = NYdb::NTopic::TDropTopicSettings();
        TStatus status = topicClient.DropTopic(TopicName, settings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicConsumer::TCommandTopicConsumer()
        : TClientCommandTree("consumer", {}, "Consumer operations") {
        AddCommand(std::make_unique<TCommandTopicConsumerAdd>());
        AddCommand(std::make_unique<TCommandTopicConsumerDrop>());
    }

    TCommandTopicConsumerAdd::TCommandTopicConsumerAdd()
        : TYdbCommand("add", {}, "Consumer add operation") {
    }

    void TCommandTopicConsumerAdd::Config(TConfig& config) {
        TYdbCommand::Parse(config);
        config.Opts->AddLongOption("consumer-name", "New consumer for topic")
            .Required()
            .StoreResult(&ConsumerName_);
        config.Opts->AddLongOption("service-type", "Service type of reader")
            .Optional()
            .StoreResult(&ServiceType_);
        config.Opts->AddLongOption("starting-message-timestamp", "Unix timestamp starting from '1970-01-01 00:00:00' from which read is allowed")
            .Optional()
            .StoreResult(&StartingMessageTimestamp_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "topic for which consumer will be added");
    }

    void TCommandTopicConsumerAdd::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
    }

    int TCommandTopicConsumerAdd::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NTopic::TTopicClient topicClient(driver);

        NYdb::NTopic::TAlterTopicSettings readRuleSettings = NYdb::NTopic::TAlterTopicSettings();
        NYdb::NTopic::TConsumerSettings<NYdb::NTopic::TAlterTopicSettings> consumerSettings(readRuleSettings);
        consumerSettings.ConsumerName(ConsumerName_);
        if (StartingMessageTimestamp_.Defined()) {
            consumerSettings.ReadFrom(TInstant::Seconds(*StartingMessageTimestamp_));
        }
        const TVector<NTopic::ECodec> codecs = GetCodecs();
        if (!codecs.empty()) {
            consumerSettings.SetSupportedCodecs(codecs);
        } else {
            consumerSettings.SetSupportedCodecs(AllowedCodecs);
        }
        readRuleSettings.AppendAddConsumers(consumerSettings);

        TStatus status = topicClient.AlterTopic(TopicName, readRuleSettings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }

    TCommandTopicConsumerDrop::TCommandTopicConsumerDrop()
        : TYdbCommand("drop", {}, "Consumer drop operation") {
    }

    void TCommandTopicConsumerDrop::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("consumer-name", "Consumer which will be dropped")
            .Required()
            .StoreResult(&ConsumerName_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic from which consumer will be dropped");
    }

    void TCommandTopicConsumerDrop::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
    }

    int TCommandTopicConsumerDrop::Run(TConfig& config) {
        TDriver driver = CreateDriver(config);
        NYdb::NTopic::TTopicClient topicClient(driver);

        NYdb::NTopic::TAlterTopicSettings removeReadRuleSettings = NYdb::NTopic::TAlterTopicSettings();
        removeReadRuleSettings.AppendDropConsumers(ConsumerName_);

        TStatus status = topicClient.AlterTopic(TopicName, removeReadRuleSettings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }
} // namespace NYdb::NConsoleClient

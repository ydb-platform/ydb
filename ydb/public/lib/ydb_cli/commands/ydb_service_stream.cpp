#include "ydb_service_stream.h"
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <util/stream/str.h>
#include <util/string/vector.h>


namespace NYdb::NConsoleClient {
    namespace {
        THashMap<NYdb::NPersQueue::ECodec, TString> CodecsDescriptions = {
                {NYdb::NPersQueue::ECodec::RAW, "Raw codec. No data compression"},
                {NYdb::NPersQueue::ECodec::GZIP, "GZIP codec. Data is compressed with GZIP compression algorithm"},
                {NYdb::NPersQueue::ECodec::LZOP, "LZOP codec. Data is compressed with LZOP compression algorithm"},
                {NYdb::NPersQueue::ECodec::ZSTD, "ZSTD codec. Data is compressed with ZSTD compression algorithm"},
        };

        THashMap<TString, NYdb::NPersQueue::ECodec> ExistingCodecs = {
                std::pair<TString, NYdb::NPersQueue::ECodec>("raw", NYdb::NPersQueue::ECodec::RAW),
                std::pair<TString, NYdb::NPersQueue::ECodec>("gzip", NYdb::NPersQueue::ECodec::GZIP),
                std::pair<TString, NYdb::NPersQueue::ECodec>("lzop", NYdb::NPersQueue::ECodec::LZOP),
                std::pair<TString, NYdb::NPersQueue::ECodec>("zstd", NYdb::NPersQueue::ECodec::ZSTD),
        };
    }// namespace

    void TCommandWithSupportedCodecs::AddAllowedCodecs(TClientCommand::TConfig &config, const TVector<NPersQueue::ECodec> &supportedCodecs) {
        TStringStream description;
        description << "Comma-separated list of supported codecs. Available codecs: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto &codec : supportedCodecs) {
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

        TVector<NPersQueue::ECodec> parsedCodecs;
        TVector<TString> split = SplitString(SupportedCodecsStr_, ",");
        for (const TString &codecStr : split) {
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

    TVector<NPersQueue::ECodec> TCommandWithSupportedCodecs::GetCodecs() {
        return SupportedCodecs_;
    }

    TCommandStream::TCommandStream()
        : TClientCommandTree("stream", {}, "DataStreams service operations") {
        AddCommand(std::make_unique<TCommandStreamCreate>());
        AddCommand(std::make_unique<TCommandStreamAlter>());
        AddCommand(std::make_unique<TCommandStreamDrop>());
        AddCommand(std::make_unique<TCommandStreamConsumer>());
    }

    TCommandStreamCreate::TCommandStreamCreate()
        : TYdbCommand("create", {}, "Create stream command") {}

    void TCommandStreamCreate::Config(TConfig &config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("partitions-count", "Total partitions count for stream")
                .DefaultValue(1)
                .StoreResult(&PartitionsCount_);
        config.Opts->AddLongOption("retention-period-hours", "Duration in hours for which data in stream is stored")
                .DefaultValue(18)
                .Optional()
                .StoreResult(&RetentionPeriodHours_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<stream-path>", "New stream name");
        AddAllowedCodecs(config, AllowedCodecs);
    }

    void TCommandStreamCreate::Parse(TConfig &config) {
        TYdbCommand::Parse(config);
        ParseStreamName(config, 0);
        ParseCodecs();
    }

    int TCommandStreamCreate::Run(TConfig &config) {
        TDriver driver = CreateDriver(config);
        NYdb::NPersQueue::TPersQueueClient persQueueClient(driver);

        auto settings = NYdb::NPersQueue::TCreateTopicSettings();
        settings.PartitionsCount(PartitionsCount_);
        settings.MaxPartitionWriteSpeed(1024 * 1024);
        settings.MaxPartitionWriteBurst(1024 * 1024);
        auto codecs = GetCodecs();
        if (!codecs.empty()) {
            settings.SupportedCodecs(GetCodecs());
        }
        settings.RetentionPeriod(TDuration::Hours(RetentionPeriodHours_));

        auto status = persQueueClient.CreateTopic(StreamName, settings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }

    TCommandStreamAlter::TCommandStreamAlter() : TYdbCommand("alter", {}, "Alter stream command") {}

    void TCommandStreamAlter::Config(TConfig &config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("partitions-count", "Total partitions count for stream")
                .StoreResult(&PartitionsCount_);
        config.Opts->AddLongOption("retention-period-hours", "Duration for which data in stream is stored")
                .Optional()
                .StoreResult(&RetentionPeriodHours_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<stream-path>", "Stream to alter");
        AddAllowedCodecs(config, AllowedCodecs);
    }

    void TCommandStreamAlter::Parse(TConfig &config) {
        TYdbCommand::Parse(config);
        ParseStreamName(config, 0);
        ParseCodecs();
    }

    NYdb::NPersQueue::TAlterTopicSettings TCommandStreamAlter::PrepareAlterSettings(
            NYdb::NPersQueue::TDescribeTopicResult &describeResult) {
        auto settings = NYdb::NPersQueue::TAlterTopicSettings();
        settings.SetSettings(describeResult.TopicSettings());

        if (PartitionsCount_.Defined()) {
            settings.PartitionsCount(*PartitionsCount_.Get());
        }

        auto codecs = GetCodecs();
        if (!codecs.empty()) {
            settings.SupportedCodecs(codecs);
        }

        if (RetentionPeriodHours_.Defined()) {
            settings.RetentionPeriod(TDuration::Hours(*RetentionPeriodHours_.Get()));
        }

        return settings;
    }

    int TCommandStreamAlter::Run(TConfig &config) {
        if (!PartitionsCount_.Defined() && GetCodecs().empty() && !RetentionPeriodHours_.Defined()) {
            return EXIT_SUCCESS;
        }

        TDriver driver = CreateDriver(config);
        NYdb::NPersQueue::TPersQueueClient persQueueClient(driver);

        auto describeResult = persQueueClient.DescribeTopic(StreamName).GetValueSync();
        ThrowOnError(describeResult);

        auto settings = PrepareAlterSettings(describeResult);
        auto result = persQueueClient.AlterTopic(StreamName, settings).GetValueSync();
        ThrowOnError(result);
        return EXIT_SUCCESS;
    }

    TCommandStreamDrop::TCommandStreamDrop() : TYdbCommand("drop", {}, "Drop stream command") {}

    void TCommandStreamDrop::Parse(TConfig &config) {
        TYdbCommand::Parse(config);
        ParseStreamName(config, 0);
    }

    void TCommandStreamDrop::Config(TConfig &config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<stream-path>", "Stream which will be dropped");
    }

    int TCommandStreamDrop::Run(TConfig &config) {
        TDriver driver = CreateDriver(config);
        NPersQueue::TPersQueueClient persQueueClient(driver);

        auto settings = NYdb::NPersQueue::TDropTopicSettings();
        TStatus status = persQueueClient.DropTopic(StreamName, settings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }

    TCommandStreamConsumer::TCommandStreamConsumer() : TClientCommandTree("consumer", {}, "Consumer operations") {
        AddCommand(std::make_unique<TCommandStreamConsumerAdd>());
        AddCommand(std::make_unique<TCommandStreamConsumerDrop>());
    }

    TCommandStreamConsumerAdd::TCommandStreamConsumerAdd() : TYdbCommand("add", {}, "Consumer add operation") {}

    void TCommandStreamConsumerAdd::Config(TConfig &config) {
        TYdbCommand::Parse(config);
        config.Opts->AddLongOption("consumer-name", "New consumer for stream")
                .Required()
                .StoreResult(&ConsumerName_);
        config.Opts->AddLongOption("service-type", "Service type of reader")
                .Optional()
                .StoreResult(&ServiceType_);
        config.Opts->AddLongOption("starting-message-timestamp", "Unix timestamp starting from '1970-01-01 00:00:00' from which read is allowed")
                .Optional()
                .StoreResult(&StartingMessageTimestamp_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<stream-path>", "Stream for which consumer will be added");
    }

    void TCommandStreamConsumerAdd::Parse(TConfig &config) {
        TYdbCommand::Parse(config);
        ParseStreamName(config, 0);
    }

    int TCommandStreamConsumerAdd::Run(TConfig &config) {
        TDriver driver = CreateDriver(config);
        NPersQueue::TPersQueueClient persQueueClient(driver);

        NYdb::NPersQueue::TReadRuleSettings readRuleSettings = NYdb::NPersQueue::TReadRuleSettings();
        readRuleSettings.ConsumerName(ConsumerName_);
        if (StartingMessageTimestamp_.Defined()) {
            readRuleSettings.StartingMessageTimestamp(TInstant::Seconds(*StartingMessageTimestamp_.Get()));
        }
        if (ServiceType_.Defined()) {
            readRuleSettings.ServiceType(ServiceType_.GetRef());
        }

        auto addReadRuleSettings = NYdb::NPersQueue::TAddReadRuleSettings();
        addReadRuleSettings.ReadRule(readRuleSettings);
        TStatus status = persQueueClient.AddReadRule(StreamName, addReadRuleSettings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }

    TCommandStreamConsumerDrop::TCommandStreamConsumerDrop() : TYdbCommand("drop", {}, "Consumer drop operation") {}

    void TCommandStreamConsumerDrop::Config(TConfig &config) {
        TYdbCommand::Config(config);
        config.Opts->AddLongOption("consumer-name", "Consumer which will be dropped")
                .Required()
                .StoreResult(&ConsumerName_);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<stream-path>", "Stream from which consumer will be dropped");
    }

    void TCommandStreamConsumerDrop::Parse(TConfig &config) {
        TYdbCommand::Parse(config);
        ParseStreamName(config, 0);
    }

    int TCommandStreamConsumerDrop::Run(TConfig &config) {
        TDriver driver = CreateDriver(config);
        NPersQueue::TPersQueueClient persQueueClient(driver);

        NYdb::NPersQueue::TRemoveReadRuleSettings removeReadRuleSettings = NYdb::NPersQueue::TRemoveReadRuleSettings();
        removeReadRuleSettings.ConsumerName(ConsumerName_);

        TStatus status = persQueueClient.RemoveReadRule(StreamName, removeReadRuleSettings).GetValueSync();
        ThrowOnError(status);
        return EXIT_SUCCESS;
    }
}// namespace NYdb::NConsoleClient

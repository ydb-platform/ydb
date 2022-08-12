#include <openssl/sha.h>

#include "ydb_service_topic.h"
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/topic/topic_read.h>
#include <ydb/public/lib/ydb_cli/topic/topic_util.h>
#include <ydb/public/lib/ydb_cli/topic/topic_write.h>

#include <util/generic/set.h>
#include <util/stream/str.h>
#include <util/string/hex.h>
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

        // TODO(shmel1k@): improve docs
        THashMap<EStreamMetadataField, TString> StreamMetadataFieldsDescriptions = {
            {EStreamMetadataField::Body, "message content"},
            {EStreamMetadataField::WriteTime, "message write time"},
            {EStreamMetadataField::CreateTime, "message creation time"},
            {EStreamMetadataField::MessageGroupID, "message group id"},
            {EStreamMetadataField::Offset, "offset"},
            {EStreamMetadataField::SeqNo, "seqno"},
            {EStreamMetadataField::Meta, "meta"},
        };

        const TVector<EStreamMetadataField> AllStreamMetadataFields = {
            EStreamMetadataField::Body,
            EStreamMetadataField::WriteTime,
            EStreamMetadataField::CreateTime,
            EStreamMetadataField::MessageGroupID,
            EStreamMetadataField::Offset,
            EStreamMetadataField::SeqNo,
            EStreamMetadataField::Meta,
        };

        const THashMap<TString, EStreamMetadataField> StreamMetadataFieldsMap = {
            {"body", EStreamMetadataField::Body},
            {"write_time", EStreamMetadataField::WriteTime},
            {"create_time", EStreamMetadataField::CreateTime},
            {"message_group_id", EStreamMetadataField::MessageGroupID},
            {"offset", EStreamMetadataField::Offset},
            {"seq_no", EStreamMetadataField::SeqNo},
            {"meta", EStreamMetadataField::Meta},
        };

        THashMap<ETransformBody, TString> TransformBodyDescriptions = {
            {ETransformBody::None, "do not transform body to any format"},
            {ETransformBody::Base64, "transform body to base64 format"},
        };

        constexpr TDuration DefaultIdleTimeout = TDuration::Seconds(1);
    } // namespace

    void TCommandWithSupportedCodecs::AddAllowedCodecs(TClientCommand::TConfig& config, const TVector<NYdb::NTopic::ECodec>& supportedCodecs) {
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
        AddCommand(std::make_unique<TCommandTopicRead>());
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

    TCommandTopicInternal::TCommandTopicInternal()
        : TClientCommandTree("topic", {}, "Experimental topic operations") {
        AddCommand(std::make_unique<TCommandTopicWrite>());
    }

    TCommandTopicRead::TCommandTopicRead()
        : TYdbCommand("read", {}, "Read from topic command") {
    }

    void TCommandTopicRead::AddAllowedMetadataFields(TConfig& config) {
        TStringStream description;
        description << "Comma-separated list of message fields to print. Available fields: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& iter : StreamMetadataFieldsDescriptions) {
            description << "\n  " << colors.BoldColor() << iter.first << colors.OldColor() << "\n    " << iter.second;
        }

        config.Opts->AddLongOption("with-metadata-fields", description.Str())
            .Optional()
            .StoreResult(&WithMetadataFields_);
    }

    void TCommandTopicRead::AddAllowedTransformFormats(TConfig& config) {
        TStringStream description;
        description << "Format to transform received body: Available \"transform\" values: ";
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        for (const auto& iter : TransformBodyDescriptions) {
            description << "\n  " << colors.BoldColor() << iter.first << colors.OldColor() << "\n    " << iter.second;
        }

        config.Opts->AddLongOption("transform", description.Str())
            .Optional()
            .StoreResult(&TransformStr_);
    }

    void TCommandTopicRead::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<stream-path>", "Stream to read data");

        AddFormats(config, {
                               EOutputFormat::Pretty,
                               EOutputFormat::NewlineDelimited,
                               EOutputFormat::Concatenated,
                           });

        // TODO(shmel1k@): improve help.
        config.Opts->AddLongOption('c', "consumer", "Consumer name")
            .Required()
            .StoreResult(&Consumer_);
        config.Opts->AddLongOption("offset", "Offset to start read from")
            .Optional()
            .StoreResult(&Offset_);
        config.Opts->AddLongOption("partition", "Partition to read from")
            .Optional()
            .StoreResult(&Partition_);
        config.Opts->AddLongOption('f', "file", "File to write data to")
            .Optional()
            .StoreResult(&File_);
        config.Opts->AddLongOption("flush-duration", "Duration for message flushing")
            .Optional()
            .StoreResult(&FlushDuration_);
        config.Opts->AddLongOption("flush-size", "Maximum flush size") // TODO(shmel1k@): improve
            .Optional()
            .StoreResult(&FlushSize_);
        config.Opts->AddLongOption("flush-messages-count", "") // TODO(shmel1k@): improve
            .Optional()
            .StoreResult(&FlushMessagesCount_);
        config.Opts->AddLongOption("idle-timeout", "Max wait duration for new messages")
            .Optional()
            .DefaultValue(DefaultIdleTimeout)
            .StoreResult(&IdleTimeout_);
        config.Opts->AddLongOption("commit", "Commit messages after successful read")
            .Optional()
            .DefaultValue(true)
            .StoreResult(&Commit_);
        config.Opts->AddLongOption("message-size-limit", "Maximum message size")
            .Optional()
            .StoreResult(&MessageSizeLimit_);
        config.Opts->AddLongOption("discard-above-limits", "Do not print messages with size more than defined in 'message-size-limit' option")
            .Optional()
            .StoreResult(&DiscardAboveLimits_);
        config.Opts->AddLongOption("limit", "Messages count to read")
            .Optional()
            .StoreResult(&Limit_);
        config.Opts->AddLongOption('w', "wait", "Wait for infinite time for first message received")
            .Optional()
            .NoArgument()
            .StoreValue(&Wait_, true);
        config.Opts->AddLongOption("timestamp", "Timestamp from which messages will be read")
            .Optional()
            .StoreResult(&Timestamp_);

        AddAllowedMetadataFields(config);
        AddAllowedTransformFormats(config);
    }

    void TCommandTopicRead::ParseMetadataFields() {
        MetadataFields_ = AllStreamMetadataFields;

        // TODO(shmel1k@): discuss: disable all fields?
        if (WithMetadataFields_ == "all") {
            return;
        }

        TVector<TString> split = SplitString(WithMetadataFields_, ",");
        if (split.empty()) {
            return;
        }

        TSet<EStreamMetadataField> set;
        for (const auto& field : split) {
            auto f = StreamMetadataFieldsMap.find(field);
            if (f == StreamMetadataFieldsMap.end()) {
                throw TMisuseException() << "Field " << field << " not found in available fields"; // TODO(shmel1k@): improve message.
            }
            set.insert(f->second);
        }

        TVector<EStreamMetadataField> result;
        result.reserve(set.size());
        // NOTE(shmel1k@): preserving the order from AllMetadataFields
        for (const auto metadataField : set) {
            auto f = std::find(AllStreamMetadataFields.begin(), AllStreamMetadataFields.end(), metadataField);
            if (f == AllStreamMetadataFields.end()) {
                continue;
            }
            result.push_back(metadataField);
        }

        MetadataFields_ = result;
    }

    void TCommandTopicRead::ParseTransformFormat() {
        if (!TransformStr_.Defined()) {
            return;
        }

        TString val = *TransformStr_;
        if (val == (TStringBuilder() << ETransformBody::None)) {
            return;
        }
        if (val == (TStringBuilder() << ETransformBody::Base64)) {
            Transform_ = ETransformBody::Base64;
            return;
        }

        throw TMisuseException() << "Transform " << *TransformStr_ << " not found in available \"transform\" values";
    }

    void TCommandTopicRead::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseFormats();
        ParseMetadataFields();
        ParseTransformFormat();
    }

    NTopic::TReadSessionSettings TCommandTopicRead::PrepareReadSessionSettings() {
        NTopic::TReadSessionSettings settings;
        settings.ConsumerName(Consumer_);
        // settings.ReadAll(); // TODO(shmel1k@): change to read only original?
        if (Timestamp_.Defined()) {
            settings.ReadFromTimestamp(TInstant::Seconds(*(Timestamp_.Get())));
        }

        // TODO(shmel1k@): partition can be added here.
        NTopic::TTopicReadSettings readSettings;
        readSettings.Path(TopicName);
        settings.AppendTopics(std::move(readSettings));
        return settings;
    }

    void TCommandTopicRead::ValidateConfig() {
        // TODO(shmel1k@): add more formats.
        if (OutputFormat != EOutputFormat::Default && (Limit_.Defined() && (Limit_ < 0 || Limit_ > 500))) {
            throw TMisuseException() << "OutputFormat " << OutputFormat << " is not compatible with "
                                     << "limit equal '0' or more than '500': '" << *Limit_ << "' was given";
        }
    }

    int TCommandTopicRead::Run(TConfig& config) {
        ValidateConfig();

        auto driver = std::make_unique<TDriver>(CreateDriver(config));
        NTopic::TTopicClient persQueueClient(*driver);
        auto readSession = persQueueClient.CreateReadSession(PrepareReadSessionSettings());

        TTopicInitializationChecker checker = TTopicInitializationChecker(persQueueClient);
        checker.CheckTopicExistence(TopicName, Consumer_);

        {
            TTopicReader reader = TTopicReader(std::move(readSession), TTopicReaderSettings(
                                                                           Limit_,
                                                                           Commit_,
                                                                           Wait_,
                                                                           OutputFormat,
                                                                           MetadataFields_,
                                                                           Transform_,
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

    TCommandTopicWrite::TCommandTopicWrite()
        : TYdbCommand("write", {}, "Write to topic command") {
    }

    void TCommandTopicWrite::Config(TConfig& config) {
        TYdbCommand::Config(config);
        config.Opts->SetFreeArgsNum(1);
        SetFreeArgTitle(0, "<topic-path>", "Topic to write data");

        AddInputFormats(config, {
                                    EOutputFormat::NewlineDelimited,
                                    EOutputFormat::SingleMessage,
                                    //      EOutputFormat::JsonRawStreamConcat,
                                    //      EOutputFormat::JsonRawArray,
                                    EOutputFormat::SingleMessage,
                                });
        AddAllowedCodecs(config);

        // TODO(shmel1k@): improve help.
        config.Opts->AddLongOption('d', "delimiter", "Delimiter to split messages")
            .Optional()
            .StoreResult(&Delimiter_);
        config.Opts->AddLongOption('f', "file", "File to read data from")
            .Optional()
            .StoreResult(&File_);
        config.Opts->AddLongOption("message-size-limit", "Size limit for a single message")
            .Optional()
            .StoreResult(&MessageSizeLimitStr_);
        config.Opts->AddLongOption("batch-duration", "Duration for message batching")
            .Optional()
            .StoreResult(&BatchDuration_);
        config.Opts->AddLongOption("batch-size", "Maximum batch size") // TODO(shmel1k@): improve
            .Optional()
            .StoreResult(&BatchSize_);
        config.Opts->AddLongOption("batch-messages-count", "") // TODO(shmel1k@): improve
            .Optional()
            .StoreResult(&BatchMessagesCount_);
        config.Opts->AddLongOption("message-group-id", "Message Group ID") // TODO(shmel1k@): improve
            .Optional()
            .StoreResult(&MessageGroupId_);
    }

    void TCommandTopicWrite::Parse(TConfig& config) {
        TYdbCommand::Parse(config);
        ParseTopicName(config, 0);
        ParseFormats();
        ParseCodec();

        if (Delimiter_.Defined() && InputFormat != EOutputFormat::Default) {
            throw TMisuseException() << "Both mutually exclusive options \"delimiter\"(\"--delimiter\", \"-d\" "
                                     << "and \"input format\"(\"--input-format\") were provided.";
        }
    }

    NPersQueue::TWriteSessionSettings TCommandTopicWrite::PrepareWriteSessionSettings() {
        NPersQueue::TWriteSessionSettings settings;
        settings.Codec(GetCodec()); // TODO(shmel1k@): codecs?
        settings.Path(TopicName);
        //settings.BatchFlushInterval(BatchDuration_);
        //settings.BatchFlushSizeBytes(BatchSize_);
        settings.ClusterDiscoveryMode(NPersQueue::EClusterDiscoveryMode::Auto);

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

        return settings;
    }

    void TCommandTopicWrite::CheckOptions(NPersQueue::TPersQueueClient& persQueueClient) {
        NPersQueue::TAsyncDescribeTopicResult descriptionFuture = persQueueClient.DescribeTopic(TopicName);
        descriptionFuture.Wait(TDuration::Seconds(1));
        NPersQueue::TDescribeTopicResult description = descriptionFuture.GetValueSync();
        ThrowOnError(description);

        NPersQueue::ECodec codec = GetCodec();
        bool exists = false;
        for (const auto c : description.TopicSettings().SupportedCodecs()) {
            if (c == codec) {
                exists = true;
                break;
            }
        }
        if (exists) {
            return;
        }
        TStringStream errorMessage;
        errorMessage << "Codec \"" << (TStringBuilder() << codec) << "\" is not available for topic. Available codecs:\n";
        for (const auto c : description.TopicSettings().SupportedCodecs()) {
            errorMessage << "    " << (TStringBuilder() << c) << "\n";
        }
        throw TMisuseException() << errorMessage.Str();
    }

    int TCommandTopicWrite::Run(TConfig& config) {
        SetInterruptHandlers();

        auto driver = std::make_unique<TDriver>(CreateDriver(config));
        NPersQueue::TPersQueueClient persQueueClient(*driver);

        CheckOptions(persQueueClient);

        // TODO(shmel1k@): return back after IWriteSession in TopicService SDK
        //        TTopicInitializationChecker checker = TTopicInitializationChecker(persQueueClient);
        //        checker.CheckTopicExistence(TopicName);

        {
            auto writeSession = NPersQueue::TPersQueueClient(*driver).CreateWriteSession(std::move(PrepareWriteSessionSettings()));
            auto writer = TTopicWriter(writeSession, std::move(TTopicWriterParams(
                                                         InputFormat,
                                                         Delimiter_,
                                                         MessageSizeLimit_,
                                                         BatchDuration_,
                                                         BatchSize_,
                                                         BatchMessagesCount_)));

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

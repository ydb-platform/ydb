#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/interruptible.h>
#include <ydb/public/lib/ydb_cli/topic/topic_read.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

namespace NYdb::NConsoleClient {
    TVector<NYdb::NTopic::ECodec> InitAllowedCodecs();
    const TVector<NYdb::NTopic::ECodec> AllowedCodecs = InitAllowedCodecs();

    class TCommandWithSupportedCodecs {
    protected:
        void AddAllowedCodecs(TClientCommand::TConfig& config, const TVector<NTopic::ECodec>& supportedCodecs);
        void ParseCodecs();
        const TVector<NTopic::ECodec> GetCodecs();

    private:
        TString SupportedCodecsStr_;
        TVector<NTopic::ECodec> AllowedCodecs_;
        TVector<NTopic::ECodec> SupportedCodecs_;
    };

    class TCommandTopic: public TClientCommandTree {
    public:
        TCommandTopic();
    };

    class TCommandTopicCreate: public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs {
    public:
        TCommandTopicCreate();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        ui64 RetentionPeriodHours_;
        ui32 PartitionsCount_;
    };

    class TCommandTopicAlter: public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs {
    public:
        TCommandTopicAlter();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TMaybe<ui64> RetentionPeriodHours_;
        TMaybe<ui32> PartitionsCount_;
        NYdb::NTopic::TAlterTopicSettings PrepareAlterSettings(NYdb::NTopic::TDescribeTopicResult& describeResult);
    };

    class TCommandTopicDrop: public TYdbCommand, public TCommandWithTopicName {
    public:
        TCommandTopicDrop();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;
    };

    class TCommandTopicConsumer: public TClientCommandTree {
    public:
        TCommandTopicConsumer();
    };

    class TCommandTopicConsumerAdd: public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs {
    public:
        TCommandTopicConsumerAdd();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TString ConsumerName_;
        TMaybe<TString> ServiceType_;
        TMaybe<ui64> StartingMessageTimestamp_;
    };

    class TCommandTopicConsumerDrop: public TYdbCommand, public TCommandWithTopicName {
    public:
        TCommandTopicConsumerDrop();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TString ConsumerName_;
    };

    class TCommandTopicInternal: public TClientCommandTree {
    public:
        TCommandTopicInternal();
    };

    class TCommandTopicRead: public TYdbCommand,
                             public TCommandWithFormat,
                             public TInterruptibleCommand,
                             public TCommandWithTopicName {
    public:
        TCommandTopicRead();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TString Consumer_ = "";
        TMaybe<uint32_t> Offset_;
        TMaybe<uint32_t> Partition_;
        TMaybe<ui64> Timestamp_;
        TMaybe<TString> File_;
        TMaybe<TString> TransformStr_;

        TMaybe<TDuration> FlushDuration_;
        TMaybe<int> FlushSize_;
        TMaybe<int> FlushMessagesCount_;
        TDuration IdleTimeout_;

        TString WithMetadataFields_ = "all"; // TODO(shmel1k@): improve.
        TVector<EStreamMetadataField> MetadataFields_;

        TMaybe<ui64> MessageSizeLimit_;
        TMaybe<i64> Limit_ = Nothing();
        ETransformBody Transform_ = ETransformBody::None;

        bool Commit_ = false;
        bool DiscardAboveLimits_ = false;
        bool Wait_ = false;

    private:
        void ValidateConfig();
        void AddAllowedMetadataFields(TConfig& config);
        void ParseMetadataFields();
        void AddAllowedTransformFormats(TConfig& config);
        void ParseTransformFormat();
        NTopic::TReadSessionSettings PrepareReadSessionSettings();
    };

    namespace {
        const THashMap<NYdb::NPersQueue::ECodec, TString> CodecsDescriptionsMigration = {
            {NYdb::NPersQueue::ECodec::RAW, "Raw codec. No data compression(default)"},
            {NYdb::NPersQueue::ECodec::GZIP, "GZIP codec. Data is compressed with GZIP compression algorithm"},
            {NYdb::NPersQueue::ECodec::LZOP, "LZOP codec. Data is compressed with LZOP compression algorithm"},
            {NYdb::NPersQueue::ECodec::ZSTD, "ZSTD codec. Data is compressed with ZSTD compression algorithm"},
        };
    }

    const TVector<NYdb::NPersQueue::ECodec> AllowedCodecsMigration = {
        NPersQueue::ECodec::RAW,
        NPersQueue::ECodec::GZIP,
        NPersQueue::ECodec::ZSTD,
    };

    class TCommandWithCodecMigration {
        // TODO(shmel1k@): remove after TopicService C++ SDK supports IWriteSession
    protected:
        void AddAllowedCodecs(TClientCommand::TConfig& config) {
            TStringStream description;
            description << "Codec used for writing. Available codecs: ";
            NColorizer::TColors colors = NColorizer::AutoColors(Cout);
            for (const auto& codec : AllowedCodecsMigration) {
                auto findResult = CodecsDescriptionsMigration.find(codec);
                Y_VERIFY(findResult != CodecsDescriptionsMigration.end(),
                         "Couldn't find description for %s codec", (TStringBuilder() << codec).c_str());
                description << "\n  " << colors.BoldColor() << codec << colors.OldColor()
                            << "\n    " << findResult->second;
            }
            config.Opts->AddLongOption("codec", description.Str())
                .RequiredArgument("STRING")
                .StoreResult(&CodecStr_);
        }

        void ParseCodec() {
            TString codec = to_lower(CodecStr_);
            if (codec == "raw") {
                Codec_ = NPersQueue::ECodec::RAW;
            }
            if (codec == "gzip") {
                Codec_ = NPersQueue::ECodec::GZIP;
            }
            if (codec = "zstd") {
                Codec_ = NPersQueue::ECodec::ZSTD;
            }
        }

        NPersQueue::ECodec GetCodec() const {
            if (CodecStr_ == "") {
                return DefaultCodec_;
            }
            return Codec_;
        }

    private:
        NPersQueue::ECodec DefaultCodec_ = NPersQueue::ECodec::RAW;
        TString CodecStr_;
        NPersQueue::ECodec Codec_;
    };

    class TCommandTopicWrite: public TYdbCommand,
                              public TCommandWithFormat,
                              public TInterruptibleCommand,
                              public TCommandWithTopicName,
                              public TCommandWithCodecMigration {
    public:
        TCommandTopicWrite();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TMaybe<TString> File_;
        TMaybe<TString> Delimiter_;
        TMaybe<TString> MessageSizeLimitStr_; // TODO(shmel1k@): think how to parse

        // TODO(shmel1k@): move to 'TWithBatchingCommand' or something like that.
        TMaybe<TDuration> BatchDuration_;
        TMaybe<ui64> BatchSize_;
        TMaybe<ui64> BatchMessagesCount_;
        TMaybe<TString> MessageGroupId_;

        ui64 MessageSizeLimit_ = 0;
        void ParseMessageSizeLimit();
        void CheckOptions(NPersQueue::TPersQueueClient& persQueueClient);

    private:
        NPersQueue::TWriteSessionSettings PrepareWriteSessionSettings();
    };
} // namespace NYdb::NConsoleClient

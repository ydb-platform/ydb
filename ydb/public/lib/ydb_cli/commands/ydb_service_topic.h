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

    class TCommandWithMeteringMode {
    protected:
        void AddAllowedMeteringModes(TClientCommand::TConfig& config);
        void ParseMeteringMode();
        NTopic::EMeteringMode GetMeteringMode() const;

    private:
        TString MeteringModeStr_;
        NTopic::EMeteringMode MeteringMode_ = NTopic::EMeteringMode::Unspecified;
    };

    class TCommandTopic: public TClientCommandTree {
    public:
        TCommandTopic();
    };

    class TCommandTopicCreate: public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs, public TCommandWithMeteringMode {
    public:
        TCommandTopicCreate();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        ui64 RetentionPeriodHours_;
        ui64 RetentionStorageMb_;
        ui32 PartitionsCount_;
        ui32 PartitionWriteSpeedKbps_;
    };

    class TCommandTopicAlter: public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs, public TCommandWithMeteringMode {
    public:
        TCommandTopicAlter();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TMaybe<ui64> RetentionPeriodHours_;
        TMaybe<ui64> RetentionStorageMb_;
        TMaybe<ui32> PartitionsCount_;
        TMaybe<ui32> PartitionWriteSpeedKbps_;

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

    class TCommandTopicConsumerOffset: public TClientCommandTree {
    public:
        TCommandTopicConsumerOffset();
    };

    class TCommandTopicConsumerAdd: public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs {
    public:
        TCommandTopicConsumerAdd();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TString ConsumerName_;
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

    class TCommandTopicConsumerCommitOffset: public TYdbCommand, public TCommandWithTopicName {
    public:
        TCommandTopicConsumerCommitOffset();
        void Config(TConfig& config) override;
        void Parse(TConfig& config) override;
        int Run(TConfig& config) override;

    private:
        TString ConsumerName_;
        ui64 PartitionId_;
        ui64 Offset_;
    };


    class TCommandWithTransformBody {
    protected:
        void AddTransform(TClientCommand::TConfig& config);
        void ParseTransform();
        ETransformBody GetTransform() const;

    private:
        TString TransformStr_;
        ETransformBody Transform_ = ETransformBody::None;
    };

    class TCommandTopicRead: public TYdbCommand,
                             public TCommandWithFormat,
                             public TInterruptibleCommand,
                             public TCommandWithTopicName,
                             public TCommandWithTransformBody {
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
        TVector<ETopicMetadataField> MetadataFields_;

        TMaybe<ui64> MessageSizeLimit_;
        TMaybe<i64> Limit_ = Nothing();
        ETransformBody Transform_ = ETransformBody::None;

        bool Commit_ = false;
//        bool DiscardAboveLimits_ = false;
        bool Wait_ = false;

    private:
        void ValidateConfig();
        void AddAllowedMetadataFields(TConfig& config);
        void ParseMetadataFields();
        void AddAllowedTransformFormats(TConfig& config);
        void ParseTransformFormat();
        NTopic::TReadSessionSettings PrepareReadSessionSettings();
    };

    class TCommandWithCodec {
    protected:
        void AddAllowedCodecs(TClientCommand::TConfig& config, const TVector<NTopic::ECodec>& allowedCodecs);
        void ParseCodec();
        NTopic::ECodec GetCodec() const;

    private:
        TVector<NTopic::ECodec> AllowedCodecs_;
        TString CodecStr_;
        NTopic::ECodec Codec_ = NTopic::ECodec::RAW;
    };

    class TCommandTopicWrite: public TYdbCommand,
                              public TCommandWithFormat,
                              public TInterruptibleCommand,
                              public TCommandWithTopicName,
                              public TCommandWithCodec,
                              public TCommandWithTransformBody {
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
        void CheckOptions(NTopic::TTopicClient& topicClient);

    private:
        NTopic::TWriteSessionSettings PrepareWriteSessionSettings();
    };
} // namespace NYdb::NConsoleClient

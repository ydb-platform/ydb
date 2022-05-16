#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

namespace NYdb::NConsoleClient {
    TVector<NYdb::NPersQueue::ECodec> InitAllowedCodecs();
    const TVector<NYdb::NPersQueue::ECodec> AllowedCodecs = InitAllowedCodecs();

    class TCommandWithSupportedCodecs {
    protected:
        void AddAllowedCodecs(TClientCommand::TConfig &config, const TVector<NPersQueue::ECodec> &supportedCodecs);
        void ParseCodecs();
        TVector<NPersQueue::ECodec> GetCodecs();

    private:
        TString SupportedCodecsStr_;
        TVector<NPersQueue::ECodec> AllowedCodecs_;
        TVector<NPersQueue::ECodec> SupportedCodecs_;
    };

    class TCommandStream : public TClientCommandTree {
    public:
        TCommandStream();
    };


    class TCommandStreamCreate : public TYdbCommand, public TCommandWithStreamName, public TCommandWithSupportedCodecs {
    public:
        TCommandStreamCreate();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        ui64 RetentionPeriodHours_;
        ui32 PartitionsCount_;
    };

    class TCommandStreamAlter : public TYdbCommand, public TCommandWithStreamName, public TCommandWithSupportedCodecs {
    public:
        TCommandStreamAlter();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        TMaybe<ui64> RetentionPeriodHours_;
        TMaybe<ui32> PartitionsCount_;
        NYdb::NPersQueue::TAlterTopicSettings PrepareAlterSettings(NYdb::NPersQueue::TDescribeTopicResult &describeResult);
    };

    class TCommandStreamDrop : public TYdbCommand, public TCommandWithStreamName {
    public:
        TCommandStreamDrop();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;
    };

    class TCommandStreamConsumer : public TClientCommandTree {
    public:
        TCommandStreamConsumer();
    };

    class TCommandStreamConsumerAdd : public TYdbCommand, public TCommandWithStreamName {
    public:
        TCommandStreamConsumerAdd();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        TString ConsumerName_;
        TMaybe<TString> ServiceType_;
        TMaybe<ui64> StartingMessageTimestamp_;
    };

    class TCommandStreamConsumerDrop : public TYdbCommand, public TCommandWithStreamName {
    public:
        TCommandStreamConsumerDrop();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        TString ConsumerName_;
    };
}// namespace NYdb::NConsoleClient

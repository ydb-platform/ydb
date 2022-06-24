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

    class TCommandTopic : public TClientCommandTree {
    public:
        TCommandTopic();
    };


    class TCommandTopicCreate : public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs {
    public:
        TCommandTopicCreate();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        ui64 RetentionPeriodHours_;
        ui32 PartitionsCount_;
    };

    class TCommandTopicAlter : public TYdbCommand, public TCommandWithTopicName, public TCommandWithSupportedCodecs {
    public:
        TCommandTopicAlter();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        TMaybe<ui64> RetentionPeriodHours_;
        TMaybe<ui32> PartitionsCount_;
        NYdb::NPersQueue::TAlterTopicSettings PrepareAlterSettings(NYdb::NPersQueue::TDescribeTopicResult &describeResult);
    };

    class TCommandTopicDrop : public TYdbCommand, public TCommandWithTopicName {
    public:
        TCommandTopicDrop();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;
    };

    class TCommandTopicConsumer : public TClientCommandTree {
    public:
        TCommandTopicConsumer();
    };

    class TCommandTopicConsumerAdd : public TYdbCommand, public TCommandWithTopicName {
    public:
        TCommandTopicConsumerAdd();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        TString ConsumerName_;
        TMaybe<TString> ServiceType_;
        TMaybe<ui64> StartingMessageTimestamp_;
    };

    class TCommandTopicConsumerDrop : public TYdbCommand, public TCommandWithTopicName {
    public:
        TCommandTopicConsumerDrop();
        void Config(TConfig &config) override;
        void Parse(TConfig &config) override;
        int Run(TConfig &config) override;

    private:
        TString ConsumerName_;
    };
}// namespace NYdb::NConsoleClient

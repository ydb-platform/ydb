#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>

namespace NYdb::NConsoleClient {

class TCommandExperimentalTopic : public TClientCommandTree {
public:
    TCommandExperimentalTopic();
};

class TCommandExperimentalTopicWrite : public TYdbCommand,
                                       public TCommandWithMessagingFormat,
                                       public TInterruptableCommand,
                                       public TCommandWithTopicName,
                                       public TCommandWithCodec,
                                       public TCommandWithTransformBody {
public:
    TCommandExperimentalTopicWrite();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    NTopic::TWriteSessionSettings PrepareWriteSessionSettings();

    TMaybe<TString> File_;
    TMaybe<TString> Delimiter_;
    TMaybe<TString> MessageGroupId_;
    TMaybe<ui32> PartitionId_;
    TMaybe<TDuration> MessagesWaitTimeout_;
    ui64 MessageSizeLimit_ = 0;

    TMaybe<ui64> DeferredIntPublicationId_;
    TMaybe<TString> DeferredExtPublicationId_;
};

class TCommandTopicDeferredPublication : public TClientCommandTree {
public:
    TCommandTopicDeferredPublication();
};

class TCommandTopicDeferredPublicationBegin : public TYdbCommand, public TCommandWithOutput {
public:
    TCommandTopicDeferredPublicationBegin();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString ExtPublicationId_;
    TMaybe<TString> WriterIdentity_;
};

class TCommandTopicDeferredPublicationPublish : public TYdbCommand {
public:
    TCommandTopicDeferredPublicationPublish();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    ui64 IntPublicationId_ = 0;
};

class TCommandTopicDeferredPublicationCancel : public TYdbCommand {
public:
    TCommandTopicDeferredPublicationCancel();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    ui64 IntPublicationId_ = 0;
};

class TCommandTopicDeferredPublicationList : public TYdbCommand, public TCommandWithOutput {
public:
    TCommandTopicDeferredPublicationList();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TMaybe<TString> WriterIdentity_;
};

class TCommandTopicDeferredPublicationDescribe : public TYdbCommand, public TCommandWithOutput {
public:
    TCommandTopicDeferredPublicationDescribe();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    ui64 IntPublicationId_ = 0;
};

} // namespace NYdb::NConsoleClient

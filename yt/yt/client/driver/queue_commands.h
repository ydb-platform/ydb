#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TRegisterQueueConsumerCommand
    : public TTypedCommand<NApi::TRegisterQueueConsumerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRegisterQueueConsumerCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath QueuePath;
    NYPath::TRichYPath ConsumerPath;
    bool Vital;
    std::optional<std::vector<int>> Partitions;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUnregisterQueueConsumerCommand
    : public TTypedCommand<NApi::TUnregisterQueueConsumerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TUnregisterQueueConsumerCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath QueuePath;
    NYPath::TRichYPath ConsumerPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListQueueConsumerRegistrationsCommand
    : public TTypedCommand<NApi::TListQueueConsumerRegistrationsOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListQueueConsumerRegistrationsCommand);

    static void Register(TRegistrar registrar);

private:
    std::optional<NYPath::TRichYPath> QueuePath;
    std::optional<NYPath::TRichYPath> ConsumerPath;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPullQueueCommand
    : public TTypedCommand<NApi::TPullQueueOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPullQueueCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath QueuePath;
    i64 Offset;
    int PartitionIndex;
    NQueueClient::TQueueRowBatchReadOptions RowBatchReadOptions;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPullQueueConsumerCommand
    : public TTypedCommand<NApi::TPullQueueConsumerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPullQueueConsumerCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath ConsumerPath;
    NYPath::TRichYPath QueuePath;
    std::optional<i64> Offset;
    int PartitionIndex;
    NQueueClient::TQueueRowBatchReadOptions RowBatchReadOptions;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TAdvanceQueueConsumerOptions
    : public TTabletWriteOptions
{ };

class TAdvanceQueueConsumerCommand
    : public TTypedCommand<TAdvanceQueueConsumerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAdvanceQueueConsumerCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath ConsumerPath;
    NYPath::TRichYPath QueuePath;
    int PartitionIndex;
    std::optional<i64> OldOffset;
    i64 NewOffset;
    std::optional<bool> ClientSide;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateQueueProducerSessionCommand
    : public TTypedCommand<NApi::TCreateQueueProducerSessionOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TCreateQueueProducerSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath ProducerPath;
    NYPath::TRichYPath QueuePath;
    NQueueClient::TQueueProducerSessionId SessionId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveQueueProducerSessionCommand
    : public TTypedCommand<NApi::TRemoveQueueProducerSessionOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TRemoveQueueProducerSessionCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath ProducerPath;
    NYPath::TRichYPath QueuePath;
    NQueueClient::TQueueProducerSessionId SessionId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TPushQueueProducerOptions
    : public NApi::TPushQueueProducerOptions
    , public TTabletWriteOptions
{ };

class TPushQueueProducerCommand
    : public TTypedCommand<TPushQueueProducerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPushQueueProducerCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath ProducerPath;
    NYPath::TRichYPath QueuePath;
    NQueueClient::TQueueProducerSessionId SessionId;
    NQueueClient::TQueueProducerEpoch Epoch;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

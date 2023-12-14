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

class TPullConsumerCommand
    : public TTypedCommand<NApi::TPullConsumerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TPullConsumerCommand);

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

struct TAdvanceConsumerOptions
    : public TTabletWriteOptions
{ };

class TAdvanceConsumerCommand
    : public TTypedCommand<TAdvanceConsumerOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAdvanceConsumerCommand);

    static void Register(TRegistrar registrar);

private:
    NYPath::TRichYPath ConsumerPath;
    NYPath::TRichYPath QueuePath;
    int PartitionIndex;
    std::optional<i64> OldOffset;
    i64 NewOffset;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

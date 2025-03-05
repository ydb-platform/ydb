#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////

class TStartShuffleCommand
    : public TTypedCommand<NApi::TStartShuffleOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartShuffleCommand);

    static void Register(TRegistrar registrar);

private:
    std::string Account;
    int PartitionCount;
    NObjectClient::TTransactionId ParentTransactionId;
    std::optional<std::string> Medium;
    std::optional<int> ReplicationFactor;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TReadShuffleDataCommand
    : public TTypedCommand<NTableClient::TTableReaderConfigPtr>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReadShuffleDataCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::TShuffleHandlePtr ShuffleHandle;
    int PartitionIndex;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TWriteShuffleDataCommand
    : public TTypedCommand<NTableClient::TTableWriterConfigPtr>
{
public:
    REGISTER_YSON_STRUCT_LITE(TWriteShuffleDataCommand);

    static void Register(TRegistrar registrar);

private:
    NApi::TShuffleHandlePtr ShuffleHandle;
    std::string PartitionColumn;
    i64 MaxRowBufferSize;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartTransactionCommand
    : public TTypedCommand<NApi::TTransactionStartOptions>
{
    REGISTER_YSON_STRUCT_LITE(TStartTransactionCommand);

    static void Register(TRegistrar registrar);

private:
    NTransactionClient::ETransactionType Type;
    NYTree::INodePtr Attributes;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPingTransactionCommand
    : public TTypedCommand<NApi::TTransactionalOptions>
{
    REGISTER_YSON_STRUCT_LITE(TPingTransactionCommand);

    static void Register(TRegistrar /*registrar*/)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitTransactionCommand
    : public TTypedCommand<NApi::TTransactionCommitOptions>
{
    REGISTER_YSON_STRUCT_LITE(TCommitTransactionCommand);

    static void Register(TRegistrar /*registrar*/)
    { }

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTransactionCommand
    : public TTypedCommand<NApi::TTransactionAbortOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAbortTransactionCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampOptions
{
    std::optional<NObjectClient::TCellTag> ClockClusterTag;
};

class TGenerateTimestampCommand
    : public TTypedCommand<TGenerateTimestampOptions>
{
    REGISTER_YSON_STRUCT_LITE(TGenerateTimestampCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

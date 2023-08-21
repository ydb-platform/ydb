#pragma once

#include "command.h"

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartTransactionCommand
    : public TTypedCommand<NApi::TTransactionStartOptions>
{
public:
    TStartTransactionCommand();

private:
    NTransactionClient::ETransactionType Type;
    NYTree::INodePtr Attributes;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TPingTransactionCommand
    : public TTypedCommand<NApi::TTransactionalOptions>
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitTransactionCommand
    : public TTypedCommand<NApi::TTransactionCommitOptions>
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortTransactionCommand
    : public TTypedCommand<NApi::TTransactionAbortOptions>
{
public:
    TAbortTransactionCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TGenerateTimestampOptions
{ };

class TGenerateTimestampCommand
    : public TTypedCommand<TGenerateTimestampOptions>
{
private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver


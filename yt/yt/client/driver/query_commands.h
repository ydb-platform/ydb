#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/query_tracker_client/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////

class TStartQueryCommand
    : public TTypedCommand<NApi::TStartQueryOptions>
{
public:
    TStartQueryCommand();

private:
    NQueryTrackerClient::EQueryEngine Engine;
    TString Query;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TAbortQueryCommand
    : public TTypedCommand<NApi::TAbortQueryOptions>
{
public:
    TAbortQueryCommand();

private:
    NQueryTrackerClient::TQueryId QueryId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TGetQueryResultCommand
    : public TTypedCommand<NApi::TGetQueryResultOptions>
{
public:
    TGetQueryResultCommand();

private:
    NQueryTrackerClient::TQueryId QueryId;
    i64 ResultIndex;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TReadQueryResultCommand
    : public TTypedCommand<NApi::TReadQueryResultOptions>
{
public:
    TReadQueryResultCommand();

private:
    NQueryTrackerClient::TQueryId QueryId;
    i64 ResultIndex;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TGetQueryCommand
    : public TTypedCommand<NApi::TGetQueryOptions>
{
public:
    TGetQueryCommand();

private:
    NQueryTrackerClient::TQueryId QueryId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TListQueriesCommand
    : public TTypedCommand<NApi::TListQueriesOptions>
{
public:
    TListQueriesCommand();

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

class TAlterQueryCommand
    : public TTypedCommand<NApi::TAlterQueryOptions>
{
public:
    TAlterQueryCommand();

private:
    NQueryTrackerClient::TQueryId QueryId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

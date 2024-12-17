#pragma once

#include "command.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/query_tracker_client/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TStartQueryCommand
    : public TTypedCommand<NApi::TStartQueryOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TStartQueryCommand);

    static void Register(TRegistrar registrar);

private:
    NQueryTrackerClient::EQueryEngine Engine;
    TString Query;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortQueryCommand
    : public TTypedCommand<NApi::TAbortQueryOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAbortQueryCommand);

    static void Register(TRegistrar registrar);

private:
    NQueryTrackerClient::TQueryId QueryId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetQueryResultCommand
    : public TTypedCommand<NApi::TGetQueryResultOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetQueryResultCommand);

    static void Register(TRegistrar registrar);

private:
    NQueryTrackerClient::TQueryId QueryId;
    i64 ResultIndex;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TReadQueryResultCommand
    : public TTypedCommand<NApi::TReadQueryResultOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TReadQueryResultCommand);

    static void Register(TRegistrar registrar);

private:
    NQueryTrackerClient::TQueryId QueryId;
    i64 ResultIndex;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetQueryCommand
    : public TTypedCommand<NApi::TGetQueryOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetQueryCommand);

    static void Register(TRegistrar registrar);

private:
    NQueryTrackerClient::TQueryId QueryId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListQueriesCommand
    : public TTypedCommand<NApi::TListQueriesOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TListQueriesCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterQueryCommand
    : public TTypedCommand<NApi::TAlterQueryOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TAlterQueryCommand);

    static void Register(TRegistrar registrar);

private:
    NQueryTrackerClient::TQueryId QueryId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetQueryTrackerInfoCommand
    : public TTypedCommand<NApi::TGetQueryTrackerInfoOptions>
{
public:
    REGISTER_YSON_STRUCT_LITE(TGetQueryTrackerInfoCommand);

    static void Register(TRegistrar registrar);

private:
    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

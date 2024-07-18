#include "query_commands.h"

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NQueryTrackerClient;
using namespace NYTree;
using namespace NConcurrency;
using namespace NYson;
using namespace NTableClient;
using namespace NFormats;

//////////////////////////////////////////////////////////////////////////////

void TStartQueryCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("engine", &TThis::Engine);

    registrar.Parameter("query", &TThis::Query);

    registrar.ParameterWithUniversalAccessor<std::vector<TQueryFilePtr>>(
        "files",
        [] (TThis* command) -> auto& {
            return command->Options.Files;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<INodePtr>(
        "settings",
        [] (TThis* command) -> auto& {
            return command->Options.Settings;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "draft",
        [] (TThis* command) -> auto& {
            return command->Options.Draft;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<IMapNodePtr>(
        "annotations",
        [] (TThis* command) -> auto& {
            return command->Options.Annotations;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "access_control_object",
        [] (TThis* command) -> auto& {
            return command->Options.AccessControlObject;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<std::vector<TString>>>(
        "access_control_objects",
        [] (TThis* command) -> auto& {
            return command->Options.AccessControlObjects;
        })
        .Optional(/*init*/ false);
}

void TStartQueryCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->StartQuery(Engine, Query, Options);
    auto queryId = WaitFor(asyncResult)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("query_id").Value(queryId)
        .EndMap());
}

//////////////////////////////////////////////////////////////////////////////

void TAbortQueryCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query_id", &TThis::QueryId);

    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Optional(/*init*/ false);
}

void TAbortQueryCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AbortQuery(QueryId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

//////////////////////////////////////////////////////////////////////////////

void TGetQueryResultCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query_id", &TThis::QueryId);
    registrar.Parameter("result_index", &TThis::ResultIndex)
        .Default(0);
    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Optional(/*init*/ false);
}

void TGetQueryResultCommand::DoExecute(ICommandContextPtr context)
{
    auto queryResult = WaitFor(context->GetClient()->GetQueryResult(QueryId, ResultIndex, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(queryResult));
}

//////////////////////////////////////////////////////////////////////////////

void TReadQueryResultCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query_id", &TThis::QueryId);

    registrar.Parameter("result_index", &TThis::ResultIndex)
        .Default(0);

    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<std::vector<TString>>>(
        "columns",
        [] (TThis* command) -> auto& {
            return command->Options.Columns;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "lower_row_index",
        [] (TThis* command) -> auto& {
            return command->Options.LowerRowIndex;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "upper_row_index",
        [] (TThis* command) -> auto& {
            return command->Options.UpperRowIndex;
        })
        .Optional(/*init*/ false);
}

void TReadQueryResultCommand::DoExecute(ICommandContextPtr context)
{
    auto rowset = WaitFor(context->GetClient()->ReadQueryResult(QueryId, ResultIndex, Options))
        .ValueOrThrow();

    auto writer = CreateStaticTableWriterForFormat(
        context->GetOutputFormat(),
        rowset->GetNameTable(),
        {rowset->GetSchema()},
        context->Request().OutputStream,
        /*enableContextSaving*/ false,
        New<TControlAttributesConfig>(),
        /*keyColumnCount*/ 0);

    Y_UNUSED(writer->Write(rowset->GetRows()));
    WaitFor(writer->Close())
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////////

void TGetQueryCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query_id", &TThis::QueryId);

    registrar.ParameterWithUniversalAccessor<TAttributeFilter>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Optional(/*init*/ false);
}

void TGetQueryCommand::DoExecute(ICommandContextPtr context)
{
    auto query = WaitFor(context->GetClient()->GetQuery(QueryId, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(query));
}

//////////////////////////////////////////////////////////////////////////////

void TListQueriesCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Default("production");

    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "from_time",
        [] (TThis* command) -> auto& {
            return command->Options.FromTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "to_time",
        [] (TThis* command) -> auto& {
            return command->Options.ToTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TInstant>>(
        "cursor_time",
        [] (TThis* command) -> auto& {
            return command->Options.CursorTime;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EOperationSortDirection>(
        "cursor_direction",
        [] (TThis* command) -> auto& {
            return command->Options.CursorDirection;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "user",
        [] (TThis* command) -> auto& {
            return command->Options.UserFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<EQueryState>>(
        "state",
        [] (TThis* command) -> auto& {
            return command->Options.StateFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<EQueryEngine>>(
        "engine",
        [] (TThis* command) -> auto& {
            return command->Options.EngineFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "filter",
        [] (TThis* command) -> auto& {
            return command->Options.SubstrFilter;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<ui64>(
        "limit",
        [] (TThis* command) -> auto& {
            return command->Options.Limit;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TAttributeFilter>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);
}

void TListQueriesCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->ListQueries(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("queries").Value(result.Queries)
            .Item("incomplete").Value(result.Incomplete)
            .Item("timestamp").Value(result.Timestamp)
        .EndMap());
}

//////////////////////////////////////////////////////////////////////////////

void TAlterQueryCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query_id", &TThis::QueryId);

    registrar.ParameterWithUniversalAccessor<IMapNodePtr>(
        "annotations",
        [] (TThis* command) -> auto& {
            return command->Options.Annotations;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "access_control_object",
        [] (TThis* command) -> auto& {
            return command->Options.AccessControlObject;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<std::vector<TString>>>(
        "access_control_objects",
        [] (TThis* command) -> auto& {
            return command->Options.AccessControlObjects;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Default("production");
}

void TAlterQueryCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AlterQuery(QueryId, Options))
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

//////////////////////////////////////////////////////////////////////////////

void TGetQueryTrackerInfoCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<TString>(
        "stage",
        [] (TThis* command) -> auto& {
            return command->Options.QueryTrackerStage;
        })
        .Default("production");

    registrar.ParameterWithUniversalAccessor<TAttributeFilter>(
        "attributes",
        [] (TThis* command) -> auto& {
            return command->Options.Attributes;
        })
        .Optional(/*init*/ false);
}

void TGetQueryTrackerInfoCommand::DoExecute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->GetQueryTrackerInfo(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("cluster_name").Value(result.ClusterName)
            .Item("supported_features").Value(result.SupportedFeatures)
            .Item("access_control_objects").Value(result.AccessControlObjects)
        .EndMap());
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

#include "query_tracker_client.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/fluent.h>

#include <contrib/libs/pfr/include/pfr/tuple_size.hpp>

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TQuery& query, NYson::IYsonConsumer* consumer)
{
    static_assert(pfr::tuple_size<TQuery>::value == 14);
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("id", query.Id)
            .OptionalItem("engine", query.Engine)
            .OptionalItem("query", query.Query)
            .OptionalItem("files", query.Files)
            .OptionalItem("start_time", query.StartTime)
            .OptionalItem("finish_time", query.FinishTime)
            .OptionalItem("settings", query.Settings)
            .OptionalItem("user", query.User)
            .OptionalItem("state", query.State)
            .OptionalItem("result_count", query.ResultCount)
            .OptionalItem("progress", query.Progress)
            .OptionalItem("annotations", query.Annotations)
            .OptionalItem("error", query.Error)
            .DoIf(static_cast<bool>(query.OtherAttributes), [&] (TFluentMap fluent) {
                for (const auto& [key, value] : query.OtherAttributes->ListPairs()) {
                    fluent.Item(key).Value(value);
                }
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TQueryResult& queryResult, NYson::IYsonConsumer* consumer)
{
    static_assert(pfr::tuple_size<TQueryResult>::value == 5);
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("id").Value(queryResult.Id)
            .Item("result_index").Value(queryResult.ResultIndex)
            .DoIf(!queryResult.Error.IsOK(), [&] (TFluentMap fluent) {
                fluent
                    .Item("error").Value(queryResult.Error);
            })
            .OptionalItem("schema", queryResult.Schema)
            .Item("data_statistics").Value(queryResult.DataStatistics)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi


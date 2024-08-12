#include "helpers.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NYPath;
using namespace NLogging;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueueClient;

////////////////////////////////////////////////////////////////////////////////

TFuture<THashMap<int, THashMap<i64, TPartitionRowInfo>>> CollectPartitionRowInfos(
    const TYPath& path,
    const IClientPtr& client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const TCollectPartitionRowInfoParams& params,
    const TLogger& logger)
{
    const auto& Logger = logger;

    if (tabletAndRowIndices.empty()) {
        return MakeFuture<THashMap<int, THashMap<i64, TPartitionRowInfo>>>({});
    }

    TStringBuilder queryBuilder;
    queryBuilder.AppendString("[$tablet_index], [$row_index]");

    std::optional<int> cumulativeDataWeightColumnId;
    std::optional<int> timestampColumnId;

    int expectedRowSize = 2;
    if (params.HasCumulativeDataWeightColumn) {
        queryBuilder.AppendFormat(", [%v]", CumulativeDataWeightColumnName);
        cumulativeDataWeightColumnId = expectedRowSize;
        ++expectedRowSize;
    }
    if (params.HasTimestampColumn) {
        queryBuilder.AppendFormat(", [%v]", TimestampColumnName);
        timestampColumnId = expectedRowSize;
        ++expectedRowSize;
    }

    queryBuilder.AppendFormat("from [%v] where ([$tablet_index], [$row_index]) in (",
        path);

    bool isFirstTuple = true;
    for (const auto& [partitionIndex, rowIndex] : tabletAndRowIndices) {
        if (!isFirstTuple) {
            queryBuilder.AppendString(", ");
        }
        queryBuilder.AppendFormat("(%vu, %vu)", partitionIndex, rowIndex);
        isFirstTuple = false;
    }

    queryBuilder.AppendString(")");

    YT_VERIFY(!isFirstTuple);

    auto query = queryBuilder.Flush();

    YT_LOG_TRACE("Executing query for partition row infos (Query: %v)", query);

    TSelectRowsOptions options;
    options.ReplicaConsistency = EReplicaConsistency::Sync;
    return client->SelectRows(query, options)
        .Apply(BIND([expectedRowSize, cumulativeDataWeightColumnId, timestampColumnId] (const TSelectRowsResult& selectResult) {
            THashMap<int, THashMap<i64, TPartitionRowInfo>> result;

            for (auto row : selectResult.Rowset->GetRows()) {
                YT_VERIFY(static_cast<int>(row.GetCount()) == expectedRowSize);

                auto tabletIndex = FromUnversionedValue<int>(row[0]);
                auto rowIndex = FromUnversionedValue<i64>(row[1]);

                result[tabletIndex].emplace(rowIndex, TPartitionRowInfo{
                    .CumulativeDataWeight = cumulativeDataWeightColumnId
                        ? FromUnversionedValue<std::optional<i64>>(row[*cumulativeDataWeightColumnId])
                        : std::nullopt,
                    .Timestamp = timestampColumnId
                        ? FromUnversionedValue<std::optional<TTimestamp>>(row[*timestampColumnId])
                        : std::nullopt,
                });
            }

            return result;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client_common.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionRowInfo
{
    std::optional<i64> CumulativeDataWeight;
    std::optional<NYT::NTransactionClient::TTimestamp> Timestamp;
};

struct TCollectPartitionRowInfoParams
{
    bool HasCumulativeDataWeightColumn = false;
    bool HasTimestampColumn = false;
};

//! Collect info (cumulative data weight and timestamp) from rows with given (tablet_index, row_index) pairs and
//! return them as a tablet_index: (row_index: info) map.
TFuture<THashMap<int, THashMap<i64, TPartitionRowInfo>>> CollectPartitionRowInfos(
    const NYPath::TYPath& path,
    const NApi::IClientPtr& client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const TCollectPartitionRowInfoParams& params,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct TYtPartitionerSettings {
    ui64 MaxDataWeightPerPart = 0;
    ui64 MaxParts = 0;
};

class IYtCoordinatorService: public TThrRefBase {
public:
    virtual ~IYtCoordinatorService() = default;

    using TPtr = TIntrusivePtr<IYtCoordinatorService>;

    virtual std::pair<std::vector<TYtTableTaskRef>, bool> PartitionYtTables(
        const std::vector<TYtTableRef>& ytTables,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        const TYtPartitionerSettings& settings
    ) = 0;
};

} // namespace NYql::NFmr

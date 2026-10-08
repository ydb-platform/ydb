#include "yql_dq_gateway.h"

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>

#include <util/string/cast.h>

#include <map>
#include <string_view>

namespace NYql {

using namespace std::string_view_literals;

std::unordered_map<ui64, IDqGateway::TStageStats> ExtractDqStagesStats(const TOperationStatistics& statistics) {
    std::unordered_map<ui64, IDqGateway::TStageStats> resultStats;
    for (const auto& entry : statistics.Entries) {
        if (!entry.Sum) {
            continue;
        }

        TString prefix;
        TString name;
        std::map<TString, TString> labels;
        if (!NCommon::ParseCounterName(&prefix, &labels, &name, entry.Name)) {
            continue;
        }

        // Only accept statistics aggregated by stage, with no labels other than Stage.
        if (labels.size() != 1) {
            continue;
        }

        auto stageIt = labels.find("Stage");
        if (stageIt == labels.end()) {
            continue;
        }

        ui64 stageId = 0;
        if (!TryFromString(stageIt->second, stageId) || !stageId) {
            continue;
        }

        auto& stage = resultStats[stageId];
        const i64 sum = *entry.Sum;
        if (name == "OutputRows"sv) {
            stage.OutputRows += sum;
        } else if (name == "InputRows"sv) {
            stage.InputRows += sum;
        } else if (name == "OutputBytes"sv) {
            stage.OutputBytes += sum;
        } else if (name == "InputBytes"sv) {
            stage.InputBytes += sum;
        } else if (name == "IngressRows"sv) {
            stage.IngressRows += sum;
        } else if (name == "IngressBytes"sv) {
            stage.IngressBytes += sum;
        } else if (name == "EgressRows"sv) {
            stage.EgressRows += sum;
        } else if (name == "EgressBytes"sv) {
            stage.EgressBytes += sum;
        }
    }
    return resultStats;
}

} // namespace NYql

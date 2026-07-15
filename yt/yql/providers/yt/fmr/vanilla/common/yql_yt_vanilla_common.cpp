#include "yql_yt_vanilla_common.h"

namespace NYql::NFmr {

TString ExtractBackboneMtnIp(const NYT::TJobAttributes& job) {
    if (!job.ExecAttributes) {
        return {};
    }
    try {
        for (const auto& node : (*job.ExecAttributes)["ip_addresses"].AsList()) {
            const auto& ip = node.AsString();
            if (ip.StartsWith("2a02:6b8:c")) {
                return ip;
            }
        }
    } catch (...) {
    }
    return {};
}

} // namespace NYql::NFmr

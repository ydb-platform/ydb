#include "request_migrator.h"

#include <vector>
#include <numeric>
#include <cmath>

namespace NYdb::inline Dev {

namespace NMath {

TStats CalcCV(const std::vector<size_t>& in) {
    if (in.empty())
        return {0, 0.0};

    if (in.size() == 1)
        return {0, static_cast<float>(in[0])};

    const size_t sum = std::accumulate(in.begin(), in.end(), 0);
    if (!sum)
        return {0, 0.0};

    const float mean = sum / static_cast<float>(in.size());

    double tmp = 0;
    for (float t : in) {
        t -= mean;
        tmp += t * t;
    }

    auto cv = static_cast<ui64>(llround(100.0f * sqrt(tmp / static_cast<float>(in.size() - 1)) / mean));
    return {cv, mean};
}

} // namespace NMath

namespace NTable {

void TRequestMigrator::SetHost(ui64 nodeId) {
    std::lock_guard lock(Lock_);
    CurHost_ = nodeId;
}

bool TRequestMigrator::IsOurSession(const TKqpSessionCommon* session) const {
    if (!CurHost_)
        return false;

    if (session->GetEndpointKey().GetNodeId() != CurHost_)
        return false;

    return true;
}

bool TRequestMigrator::Reset() {
    if (Lock_.try_lock()) {
        if (CurHost_) {
            CurHost_ = 0;
            Lock_.unlock();
            return true;
        } else {
            Lock_.unlock();
            return false;
        }
    } else {
        return false;
    }
}

bool TRequestMigrator::DoCheckAndMigrate(const TKqpSessionCommon* session) {
    if (session->GetEndpoint().empty())
        return false;

    if (Lock_.try_lock()) {
        if (IsOurSession(session)) {
            CurHost_ = 0;
            Lock_.unlock();
            return true;
        } else {
            Lock_.unlock();
            return false;
        }
    } else {
        return false;
    }
}

} // namespace NTable
} // namespace NYdb

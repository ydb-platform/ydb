#include "host_stat.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void THostStat::OnRequest(EOperation operation)
{
    ++AccessInflightCount(operation);
}

void THostStat::OnError(TInstant now, EOperation operation)
{
    auto& inflight = AccessInflightCount(operation);
    // Clamp to 0 to be defensive against unbalanced OnRequest/OnError pairs.
    if (inflight > 0) {
        --inflight;
    }

    if (!FirstErrorAt) {
        FirstErrorAt = now;
    }
    LastErrorAt = now;
    ++ErrorCount;
}

void THostStat::OnSuccess(
    TInstant now,
    TDuration executionTime,
    EOperation operation)
{
    Y_UNUSED(executionTime);

    auto& inflight = AccessInflightCount(operation);
    if (inflight > 0) {
        --inflight;
    }

    LastSuccessAt = now;
    FirstErrorAt = TInstant();
    LastErrorAt = TInstant();
    ErrorCount = 0;
}

THostStat::TErrorsInfo THostStat::GetErrorsInfo(TInstant now) const
{
    TErrorsInfo result;
    if (FirstErrorAt) {
        result.FromFirstError = now - FirstErrorAt;
    }
    if (LastErrorAt) {
        result.FromLastError = now - LastErrorAt;
    }
    result.ErrorCount = ErrorCount;
    return result;
}

size_t THostStat::InflightCount(EOperation operation) const
{
    return InflightByOperation[static_cast<size_t>(operation)];
}

TString THostStat::DebugPrint() const
{
    TStringBuilder inflight;
    for (size_t i = 0; i < InflightByOperation.size(); ++i) {
        if (InflightByOperation[i] == 0) {
            continue;
        }
        if (!inflight.empty()) {
            inflight << ", ";
        }

        inflight << ToString(static_cast<EOperation>(i)) << ": "
                 << InflightByOperation[i];
    }

    TStringBuilder sb;
    const TInstant now = TInstant::Now();
    if (LastSuccessAt) {
        sb << "LastSuccess: " << FormatDuration(now - LastSuccessAt);
    }
    if (FirstErrorAt) {
        sb << "FirstError: " << FormatDuration(now - FirstErrorAt);
    }
    if (LastErrorAt) {
        sb << "LastError: " << FormatDuration(now - LastErrorAt);
    }

    sb << ", ErrorCount: " << ErrorCount << ", InflightByOperation: ["
       << inflight << "]";

    return sb;
}

size_t& THostStat::AccessInflightCount(EOperation operation)
{
    return InflightByOperation[static_cast<size_t>(operation)];
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

#include "indexation.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TIndexationCounters::TIndexationCounters(const TString& module)
    : TBase(module)
{
    SubColumnCounters = std::make_shared<TSubColumnCounters>(CreateSubGroup("Speciality", "SubColumns"));
    CompactionDuration = TBase::GetHistogram("CompactionDuration", NMonitoring::ExponentialHistogram(18, 2, 20));
    HistogramCompactionInputBytes = TBase::GetHistogram("CompactionInput/Bytes", NMonitoring::ExponentialHistogram(18, 2, 1024));
    HistogramCompactionCorrectRawBytes = TBase::GetHistogram("CompactionCorrectInput/Bytes", NMonitoring::ExponentialHistogram(18, 2, 1024));
    HistogramCompactionHugeRawBytes = TBase::GetHistogram("CompactionHugeInput/Bytes", NMonitoring::ExponentialHistogram(18, 2, 1024));
    CompactionHugePartsCount = TBase::GetDeriviative("CompactionHugeInput/Parts");

    CompactionInputBytes = TBase::GetDeriviative("CompactionInput/Bytes");
    CompactionFails = TBase::GetDeriviative("CompactionFails/Count");

    SplitterCounters = std::make_shared<TSplitterCounters>(*this);
}

}   // namespace NKikimr::NColumnShard

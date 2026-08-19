#pragma once

#include <ydb/core/base/tablet_types.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr {

struct TDetailedMetricsCounterNames {
    THashSet<TString> ExecutorNames;
    THashSet<TString> AppNames;
};

const TDetailedMetricsCounterNames* GetDetailedMetricsCounterNames(TTabletTypes::EType tabletType);

} // namespace NKikimr

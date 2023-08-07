#pragma once

#include "registry.h"

#include <yt/yt/library/profiling/tag.h>
#include <yt/yt/library/profiling/summary.h>
#include <yt/yt/library/profiling/histogram_snapshot.h>
#include <yt/yt/library/profiling/solomon/sensor_dump.pb.h>

#include <util/generic/hash_set.h>

#include <deque>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSummaryDouble* proto, const TSummarySnapshot<double>& summary);
void FromProto(TSummarySnapshot<double>* summary, const NProto::TSummaryDouble& proto);

void ToProto(NProto::TSummaryDuration* proto, const TSummarySnapshot<TDuration>& summary);
void FromProto(TSummarySnapshot<TDuration>* summary, const NProto::TSummaryDuration& proto);

void ToProto(NProto::THistogramSnapshot* proto, const THistogramSnapshot& histogram);
void FromProto(THistogramSnapshot* histogram, const NProto::THistogramSnapshot& proto);

////////////////////////////////////////////////////////////////////////////////

class TRemoteRegistry final
{
public:
    explicit TRemoteRegistry(TSolomonRegistry* registry);
    void Transfer(const NProto::TSensorDump& dump);
    void Detach();

private:
    TSolomonRegistry* Registry_ = nullptr;

    std::deque<TTagId> TagRename_;

    struct TRemoteSensorSet
    {
        THashSet<std::pair<ESensorType, TTagIdList>> UsedTags;
    };

    THashMap<TString, TRemoteSensorSet> Sensors_;

    TTagIdList RenameTags(const TTagIdList& tags);
    void DoDetach(const THashMap<TString, TRemoteSensorSet>& sensors);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

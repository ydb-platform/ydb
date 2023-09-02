#pragma once

#include "libporto.hpp"

#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/type.h>

#include <library/cpp/porto/proto/rpc.pb.h>
namespace Porto {

constexpr const char *M_CTXSW = "ctxsw";

enum class EMetric {
        NONE,
        CTXSW,
};

class TMetric {
public:
    TString Name;
    EMetric Metric;

    TMetric(const TString& name, EMetric metric);

    void ClearValues(const TVector<TString>& names, TMap<TString, uint64_t>& values) const;
    EError GetValues(const TVector<TString>& names, TMap<TString, uint64_t>& values, TPortoApi& api) const;

    // Returns value of metric from /proc/tid/sched for some tid
    uint64_t GetTidSchedMetricValue(int procFd, const TString& tid, const TString& metricName) const;

    void TidSnapshot(TVector<TString>& tids) const;
    void GetPidTasks(const TString& pid, TVector<TString>& tids) const;

    // Returns freezer cgroup from /proc/tid/cgroup
    TString GetFreezerCgroup(int procFd, const TString& tid) const;

    // Resurns clean cgroup[freezer] for containers names
    TMap<TString, TString> GetCtFreezerCgroups(const TGetResponse* response) const;

    // Verify inclusion of container cgroup in process cgroup
    bool MatchCgroups(const TString& tidCgroup, const TString& ctCgroup) const;

private:
    virtual uint64_t GetMetric(int procFd, const TString& tid) const = 0;
};

extern TMap<TString, TMetric*> ProcMetrics;
} /* namespace Porto */

#include "metrics.hpp"

#include <util/folder/path.h>
#include <util/generic/maybe.h>
#include <util/stream/file.h>

namespace Porto {

TMap<TString, TMetric*> ProcMetrics;

TMetric::TMetric(const TString& name, EMetric metric) {
    Name = name;
    Metric = metric;
    ProcMetrics[name] = this;
}

void TMetric::ClearValues(const TVector<TString>& names, TMap<TString, uint64_t>& values) const {
    values.clear();

    for (const auto&name : names)
        values[name] = 0;
}

EError TMetric::GetValues(const TVector<TString>& names, TMap<TString, uint64_t>& values, TPortoApi& api) const {
    ClearValues(names, values);

    int procFd = open("/proc", O_RDONLY | O_CLOEXEC | O_DIRECTORY | O_NOCTTY);
    TFileHandle procFdHandle(procFd);
    if (procFd == -1)
        return EError::Unknown;

    TVector<TString> tids;
    TidSnapshot(tids);

    auto getResponse = api.Get(names, TVector<TString>{"cgroups[freezer]"});

    if (getResponse == nullptr)
        return EError::Unknown;

    const auto containersCgroups = GetCtFreezerCgroups(getResponse);

    for (const auto& tid : tids) {
        const TString tidCgroup = GetFreezerCgroup(procFd, tid);
        if (tidCgroup == "")
            continue;

        TMaybe<uint64_t> metricValue;

        for (const auto& keyval : containersCgroups) {
            const TString& containerCgroup = keyval.second;
            if (MatchCgroups(tidCgroup, containerCgroup)) {
                if (!metricValue)
                    metricValue = GetMetric(procFd, tid);
                values[keyval.first] += *metricValue;
            }
        }
    }

    return EError::Success;
}

uint64_t TMetric::GetTidSchedMetricValue(int procFd, const TString& tid, const TString& metricName) const {
    const TString schedPath = tid + "/sched";
    try {
        int fd = openat(procFd, schedPath.c_str(), O_RDONLY | O_CLOEXEC | O_NOCTTY, 0);
        TFile file(fd);
        if (!file.IsOpen())
            return 0ul;

        TIFStream iStream(file);
        TString line;
        while (iStream.ReadLine(line)) {
            auto metricPos = line.find(metricName);

            if (metricPos != TString::npos) {
                auto valuePos = metricPos;

                while (valuePos < line.size() && !::isdigit(line[valuePos]))
                    ++valuePos;

                TString value = line.substr(valuePos);
                if (!value.empty() && IsNumber(value))
                    return IntFromString<uint64_t, 10>(value);
            }
        }
    }
    catch(...) {}

    return 0ul;
}

void TMetric::GetPidTasks(const TString& pid, TVector<TString>& tids) const {
    TFsPath task("/proc/" + pid + "/task");
    TVector<TString> rawTids;

    try {
        task.ListNames(rawTids);
    }
    catch(...) {}

    for (const auto& tid : rawTids) {
            tids.push_back(tid);
    }
}

void TMetric::TidSnapshot(TVector<TString>& tids) const {
    TFsPath proc("/proc");
    TVector<TString> rawPids;

    try {
        proc.ListNames(rawPids);
    }
    catch(...) {}

    for (const auto& pid : rawPids) {
        if (IsNumber(pid))
            GetPidTasks(pid, tids);
    }
}

TString TMetric::GetFreezerCgroup(int procFd, const TString& tid) const {
    const TString cgroupPath = tid + "/cgroup";
    try {
        int fd = openat(procFd, cgroupPath.c_str(), O_RDONLY | O_CLOEXEC | O_NOCTTY, 0);
        TFile file(fd);
        if (!file.IsOpen())
            return TString();

        TIFStream iStream(file);
        TString line;

        while (iStream.ReadLine(line)) {
            static const TString freezer = ":freezer:";
            auto freezerPos = line.find(freezer);

            if (freezerPos != TString::npos) {
                line = line.substr(freezerPos + freezer.size());
                return line;
            }
        }
    }
    catch(...){}

    return TString();
}

TMap<TString, TString> TMetric::GetCtFreezerCgroups(const TGetResponse* response) const {
    TMap<TString, TString> containersProps;

    for (const auto& ctGetListResponse : response->list()) {
        for (const auto& keyval : ctGetListResponse.keyval()) {
            if (!keyval.error()) {
                TString value = keyval.value();
                static const TString freezerPath = "/sys/fs/cgroup/freezer";

                if (value.find(freezerPath) != TString::npos)
                    value = value.substr(freezerPath.size());

                containersProps[ctGetListResponse.name()] = value;
            }
        }
    }

    return containersProps;
}

bool TMetric::MatchCgroups(const TString& tidCgroup, const TString& ctCgroup) const {
    if (tidCgroup.size() <= ctCgroup.size())
        return tidCgroup == ctCgroup;
    return ctCgroup ==  tidCgroup.substr(0, ctCgroup.size()) && tidCgroup[ctCgroup.size()] == '/';
}

class TCtxsw : public TMetric {
public:
    TCtxsw() : TMetric(M_CTXSW, EMetric::CTXSW)
    {}

    uint64_t GetMetric(int procFd, const TString& tid) const override {
        return GetTidSchedMetricValue(procFd, tid, "nr_switches");
    }
} static Ctxsw;

} /* namespace Porto */

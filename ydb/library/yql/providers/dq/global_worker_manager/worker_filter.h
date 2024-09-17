#pragma once

#include <ydb/library/yql/providers/dq/worker_manager/interface/worker_info.h>

namespace NYql {

struct TResourceStat {
    TString Id;
    Yql::DqsProto::TFile::EFileType ObjectType;
    TString Name;
    i64 Size;
    TDuration UseTime;
    TDuration WaitTime;
    i64 UseCount = 0;
    i64 TryCount = 0;
    bool Uploaded = false;
    TInstant LastSeenTime = TInstant::Now();

    TResourceStat(const TString& id, Yql::DqsProto::TFile::EFileType type, const TString& name, i64 size)
        : Id(id)
        , ObjectType(type)
        , Name(name)
        , Size(size)
    { }
};

class TWorkerFilter {
public:
    struct TStats {
        THashMap<TString, THashSet<int>>* WaitingResources;
        THashMap<TString, TResourceStat>* Uploaded;
    };

    enum EMatchStatus {
        EFAIL = 0,
        EOK = 1,
        EPARTIAL = 2
    };

    TWorkerFilter(const Yql::DqsProto::TWorkerFilter& filter);

    EMatchStatus Match(const NDqs::TWorkerInfo::TPtr& workerInfo, int taskId, TStats* stats) const;

    void Visit(const std::function<void(const Yql::DqsProto::TFile&)>& visitor) const;

private:
    Yql::DqsProto::TWorkerFilter Filter;
    bool FullMatch;
    THashSet<TString> Addresses;
    THashSet<ui64> NodeIds;
    THashSet<ui64> NodeIdHints;
};

} // namespace NYql

#include "user_job_statistics.h"
#include <yt/cpp/mapreduce/common/helpers.h>
#include <util/stream/null.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/env.h>

using namespace NYtTools;

static TMutex GlobalStatsWritingMutex;

#if defined(_unix_)
const FHANDLE TUserJobStatsProxy::JobStatisticsHandle = 5;
#elif defined(_win_)
const FHANDLE TUserJobStatsProxy::JobStatisticsHandle = nullptr;
#endif

static IOutputStream* CorrectHandle(const FHANDLE h) {
#if defined(_unix_)
    if (fcntl(h, F_GETFD) == -1) {
        return &Cerr;
    }
    return nullptr;
#elif defined(_win_)
    return &Cerr;
#endif
}

static TString PrintNodeSimple(const NYT::TNode& n) {
    return NYT::NodeToYsonString(n, NYson::EYsonFormat::Text);
}

void TUserJobStatsProxy::Init(IOutputStream * usingStream) {
    if (usingStream == nullptr) {
        usingStream = CorrectHandle(JobStatisticsHandle);
    }

    if (usingStream == nullptr && GetEnv("YT_JOB_ID").empty()) {
        usingStream = &Cerr;
    }


    if (usingStream == nullptr) {
        TFileHandle fixedDesrc(JobStatisticsHandle);
        FetchedOut = MakeHolder<TFixedBufferFileOutput>(TFile(fixedDesrc.Duplicate()));
        UsingStream = FetchedOut.Get();
        fixedDesrc.Release();
    } else {
        UsingStream = usingStream;
    }
}

void TUserJobStatsProxy::InitChecked(IOutputStream* def) {
    IOutputStream* usingStream =  CorrectHandle(JobStatisticsHandle);

    if (usingStream == nullptr && !GetEnv("YT_JOB_ID").empty()) {
        TFileHandle fixedDesrc(JobStatisticsHandle);
        FetchedOut = MakeHolder<TFixedBufferFileOutput>(TFile(fixedDesrc.Duplicate()));
        UsingStream = FetchedOut.Get();
        fixedDesrc.Release();
    } else {
        UsingStream = def;
    }
}

void TUserJobStatsProxy::InitIfNotInited(IOutputStream * usingStream) {
    if (UsingStream == nullptr) {
        Init(usingStream);
    }
}

void TUserJobStatsProxy::CommitStats() {
    if (Stats.empty()) {
        return;
    }

    auto res = NYT::TNode::CreateMap();
    for (auto& p : Stats) {
        res[p.first] = p.second;
    }
    for (auto& p : TimeStats) {
        res[p.first] = p.second.MilliSeconds();
    }
    with_lock(GlobalStatsWritingMutex) {
        *UsingStream << PrintNodeSimple(res) << ";" << Endl;
    }
    Stats.clear();
}


TTimeStatHolder TUserJobStatsProxy::TimerStart(TString name, bool commitOnFinish) {
    return THolder(new TTimeStat(this, name, commitOnFinish));
}

void TUserJobStatsProxy::WriteStat(TString name, i64 val) {
    auto res = NYT::TNode {} (name, val);
    with_lock(GlobalStatsWritingMutex) {
        *UsingStream << PrintNodeSimple(res) << ";" << Endl;
    }
}

void TUserJobStatsProxy::WriteStatNoFlush(TString name, i64 val) {
    auto res = NYT::TNode {} (name, val);
    with_lock(GlobalStatsWritingMutex) {
        *UsingStream << (TStringBuilder{} << PrintNodeSimple(res) << ";\n");
    }
}

TTimeStat::TTimeStat(TUserJobStatsProxy* parent, TString name, bool commit)
    : Parent(parent)
    , Name(name)
    , Commit(commit) {}

TTimeStat::~TTimeStat() {
    Finish();
}

void TTimeStat::Cancel() {
    Parent = nullptr;
}

void TTimeStat::Finish() {
    if (!Parent) {
        return;
    }

    if (Commit) {
        Parent->WriteStatNoFlush(Name, Timer.Get().MilliSeconds());
    } else {
        Parent->TimeStats[Name] += Timer.Get();
    }
    Cancel();
}

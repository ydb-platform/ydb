#include "job.h"

#include <util/stream/file.h>

const std::string LogFileName = "benchmark.log";


TThreadJob::TThreadJob(const TCommonOptions& opts)
    : RpsProvider(opts.Rps)
    , Prefix(opts.DatabaseOptions.Prefix)
    , Stats(
        opts.ReactionTime,
        opts.ResultFileName,
        !opts.DontPushMetrics,
        opts.RetryMode
    )
    , StopOnError(opts.StopOnError)
    , MaxDelay(opts.ReactionTime)
    , UseFollowers(opts.UseFollowers)
{
}

// virtual
void TThreadJob::Start(TInstant deadline) {
    Deadline = deadline;
    StartThread();
}

void TThreadJob::StartThread() {
    auto threadFunc = [this]() {
        Stats.Reset();
        RpsProvider.Reset();
        DoJob();
        Stats.Finish();
        OnFinish();
    };
    WorkThread.reset(SystemThreadFactory()->Run(threadFunc).Release());
}

void TThreadJob::Stop() {
    if (WorkThread) {
        ShouldStop.store(true);
    } else {
        Cerr << (TStringBuilder() << "TGenerateInitialContentJob::Stop Error: Thread is not running." << Endl);
    }
}

void TThreadJob::Wait() {
    if (WorkThread) {
        WorkThread->Join();
    }
}
void TThreadJob::OnFinish() {
}

void TJobContainer::Add(TThreadJob* job) {
    Y_ABORT_UNLESS(job);
    Jobs.push_back(std::unique_ptr<TThreadJob>(job));
}

void TJobContainer::Start(TInstant deadline) {
    for (auto& job : Jobs) {
        job->Start(deadline);
    }
}

void TJobContainer::Stop() {
    for (auto& job : Jobs) {
        job->Stop();
    }
}

void TJobContainer::Wait() {
    for (auto& job : Jobs) {
        job->Wait();
    }
}

void TJobContainer::ShowProgress() {
    TStringBuilder report;
    for (auto& job : Jobs) {
        job->ShowProgress(report);
    }
    Cout << report;
    TFile logFile(LogFileName, OpenAlways | WrOnly | Seq | ForAppend);
    TFileOutput out(logFile);
    out << report;
}

void SetUpInteraction() {
    signal(SIGUSR1, [](int) -> void {
        Cout << (TStringBuilder() << TInstant::Now().ToRfc822StringLocal() << " " << "SIGUSR1 handle" << Endl);
        std::shared_ptr<TJobContainer> jobs = *Singleton<std::shared_ptr<TJobContainer>>();
        if (jobs) {
            jobs->ShowProgress();
        } else {
            Cerr << "Jobs are already destroyed" << Endl;
        }
    });

    signal(SIGINT, [](int) -> void {
        Cerr << (TStringBuilder() << TInstant::Now().ToRfc822StringLocal() << " " << "SIGINT handle received. Stop signal sent." << Endl);
        std::shared_ptr<TJobContainer> jobs = *Singleton<std::shared_ptr<TJobContainer>>();
        if (jobs) {
            jobs->Stop();
        } else {
            Cerr << "Jobs are already destroyed" << Endl;
        }
    });
}

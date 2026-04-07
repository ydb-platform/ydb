#pragma once

#include "statistics.h"
#include "utils.h"

#include <util/string/builder.h>
#include <util/thread/pool.h>

class TThreadJob {
public:
    TThreadJob(const TCommonOptions& opts, const std::string& operationType);
    virtual ~TThreadJob() = default;

    virtual void Start(TInstant deadline);
    void Stop();
    void Wait();
    virtual void OnFinish();

    virtual void ShowProgress(TStringBuilder& report) = 0;
    virtual void DoJob() = 0;

protected:
    void StartThread();

    TInstant Deadline;
    TRpsProvider RpsProvider;
    const std::string& Prefix;
    std::atomic<bool> ShouldStop = false;
    std::atomic<bool> ErrorOccured = false;
    std::unique_ptr<IThreadFactory::IThread> WorkThread;
    TInstant StartTime;
    TStat Stats;
    bool StopOnError;
    TDuration MaxDelay;
};

class TJobContainer {
public:
    void Add(TThreadJob* job);
    void Start(TInstant deadline = TInstant());
    void Stop();
    void Wait();
    void ShowProgress();

private:
    std::vector<std::unique_ptr<TThreadJob>> Jobs;
};

class TJobGC {
public:
    TJobGC(std::shared_ptr<TJobContainer>& jobs)
        : Jobs(jobs)
    {}

    ~TJobGC() {
        if (Jobs) {
            Jobs->Wait();
            Jobs.reset();
        }
    }

private:
    std::shared_ptr<TJobContainer>& Jobs;
};

void SetUpInteraction();

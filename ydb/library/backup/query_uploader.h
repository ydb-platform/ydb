#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/bucket_quoter/bucket_quoter.h>

#include <util/thread/pool.h>
#include <util/generic/string.h>
#include <util/generic/map.h>

namespace NYdb::NBackup {

class TUploader {
public:
    struct TOptions {
        ui32 Rate = 20; // requests per Interval
        TDuration Interval = TDuration::Seconds(1);
        ui32 InFly = 10;
        ui32 RetryOperaionMaxRetries = 30;
        ui32 TransportErrorsMaxRetries = 9;
        TDuration ReactionTime = TDuration::MilliSeconds(50);

        ui32 GetRps() const;
    };

private:
    const TOptions Opts;
    const TString Query;

    TAtomic ShouldStop;
    TSimpleSharedPtr<IThreadPool> TasksQueue;
    // Total wait is 1 * (2 ** TransportErrorsMaxRetries - 1), for TransportErrorsMaxRetries == 9 it gives ~8.5 minutes
    TDuration BulkUpsertRetryDuration = TDuration::Seconds(1);

    using TRpsLimiter = TBucketQuoter<ui64>;
    TRpsLimiter RequestLimiter;
    NYdb::NTable::TTableClient& Client;

public:
    TUploader(const TOptions& opts, NYdb::NTable::TTableClient& client, const TString& query);

    bool Push(TParams params);
    bool Push(const TString& path, TValue&& value);

    void WaitAllJobs() {
        TasksQueue->Stop();
    }

    void PleaseStop() {
        AtomicSet(ShouldStop, 1);
    }

    bool IsStopped() const {
        return AtomicGet(ShouldStop) == 1;
    }
};

} // NYdb::Backup

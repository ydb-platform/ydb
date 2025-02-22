#include "secondary_index.h"

#include <util/thread/pool.h>

#include <mutex>
#include <random>

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NStatusHelpers;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TExecutor {
public:
    TExecutor()
        : Queue(TThreadPool::TParams().SetBlocking(true).SetCatching(false))
    { }

    ~TExecutor() {
        Stop(false);
    }

    void Start(size_t threads) {
        Queue.Start(threads, threads);
    }

    void Stop(bool rethrow = true) {
        {
            std::lock_guard guard(Lock);
            Stopped = true;
        }
        Wait(rethrow);
    }

    void Wait(bool rethrow = true) {
        Queue.Stop();
        if (rethrow) {
            std::lock_guard guard(Lock);
            if (Exception) {
                std::rethrow_exception(Exception);
            }
        }
    }

    bool Execute(std::function<void()> callback) {
        {
            std::lock_guard guard(Lock);
            if (Stopped) {
                if (Exception) {
                    std::rethrow_exception(Exception);
                } else {
                    return false;
                }
            }
        }

        std::unique_ptr<TTask> task = std::make_unique<TTask>(this, std::move(callback));
        if (Queue.Add(task.get())) {
            Y_UNUSED(task.release());
            return true;
        }

        return false;
    }

private:
    struct TTask : public IObjectInQueue {
        TExecutor* const Owner;
        std::function<void()> Callback;

        TTask(TExecutor* owner, std::function<void()> callback)
            : Owner(owner)
            , Callback(std::move(callback))
        { }

        void Process(void*) override {
            std::unique_ptr<TTask> self(this);
            {
                std::lock_guard guard(Owner->Lock);
                if (Owner->Stopped) {
                    return;
                }
            }
            try {
                auto callback = std::move(Callback);
                callback();
            } catch (...) {
                std::lock_guard guard(Owner->Lock);
                if (!Owner->Stopped && !Owner->Exception) {
                    Owner->Stopped = true;
                    Owner->Exception = std::current_exception();
                }
            }
        }
    };

private:
    TAdaptiveLock Lock;
    bool Stopped = false;
    std::exception_ptr Exception;
    TThreadPool Queue;
};

}

////////////////////////////////////////////////////////////////////////////////

static TStatus InsertSeries(TSession& session, const std::string& prefix, const TSeries& series) {
    auto queryText = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $seriesId AS Uint64;
        DECLARE $title AS Utf8;
        DECLARE $seriesInfo AS Utf8;
        DECLARE $releaseDate AS Uint32;
        DECLARE $views AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;
        $revViews = $maxUint64 - $views;

        INSERT INTO series (series_id, title, series_info, release_date, views)
        VALUES ($seriesId, $title, $seriesInfo, $releaseDate, $views);

        -- Insert above already verified series_id is unique, so it is safe to use upsert
        UPSERT INTO series_rev_views (rev_views, series_id)
        VALUES ($revViews, $seriesId);
    )", prefix);

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();

    auto params = query.GetParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(series.SeriesId)
            .Build()
        .AddParam("$title")
            .Utf8(series.Title)
            .Build()
        .AddParam("$seriesInfo")
            .Utf8(series.SeriesInfo)
            .Build()
        .AddParam("$releaseDate")
            .Uint32(series.ReleaseDate.Days())
            .Build()
        .AddParam("$views")
            .Uint64(series.Views)
            .Build()
        .Build();

    auto result = query.Execute(
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();

    return result;
}

int RunGenerateSeries(TDriver& driver, const std::string& prefix, int argc, char** argv) {
    TOpts opts = TOpts::Default();

    uint64_t seriesId = 1;
    uint64_t count = 10;
    size_t threads = 10;

    opts.AddLongOption("start", "First id to generate").Optional().RequiredArgument("NUM")
        .StoreResult(&seriesId);
    opts.AddLongOption("count", "Number of series to generate").Required().RequiredArgument("NUM")
        .StoreResult(&count);
    opts.AddLongOption("threads", "Number of threads to use").Optional().RequiredArgument("NUM")
        .StoreResult(&threads);
    TOptsParseResult res(&opts, argc, argv);

    TTableClient client(driver);
    TExecutor executor;
    executor.Start(threads);

    size_t generated = 0;
    std::mt19937_64 engine;
    std::uniform_int_distribution<uint64_t> dist(0, 1000000);
    while (count > 0) {
        bool ok = executor.Execute([views = dist(engine), &client, &prefix, seriesId] {
            TSeries series;
            series.SeriesId = seriesId;
            series.Title = TStringBuilder() << "Name " << seriesId;
            series.SeriesInfo = TStringBuilder() << "Info " << seriesId;
            series.ReleaseDate = TInstant::Days(TInstant::Now().Days());
            series.Views = views;
            ThrowOnError(client.RetryOperationSync([&prefix, &series](TSession session) -> TStatus {
                return InsertSeries(session, prefix, series);
            }));
        });
        if (!ok) {
            break;
        }
        ++generated;
        ++seriesId;
        --count;
    }

    executor.Wait();
    std::cout << "Generated " << generated << " new series" << std::endl;
    return 0;
}

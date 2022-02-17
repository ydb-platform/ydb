#include "secondary_index.h"

#include <util/random/random.h>
#include <util/thread/pool.h>

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;

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
        with_lock (Lock) {
            Stopped = true;
        }
        Wait(rethrow);
    }

    void Wait(bool rethrow = true) {
        Queue.Stop();
        if (rethrow) {
            with_lock (Lock) {
                if (Exception) {
                    std::rethrow_exception(Exception);
                }
            }
        }
    }

    bool Execute(std::function<void()> callback) {
        with_lock (Lock) {
            if (Stopped) {
                if (Exception) {
                    std::rethrow_exception(Exception);
                } else {
                    return false;
                }
            }
        }

        THolder<TTask> task = MakeHolder<TTask>(this, std::move(callback));
        if (Queue.Add(task.Get())) {
            Y_UNUSED(task.Release());
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
            THolder<TTask> self(this);
            with_lock (Owner->Lock) {
                if (Owner->Stopped) {
                    return;
                }
            }
            try {
                auto callback = std::move(Callback);
                callback();
            } catch (...) {
                with_lock (Owner->Lock) {
                    if (!Owner->Stopped && !Owner->Exception) {
                        Owner->Stopped = true;
                        Owner->Exception = std::current_exception();
                    }
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

static TStatus InsertSeries(TSession& session, const TString& prefix, const TSeries& series) {
    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%1$s");

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
    )", prefix.data());

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

int RunGenerateSeries(TDriver& driver, const TString& prefix, int argc, char** argv) {
    TOpts opts = TOpts::Default();

    ui64 seriesId = 1;
    ui64 count = 10;
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
    while (count > 0) {
        bool ok = executor.Execute([&client, &prefix, seriesId] {
            TSeries series;
            series.SeriesId = seriesId;
            series.Title = TStringBuilder() << "Name " << seriesId;
            series.SeriesInfo = TStringBuilder() << "Info " << seriesId;
            series.ReleaseDate = TInstant::Days(TInstant::Now().Days());
            series.Views = RandomNumber<ui64>(1000000);
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
    Cout << "Generated " << generated << " new series" << Endl;
    return 0;
}

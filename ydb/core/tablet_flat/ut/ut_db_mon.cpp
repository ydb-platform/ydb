#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tablet/tablet_monitoring_proxy.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/testlib/actors/wait_events.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>
#include <util/string/cast.h>
#include <util/system/env.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace {

struct TDbMonRowsModel {
    enum : ui32 {
        TableId = 101,
        ColumnKeyId = 1,
        ColumnValueId = 20,
    };

    struct TTxSchema : public ITransaction {
        bool Execute(TTransactionContext& txc, const TActorContext&) override
        {
            if (txc.DB.GetScheme().GetTableInfo(TableId)) {
                return true;
            }

            TCompactionPolicy policy;
            txc.DB.Alter()
                .AddTable("dbmon_test", TableId)
                .AddColumn(TableId, "key", ColumnKeyId, NScheme::TInt64::TypeId, false, false)
                .AddColumn(TableId, "value", ColumnValueId, NScheme::TString::TypeId, false, false)
                .AddColumnToKey(TableId, ColumnKeyId)
                .SetCompactionPolicy(TableId, policy);

            return true;
        }

        void Complete(const TActorContext& ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }
    };

    struct TTxAddRows : public ITransaction {
        explicit TTxAddRows(ui32 rows)
            : Rows(rows)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override
        {
            for (ui32 key = 0; key < Rows; ++key) {
                const auto keyCell = NScheme::TInt64::TInstance(key);
                const auto valueCell = NScheme::TString::TInstance(TString(512, 'x'));
                NTable::TUpdateOp op{ ColumnValueId, NTable::ECellOp::Set, valueCell };

                txc.DB.Update(TableId, NTable::ERowOp::Upsert, { keyCell }, { op });
            }

            return true;
        }

        void Complete(const TActorContext& ctx) override
        {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        const ui32 Rows;
    };

    static NFake::TEvExecute* MakeScheme()
    {
        return new NFake::TEvExecute{ new TTxSchema };
    }

    static NFake::TEvExecute* MakeRows(ui32 rows)
    {
        return new NFake::TEvExecute{ new TTxAddRows(rows) };
    }
};

class TDbMonTablet : public TActor<TDbMonTablet>, public TTabletExecutedFlat {
public:
    TDbMonTablet(const TActorId& sender, const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
        , Sender(sender)
    {}

private:
    void CompactionComplete(ui32 table, const TActorContext&) override
    {
        Send(Sender, new NFake::TEvCompacted(table));
    }

    void Handle(NFake::TEvExecute::TPtr& ev, const TActorContext& ctx)
    {
        for (auto& tx : ev->Get()->Txs) {
            Execute(tx.Release(), ctx);
        }
        for (auto& lambda : ev->Get()->Lambdas) {
            std::move(lambda)(Executor(), ctx);
        }
    }

    void Handle(NFake::TEvCompact::TPtr& ev, const TActorContext&)
    {
        if (ev->Get()->MemOnly) {
            Executor()->CompactMemTable(ev->Get()->Table);
        } else {
            Executor()->CompactTable(ev->Get()->Table);
        }
        Send(Sender, new TEvents::TEvWakeup);
    }

    void Handle(NFake::TEvReturn::TPtr&, const TActorContext&)
    {
        Send(Sender, new TEvents::TEvWakeup);
    }

    void Handle(TEvents::TEvPoison::TPtr&, const TActorContext& ctx)
    {
        Become(&TThis::StateBroken);
        Executor()->DetachTablet();
        Detach(ctx);
        ctx.Send(Sender, new TEvents::TEvGone);
    }

    void DefaultSignalTabletActive(const TActorContext&) override
    {}

    void OnActivateExecutor(const TActorContext&) override
    {
        Executor()->RegisterExternalTabletCounters(new TTabletCountersBase);
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
        Send(Sender, new TEvents::TEvWakeup);
    }

    void OnDetach(const TActorContext& ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) override
    {
        Die(ctx);
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NFake::TEvExecute, Handle);
            HFunc(NFake::TEvCompact, Handle);
            HFunc(NFake::TEvReturn, Handle);
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            HFunc(TEvents::TEvPoison, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
            break;
        }
    }

    STFUNC(StateBroken)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        }
    }

private:
    const TActorId Sender;
};

class TDbMonEnv : public TMyEnvBase {
public:
    TDbMonEnv()
    {
        FireTablet(Edge, Tablet, [edge = Edge](const TActorId& tablet, TTabletStorageInfo* info) {
            return new TDbMonTablet(edge, tablet, info);
        });

        WaitForWakeUp();
        SendSync(TDbMonRowsModel::MakeScheme());
    }

    ~TDbMonEnv()
    {
        SendSync(new TEvents::TEvPoison, false, true);
    }

    void AddRowsAndCompact(ui32 rows)
    {
        SendSync(TDbMonRowsModel::MakeRows(rows));
        SendSync(new NFake::TEvCompact(TDbMonRowsModel::TableId));
        WaitFor<NFake::TEvCompacted>();
    }

    TString RequestDbPage(
            ui64 rowsOffset,
            ui64 maxRows,
            bool disableOffsetScanResume,
            bool disableOffsetScanPrecharge)
    {
        TStringBuilder request;
        request << "/db?TabletID=" << Tablet
            << "&TableID=" << ui32(TDbMonRowsModel::TableId)
            << "&RowsOffset=" << rowsOffset
            << "&MaxRows=" << maxRows
            << "&MaxString=1"
            << "&TraceOffsetScan=1";
        if (disableOffsetScanResume) {
            request << "&DisableOffsetScanResume=1";
        }
        if (disableOffsetScanPrecharge) {
            request << "&DisableOffsetScanPrecharge=1";
        }

        SendAsync(new NMon::TEvRemoteHttpInfo(request));

        return GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>()->Get()->Html;
    }

    void SetSharedCacheLimit(ui64 memoryLimit)
    {
        TWaitForFirstEvent<NMemory::TEvConsumerLimit> wait(Env);
        Env.Send(NSharedCache::MakeSharedPageCacheId(), TActorId{}, new NMemory::TEvConsumerLimit(memoryLimit));
        wait.Wait();
    }

    ui64 GetExecutorCumulativeCounter(TStringBuf name)
    {
        SendAsync(new TEvTablet::TEvGetCounters);
        const auto response = GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>();
        const auto& counters = response->Get()->Record.GetTabletCounters().GetExecutorCounters();
        for (const auto& counter : counters.GetCumulativeCounters()) {
            if (counter.GetName() == name) {
                return counter.GetValue();
            }
        }
        UNIT_FAIL("Missing executor cumulative counter " << name);
        return 0;
    }

    ui64 GetExecutorSimpleCounter(TStringBuf name)
    {
        SendAsync(new TEvTablet::TEvGetCounters);
        const auto response = GrabEdgeEvent<TEvTablet::TEvGetCountersResponse>();
        const auto& counters = response->Get()->Record.GetTabletCounters().GetExecutorCounters();
        for (const auto& counter : counters.GetSimpleCounters()) {
            if (counter.GetName() == name) {
                return counter.GetValue();
            }
        }
        UNIT_FAIL("Missing executor simple counter " << name);
        return 0;
    }
};

class TDbMonHttpRequest : public NMonitoring::IHttpRequest {
public:
    TDbMonHttpRequest(ui64 tabletId, ui32 tableId, ui64 rowsOffset, ui64 maxRows)
        : Uri("/tablets/db")
        , Path("/tablets/db")
    {
        Params.InsertUnescaped("TabletID", ToString(tabletId));
        Params.InsertUnescaped("TableID", ToString(tableId));
        Params.InsertUnescaped("RowsOffset", ToString(rowsOffset));
        Params.InsertUnescaped("MaxRows", ToString(maxRows));
        Params.InsertUnescaped("MaxString", "1");
        Params.InsertUnescaped("DisableOffsetScanPrecharge", "1");
    }

    const char* GetURI() const override
    {
        return Uri.c_str();
    }

    const char* GetPath() const override
    {
        return Path.c_str();
    }

    const TCgiParameters& GetParams() const override
    {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override
    {
        return PostParams;
    }

    TStringBuf GetPostContent() const override
    {
        return {};
    }

    HTTP_METHOD GetMethod() const override
    {
        return HTTP_METHOD_GET;
    }

    const THttpHeaders& GetHeaders() const override
    {
        return Headers;
    }

    TString GetRemoteAddr() const override
    {
        return {};
    }

private:
    TString Uri;
    TString Path;
    TCgiParameters Params;
    TCgiParameters PostParams;
    THttpHeaders Headers;
};

struct TDbPageStats {
    TString Html;
    ui64 TxPostponed = 0;
    ui64 TxPageCacheMisses = 0;
};

TDbPageStats RequestDbPageWithStats(
        TDbMonEnv& env,
        ui64 rowsOffset,
        ui64 maxRows,
        bool disableOffsetScanResume = false,
        bool disableOffsetScanPrecharge = false)
{
    const ui64 postponedBefore = env.GetExecutorCumulativeCounter("TxPostponed");
    const ui64 missesBefore = env.GetExecutorCumulativeCounter("TxPageCacheMisses");

    TString html = env.RequestDbPage(
        rowsOffset,
        maxRows,
        disableOffsetScanResume,
        disableOffsetScanPrecharge);

    const ui64 postponedAfter = env.GetExecutorCumulativeCounter("TxPostponed");
    const ui64 missesAfter = env.GetExecutorCumulativeCounter("TxPageCacheMisses");

    return {
        std::move(html),
        postponedAfter - postponedBefore,
        missesAfter - missesBefore,
    };
}

size_t CountSubstrings(const TString& text, const TString& needle)
{
    size_t count = 0;
    size_t pos = 0;
    while ((pos = text.find(needle, pos)) != TString::npos) {
        ++count;
        pos += needle.size();
    }
    return count;
}

void AssertRenderedWindow(const TString& html, i64 firstKey, ui32 rows)
{
    size_t pos = 0;
    for (ui32 i = 0; i < rows; ++i) {
        const auto key = firstKey + i;
        const TString cell = TStringBuilder() << "<td>" << key << "</td>";
        const size_t nextPos = html.find(cell, pos);
        UNIT_ASSERT_C(nextPos != TString::npos, "Missing rendered key " << key);
        UNIT_ASSERT_VALUES_EQUAL_C(CountSubstrings(html, cell), 1, "Duplicated rendered key " << key);
        pos = nextPos + cell.size();
    }
}

ui64 GetOffsetScanDataSteps(const TString& html)
{
    const TString prefix = "<!-- DbMonOffsetScanDataSteps=";
    const size_t begin = html.find(prefix);
    UNIT_ASSERT_C(begin != TString::npos, "Missing offset scan trace in response");
    const size_t valueBegin = begin + prefix.size();
    const size_t valueEnd = html.find(" -->", valueBegin);
    UNIT_ASSERT_C(valueEnd != TString::npos, "Malformed offset scan trace in response");
    return FromString<ui64>(html.substr(valueBegin, valueEnd - valueBegin));
}

bool DisableOffsetScanResumeForValidation(bool defaultValue)
{
    const auto envValue = GetEnv("YDB_DBMON_DISABLE_OFFSET_SCAN_RESUME");
    if (envValue.empty()) {
        return defaultValue;
    }
    return envValue == "1";
}

bool DisableOffsetScanPrechargeForValidation(bool defaultValue)
{
    const auto envValue = GetEnv("YDB_DBMON_DISABLE_OFFSET_SCAN_PRECHARGE");
    if (envValue.empty()) {
        return defaultValue;
    }
    return envValue == "1";
}

} // namespace

Y_UNIT_TEST_SUITE(TabletMon) {
    Y_UNIT_TEST(ExecutorDbMonResumesOffsetScanAfterPageRetry)
    {
        constexpr ui32 Rows = 6000;
        constexpr ui32 RowsOffset = 5000;
        constexpr ui32 MaxRows = 10;

        TDbMonEnv env;
        env.AddRowsAndCompact(Rows);
        env.SetSharedCacheLimit(0);

        const ui64 postponedBefore = env.GetExecutorCumulativeCounter("TxPostponed");
        const ui64 missesBefore = env.GetExecutorCumulativeCounter("TxPageCacheMisses");

        const TString html = env.RequestDbPage(
            RowsOffset,
            MaxRows,
            DisableOffsetScanResumeForValidation(false),
            DisableOffsetScanPrechargeForValidation(true));

        const ui64 postponedAfter = env.GetExecutorCumulativeCounter("TxPostponed");
        const ui64 missesAfter = env.GetExecutorCumulativeCounter("TxPageCacheMisses");
        UNIT_ASSERT_C(postponedAfter > postponedBefore,
            "Expected /db request to hit page-cache retry path");
        UNIT_ASSERT_C(missesAfter > missesBefore,
            "Expected /db request to load table pages");

        AssertRenderedWindow(html, RowsOffset, MaxRows);
        UNIT_ASSERT_VALUES_EQUAL(GetOffsetScanDataSteps(html), RowsOffset + MaxRows);
        UNIT_ASSERT_VALUES_EQUAL(CountSubstrings(html, "<td>4999</td>"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountSubstrings(html, "<td>5010</td>"), 0);
    }

    Y_UNIT_TEST(ExecutorDbMonKeepsRenderedRowsAfterPageRetry)
    {
        constexpr ui32 Rows = 2000;
        constexpr ui32 RowsOffset = 5;
        constexpr ui32 MaxRows = 1000;

        TDbMonEnv env;
        env.AddRowsAndCompact(Rows);
        env.SetSharedCacheLimit(0);

        const TString html = env.RequestDbPage(
            RowsOffset,
            MaxRows,
            DisableOffsetScanResumeForValidation(false),
            DisableOffsetScanPrechargeForValidation(true));

        AssertRenderedWindow(html, RowsOffset, MaxRows);
        UNIT_ASSERT_VALUES_EQUAL(GetOffsetScanDataSteps(html), RowsOffset + MaxRows);
        UNIT_ASSERT_VALUES_EQUAL(CountSubstrings(html, "<td>4</td>"), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountSubstrings(html, "<td>1005</td>"), 0);
    }

    Y_UNIT_TEST(ExecutorDbMonPrechargeReducesOffsetScanRetries)
    {
        constexpr ui32 Rows = 6000;
        constexpr ui32 RowsOffset = 5000;
        constexpr ui32 MaxRows = 10;

        auto runRequest = [] (bool disableOffsetScanPrecharge) {
            TDbMonEnv env;
            env.AddRowsAndCompact(Rows);
            env.SetSharedCacheLimit(0);

            return RequestDbPageWithStats(
                env,
                RowsOffset,
                MaxRows,
                DisableOffsetScanResumeForValidation(false),
                disableOffsetScanPrecharge);
        };

        const TDbPageStats withoutPrecharge = runRequest(true);
        const TDbPageStats withPrecharge = runRequest(false);

        AssertRenderedWindow(withoutPrecharge.Html, RowsOffset, MaxRows);
        AssertRenderedWindow(withPrecharge.Html, RowsOffset, MaxRows);

        UNIT_ASSERT_C(withoutPrecharge.TxPostponed > 10,
            "Expected multiple retries without precharge, got "
                << withoutPrecharge.TxPostponed);
        UNIT_ASSERT_C(withPrecharge.TxPostponed > 0,
            "Expected precharge request to start from a real page miss");
        UNIT_ASSERT_C(withPrecharge.TxPostponed * 4 < withoutPrecharge.TxPostponed,
            "Expected precharge to reduce retries, with precharge: "
                << withPrecharge.TxPostponed
                << ", without precharge: "
                << withoutPrecharge.TxPostponed);
    }

    Y_UNIT_TEST(TabletMonitoringProxyTimeoutStopsDbMonTransaction)
    {
        constexpr ui32 Rows = 6000;
        constexpr ui32 RowsOffset = 5000;
        constexpr ui32 MaxRows = 10;

        TDbMonEnv env;
        env.AddRowsAndCompact(Rows);
        env.SetSharedCacheLimit(0);

        auto prevDispatchTimeout = env.Env.SetDispatchTimeout(TDuration::Seconds(5));
        ui32 registeredActors = 0;
        ui32 forwardingActors = 0;
        TActorId forwardingActor;
        auto prevRegistrationObserver = env.Env.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntimeBase::DefaultRegistrationObserver(runtime, parentId, actorId);
                ++registeredActors;
                if (runtime.GetActorName(actorId).Contains("TForwardingActor")) {
                    forwardingActor = actorId;
                    runtime.EnableScheduleForActor(actorId);
                    ++forwardingActors;
                }
            });
        auto prevScheduledEventsSelector = env.Env.SetScheduledEventsSelectorFunc(
            [&](TTestActorRuntimeBase&, TScheduledEventsList& scheduledEvents, TEventsList& queue) {
                auto it = scheduledEvents.begin();
                while (it != scheduledEvents.end()) {
                    auto current = it++;
                    auto& item = *current;
                    if (item.Event->GetRecipientRewrite() == forwardingActor) {
                        if (item.Cookie->Get()) {
                            if (item.Cookie->Detach()) {
                                queue.push_back(item.Event);
                            }
                        } else {
                            queue.push_back(item.Event);
                        }
                    }
                    scheduledEvents.erase(current);
                }
            });

        const TActorId proxy = env.Env.Register(NTabletMonitoringProxy::CreateTabletMonitoringProxy());
        TDispatchOptions bootstrapOptions;
        bootstrapOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT_C(env.Env.DispatchEvents(bootstrapOptions, TDuration::Seconds(1)),
            "Expected tablet monitoring proxy actor to bootstrap");

        bool delayBlobResults = true;
        bool timeoutSeen = false;
        TString timeoutBody;
        TVector<THolder<IEventHandle>> delayedBlobResults;
        ui32 remoteRequests = 0;
        ui32 delayedBlobResultCount = 0;
        ui32 remoteResponses = 0;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NMon::TEvRemoteHttpInfo::EventType:
                    ++remoteRequests;
                    break;

                case TEvBlobStorage::EvGetResult:
                    if (delayBlobResults) {
                        ++delayedBlobResultCount;
                        delayedBlobResults.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;

                case NMon::TEvHttpInfoRes::EventType:
                    if (ev->GetRecipientRewrite() == env.Edge) {
                        TStringStream body;
                        ev->Get<NMon::TEvHttpInfoRes>()->Output(body);
                        timeoutBody = body.Str();
                        timeoutSeen = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;

                case NMon::TEvRemoteHttpInfoRes::EventType:
                    ++remoteResponses;
                    return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserver = env.Env.SetObserverFunc(observer);

        TDbMonHttpRequest httpRequest(env.Tablet, TDbMonRowsModel::TableId, RowsOffset, MaxRows);
        NMonitoring::TMonService2HttpRequest monRequest(
            nullptr,
            &httpRequest,
            nullptr,
            nullptr,
            "/db",
            nullptr);

        env.Env.Send(new IEventHandle(proxy, env.Edge, new NMon::TEvHttpInfo(monRequest)));

        auto dispatchUntil = [&](std::function<bool()> condition, TDuration timeout) {
            TDispatchOptions options;
            options.CustomFinalCondition = std::move(condition);
            options.FinalEvents.emplace_back([](IEventHandle&) {
                return false;
            });
            env.Env.DispatchEvents(options, timeout);
        };

        dispatchUntil([&] {
            return delayedBlobResultCount > 0;
        }, TDuration::Seconds(5));

        UNIT_ASSERT_C(delayedBlobResultCount > 0,
            "Expected db monitoring transaction to stop on page miss"
                << ", registered actors: " << registeredActors
                << ", forwarding actors: " << forwardingActors
                << ", remote requests: " << remoteRequests);

        env.Env.AdvanceCurrentTime(TDuration::Seconds(60));
        dispatchUntil([&] {
            return timeoutSeen;
        }, TDuration::Seconds(5));

        UNIT_ASSERT_C(timeoutSeen,
            "Expected tablet monitoring proxy timeout"
                << ", delayed blob results: " << delayedBlobResultCount
                << ", remote responses: " << remoteResponses);
        UNIT_ASSERT_STRING_CONTAINS(timeoutBody, "Timeout");

        delayBlobResults = false;
        for (auto& delayed : delayedBlobResults) {
            env.Env.Send(delayed.Release());
        }
        delayedBlobResults.clear();

        UNIT_ASSERT_VALUES_EQUAL(env.GetExecutorSimpleCounter("ExecutorTxInFly"), 0);
        UNIT_ASSERT_VALUES_EQUAL(remoteResponses, 0);

        env.Env.SetObserverFunc(prevObserver);
        env.Env.SetScheduledEventsSelectorFunc(prevScheduledEventsSelector);
        env.Env.SetRegistrationObserverFunc(prevRegistrationObserver);
        env.Env.SetDispatchTimeout(prevDispatchTimeout);
    }
}

} // namespace NTabletFlatExecutor
} // namespace NKikimr

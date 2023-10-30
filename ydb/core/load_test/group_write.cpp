#include "service_actor.h"
#include "size_gen.h"
#include "interval_gen.h"
#include "quantile.h"
#include "speed.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/util/lz4_data_generator.h>

#include <google/protobuf/text_format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/system/type_name.h>
#include <util/random/fast.h>

namespace NKikimr {

class TLogWriterLoadTestActor : public TActorBootstrapped<TLogWriterLoadTestActor> {
    class TWakeupQueue {
        using TCallback = std::function<void(const TActorContext&)>;

        struct TEvent {
            TMonotonic Timestamp;
            TCallback Callback;

            TEvent(TMonotonic timestamp, TCallback callback)
                : Timestamp(timestamp)
                , Callback(std::move(callback))
            {}

            friend bool operator <(const TEvent& x, const TEvent& y) {
                return y.Timestamp < x.Timestamp;
            }
        };

        TPriorityQueue<TEvent> Events;

    public:
        void Wakeup(const TActorContext& ctx) {
            while (Events && TActivationContext::Monotonic() >= Events.top().Timestamp) {
                TCallback callback = std::move(Events.top().Callback);
                Events.pop();
                callback(ctx);
            }
        }

        TMaybe<TMonotonic> GetNextWakeupTime() const {
            return Events ? Events.top().Timestamp : TMaybe<TMonotonic>();
        }

        void Put(TMonotonic timestamp, TCallback callback, const TActorContext& /*ctx*/) {
            Events.emplace(timestamp, callback);
        }
    };

    class TQueryDispatcher {
        using TCallback = std::function<void(IEventBase*, const TActorContext&)>;

        ui64 NextCookie = 1;
        THashMap<ui64, TCallback> Callbacks;

    public:
        ui64 ObtainCookie(TCallback callback) {
            ui64 cookie = NextCookie++;
            Callbacks.emplace(cookie, std::move(callback));
            return cookie;
        }

        template<typename TEventPtr>
        void ProcessEvent(TEventPtr& ev, const TActorContext& ctx) {
            auto iter = Callbacks.find(ev->Cookie);
            Y_ABORT_UNLESS(iter != Callbacks.end(), "Cookie# %" PRIu64 " Type# %s", ev->Cookie, TypeName<TEventPtr>().data());
            iter->second(ev->Get(), ctx);
            Callbacks.erase(iter);
        }
    };

    struct TReqInfo {
        TDuration SendTime;
        TEvBlobStorage::EEv EvType;
        ui64 Size;
        NKikimrBlobStorage::EPutHandleClass PutHandleClass;
    };

    friend struct THardRateDelayManager;

    struct TRequestDelayManager {
        virtual ~TRequestDelayManager() = default;

        virtual TDuration CalculateDelayForNextRequest(TMonotonic now) = 0;
        virtual void CountResponse() = 0;

        virtual TString DumpState() const {
            return "";
        }

        virtual TDuration GetDelayForCurrentState() const = 0;
    };

    struct TRandomIntervalDelayManager : public TRequestDelayManager {
        TRandomIntervalDelayManager(const TIntervalGenerator& intervalGenerator)
            : IntervalGenerator(intervalGenerator) {
        }

        TDuration CalculateDelayForNextRequest(TMonotonic/* now*/) override {
            return IntervalGenerator.Generate();
        }

        void CountResponse() override {}

        virtual TDuration GetDelayForCurrentState() const override {
            return IntervalGenerator.Generate();
        }

        TIntervalGenerator IntervalGenerator;
    };

    struct THardRateDelayManager : public TRequestDelayManager {
        THardRateDelayManager(double requestsPerSecondAtStart, double requestsPerSecondOnFinish, TMonotonic now, const TMaybe<TDuration>& duration)
            : EpochDuration(TDuration::MilliSeconds(100))
            , RequestRateAtStart((EpochDuration / TDuration::Seconds(1)) * requestsPerSecondAtStart)
            , RequestRateOnFinish((EpochDuration / TDuration::Seconds(1)) * requestsPerSecondOnFinish)
            , CurrentEpochEnd(now)
            , PlannedForCurrentEpoch(std::max(1., CalculateRequestRate(now)))
            , LoadStart(now)
            , LoadDuration(duration) {
            CalculateDelayForNextRequest(now);
        }

        const TDuration EpochDuration;

        const double RequestRateAtStart;
        const double RequestRateOnFinish;

        double RequestsPerEpoch;
        TMonotonic CurrentEpochEnd;

        TDuration CurrentDelay = TDuration::Seconds(0);
        TMonotonic Now = TMonotonic::Max();

        double PlannedForCurrentEpoch;
        ui32 ResponsesAwaiting = 0;

        const TMonotonic LoadStart;
        const TMaybe<TDuration> LoadDuration;

        double CalculateRequestRate(TMonotonic now) {
            double ratio = LoadDuration ? (now - LoadStart) / *LoadDuration : 0;
            return ratio * (RequestRateOnFinish - RequestRateAtStart) + RequestRateAtStart;
        }

        TDuration CalculateDelayForNextRequest(TMonotonic now) override {
            Now = now;
            while (now >= CurrentEpochEnd) {
                CurrentEpochEnd += EpochDuration;
                RequestsPerEpoch = CalculateRequestRate(now); 
                PlannedForCurrentEpoch += RequestsPerEpoch;
                PlannedForCurrentEpoch += ResponsesAwaiting;
                ResponsesAwaiting = 0;
            }

            if (PlannedForCurrentEpoch < 1) {
                CurrentDelay = EpochDuration / RequestsPerEpoch * (1 - PlannedForCurrentEpoch);
            } else {
                TDuration remainingTime = CurrentEpochEnd - now;
                CurrentDelay = remainingTime / (PlannedForCurrentEpoch + 1);
            }
            ResponsesAwaiting += 1;
            PlannedForCurrentEpoch -= 1;
            return CurrentDelay;
        }

        void CountResponse() override {
            if (ResponsesAwaiting > 0) {
                ResponsesAwaiting -= 1;
            } else {
                PlannedForCurrentEpoch -= 1;
            }
        }

        TString DumpState() const override {
            TStringStream str;

            str <<  "EpochDuration# " << EpochDuration
                << " RequestRateAtStart# " << RequestRateAtStart
                << " RequestRateOnFinish# " << RequestRateOnFinish
                << " RequestsPerEpoch# " << RequestsPerEpoch
                << " CurrentEpochEnd# " << CurrentEpochEnd
                << " CurrentDelay# " << CurrentDelay
                << " PlannedForCurrentEpoch# " << PlannedForCurrentEpoch
                << " LoadStart# " << LoadStart
                << " Now# " << Now;

            return str.Str();
        }

        virtual TDuration GetDelayForCurrentState() const override {
            return CurrentDelay;
        }
    };

    class TTabletWriter {
    public:
        struct TRequestDispatchingSettings {
            TSizeGenerator SizeGen;
            std::shared_ptr<TRequestDelayManager> DelayManager;
            const ui32 MaxRequestsInFlight;
            const ui64 MaxBytesInFlight;
            const ui64 MaxTotalBytes;
        };

    private:
        using TLatencyTrackerUs = NMonitoring::TPercentileTrackerLg<5, 5, 10>;
        static_assert(TLatencyTrackerUs::TRACKER_LIMIT >= 100e6,
                "TLatencyTrackerUs must have limit grater than 100 second");

        const TVector<float> Percentiles{0.1, 0.15, 0.5, 0.9, 0.99, 0.999, 1.0};

        const TDuration ExposePeriod = TDuration::Seconds(10);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> TagCounters;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
        TWakeupQueue& WakeupQueue;
        TQueryDispatcher& QueryDispatcher;
        const ui64 TabletId;
        const ui32 Channel;
        ui32 Generation;
        ui32 GarbageCollectStep;
        ui32 WriteStep;
        ui32 Cookie;
        ui32 GroupBlockRetries;
        const ui32 GroupId;

        // Writes
        const NKikimrBlobStorage::EPutHandleClass PutHandleClass;
        TRequestDispatchingSettings WriteSettings;
        TMonotonic NextWriteTimestamp;
        ui32 WritesInFlight = 0;
        ui64 WriteBytesInFlight = 0;
        ui64 TotalBytesWritten = 0;
        THashMap<ui64, ui64> SentTimestamp;
        ui64 WriteQueryId = 0;
        bool NextWriteInQueue = false;
        TDeque<std::pair<ui64, ui64>> WritesInFlightTimestamps;
        TSpeedTracker<ui64> MegabytesPerSecondST;
        TQuantileTracker<ui64> MegabytesPerSecondQT;
        std::unique_ptr<TLatencyTrackerUs> ResponseQT;
        TQuantileTracker<ui32> WritesInFlightQT;
        TQuantileTracker<ui64> WriteBytesInFlightQT;
        TDeque<TMonotonic> IssuedWriteTimestamp;
    
        // Reads
        const NKikimrBlobStorage::EGetHandleClass GetHandleClass;
        TRequestDispatchingSettings ReadSettings;
        TMonotonic NextReadTimestamp;
        ui32 ReadsInFlight = 0;
        ui64 ReadBytesInFlight = 0;
        ui64 TotalBytesRead = 0;
        THashMap<ui64, ui64> ReadSentTimestamp;
        ui64 ReadQueryId = 0;
        bool NextReadInQueue = false;
        TSpeedTracker<ui64> ReadMegabytesPerSecondST;
        TQuantileTracker<ui64> ReadMegabytesPerSecondQT;
        std::unique_ptr<TLatencyTrackerUs> ReadResponseQT;
        TQuantileTracker<ui32> ReadsInFlightQT;
        TQuantileTracker<ui64> ReadBytesInFlightQT;

        // Collecting garbage
        TIntervalGenerator GarbageCollectIntervalGen;
    
        TDeque<TLogoBlobID> ConfirmedBlobIds;
        TIntrusivePtr<NMonitoring::TCounterForPtr> MaxInFlightLatency;
        bool IsWorkingNow = true;

        TMonotonic LastLatencyTrackerUpdate;

        TMonotonic StartTimestamp;
        TDuration ScriptedRoundDuration;
        // Incremented in every write request
        ui64 ScriptedCounter;
        // Incremented on cycle;
        ui64 ScriptedRound;
        TVector<TReqInfo> ScriptedRequests;

    public:
        TTabletWriter(ui64 tag, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                TWakeupQueue& wakeupQueue, TQueryDispatcher& queryDispatcher, ui64 tabletId, ui32 channel,
                TMaybe<ui32> generation, ui32 groupId,
                NKikimrBlobStorage::EPutHandleClass putHandleClass, const TRequestDispatchingSettings& writeSettings,
                NKikimrBlobStorage::EGetHandleClass getHandleClass, const TRequestDispatchingSettings& readSettings,
                TIntervalGenerator garbageCollectIntervalGen,
                TDuration scriptedRoundDuration, TVector<TReqInfo>&& scriptedRequests)
            : TagCounters(counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag)))
            , Counters(TagCounters->GetSubgroup("channel", Sprintf("%" PRIu32, channel)))
            , WakeupQueue(wakeupQueue)
            , QueryDispatcher(queryDispatcher)
            , TabletId(tabletId)
            , Channel(channel)
            , Generation(generation ? *generation : 0)
            , GarbageCollectStep(1)
            , WriteStep(3)
            , Cookie(1)
            , GroupBlockRetries(3)
            , GroupId(groupId)
            , PutHandleClass(putHandleClass)
            , WriteSettings(writeSettings)
            , MegabytesPerSecondST(TDuration::Seconds(3)) // average speed at last 3 seconds
            , MegabytesPerSecondQT(ExposePeriod, Counters->GetSubgroup("metric", "writeSpeed"),
                    "bytesPerSecond", Percentiles)
            , ResponseQT(new TLatencyTrackerUs())
            , WritesInFlightQT(ExposePeriod, Counters->GetSubgroup("metric", "writesInFlight"),
                    "items", Percentiles)
            , WriteBytesInFlightQT(ExposePeriod, Counters->GetSubgroup("metric", "writeBytesInFlight"),
                    "bytes", Percentiles)
            , GetHandleClass(getHandleClass)
            , ReadSettings(readSettings)
            , ReadMegabytesPerSecondST(TDuration::Seconds(3))
            , ReadMegabytesPerSecondQT(ExposePeriod, Counters->GetSubgroup("metric", "readSpeed"),
                    "bytesPerSecond", Percentiles)
            , ReadResponseQT(new TLatencyTrackerUs())
            , ReadsInFlightQT(ExposePeriod, Counters->GetSubgroup("metric", "readsInFlight"),
                    "items", Percentiles)
            , ReadBytesInFlightQT(ExposePeriod, Counters->GetSubgroup("metric", "readBytesInFlight"),
                    "bytes", Percentiles)
            , GarbageCollectIntervalGen(garbageCollectIntervalGen)
            , ScriptedRoundDuration(scriptedRoundDuration)
            , ScriptedCounter(0)
            , ScriptedRound(0)
            , ScriptedRequests(std::move(scriptedRequests))
        {
            *Counters->GetCounter("tabletId") = tabletId;
            const auto& percCounters = Counters->GetSubgroup("sensor", "microseconds");
            MaxInFlightLatency = percCounters->GetCounter("MaxInFlightLatency");
            ResponseQT->Initialize(percCounters->GetSubgroup("metric", "writeResponse"), Percentiles);
            ReadResponseQT->Initialize(percCounters->GetSubgroup("metric", "readResponse"), Percentiles);
        }

        TString PrintMe() {
            return TStringBuilder() << "TabletId# " << TabletId << " Generation# " << Generation;
        }

        ~TTabletWriter() {
            TagCounters->ResetCounters();
        }

        template<typename T>
        bool CheckStatus(const TActorContext& ctx, T *ev, const TVector<NKikimrProto::EReplyStatus>& goodStatuses) {
            if (goodStatuses.empty() || Count(goodStatuses, ev->Status)) {
                return true;
            } else {
                LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved not OK, msg# "
                        << ev->ToString());
                IsWorkingNow = false;
                ctx.Send(ctx.SelfID, new TEvStopTest());
                return false;
            }
        }

        // Issue TEvDiscover
        void Bootstrap(const TActorContext& ctx) {
            NextWriteTimestamp = TActivationContext::Monotonic();
            auto ev = std::make_unique<TEvBlobStorage::TEvDiscover>(TabletId, Generation, false, true, TInstant::Max(), 0, true);
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " is bootstrapped, going to send "
                    << ev->ToString());
            auto callback = [this] (IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvDiscoverResult *>(event);
                Y_ABORT_UNLESS(res);
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK, NKikimrProto::EReplyStatus::NODATA})) {
                    return;
                }
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());
                Generation = res->BlockedGeneration + 1;
                IssueTEvBlock(ctx);
            };
            SendToBSProxy(ctx, GroupId, ev.release(), QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void IssueTEvBlock(const TActorContext& ctx) {
            auto ev = std::make_unique<TEvBlobStorage::TEvBlock>(TabletId, Generation, TInstant::Max());
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " going to send " << ev->ToString());
            auto callback = [this] (IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvBlockResult *>(event);
                Y_ABORT_UNLESS(res);
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK, NKikimrProto::EReplyStatus::ALREADY})) {
                    return;
                } else if (res->Status == NKikimrProto::EReplyStatus::ALREADY && GroupBlockRetries-- > 0) {
                    LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());
                    IssueTEvBlock(ctx);
                    return;
                }

                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());
                // For work use next generation after blocked
                ++Generation;
                IssueLastBlob(ctx);
            };
            SendToBSProxy(ctx, GroupId, ev.release(), QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void IssueLastBlob(const TActorContext& ctx) {
            const ui32 size = 1;
            const ui32 lastStep = Max<ui32>();
            const TLogoBlobID id(TabletId, Generation, lastStep, Channel, size, 0);
            const TSharedData buffer = GenerateBuffer<TSharedData>(id);
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, buffer, TInstant::Max(), PutHandleClass);

            auto callback = [this] (IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvPutResult *>(event);
                Y_ABORT_UNLESS(res);
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK})) {
                    return;
                }

                IssueTEvCollectGarbage(ctx);
            };

            SendToBSProxy(ctx, GroupId, ev.release(), QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void IssueTEvCollectGarbage(const TActorContext& ctx) {
            auto ev = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(TabletId, Generation, GarbageCollectStep,
                    Channel, Generation, 0, TInstant::Max());
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " going to send " << ev->ToString());
            ++GarbageCollectStep;
            auto callback = [this] (IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult *>(event);
                Y_ABORT_UNLESS(res);
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK})) {
                    return;
                }
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());
                StartWorking(ctx);
            };

            SendToBSProxy(ctx, GroupId, ev.Release(), QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void StartWorking(const TActorContext& ctx) {
            StartTimestamp = TActivationContext::Monotonic();
            InitializeTrackers(StartTimestamp);
            IssueWriteIfPossible(ctx);
            ScheduleGarbageCollect(ctx);
            ExposeCounters(ctx);
        }

        void StopWorking(const TActorContext& ctx) {
            auto ev = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(TabletId, Generation, GarbageCollectStep,
                    Channel, Generation, Max<ui32>(), TInstant::Max());
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " end working, going to send " << ev->ToString());
            ++GarbageCollectStep;
            auto callback = [this](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult *>(event);
                Y_ABORT_UNLESS(res);
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK})) {
                    return;
                }
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());

                if (IsWorkingNow) {
                    ctx.Send(ctx.SelfID, new TEvStopTest());
                }
            };
            SendToBSProxy(ctx, GroupId, ev.Release(), QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void InitializeTrackers(TMonotonic now) {
            LastLatencyTrackerUpdate = now;

            MegabytesPerSecondST.Add(now, 0);
            MegabytesPerSecondQT.Add(now, 0);

            ReadMegabytesPerSecondST.Add(now, 0);
            ReadMegabytesPerSecondQT.Add(now, 0);

            WritesInFlightQT.Add(now, 0);
            WriteBytesInFlightQT.Add(now, 0);
            ReadsInFlightQT.Add(now, 0);
            ReadBytesInFlightQT.Add(now, 0);
        }

        void UpdateQuantile(TMonotonic now) {
            ui64 speed;
            MegabytesPerSecondST.Add(now, TotalBytesWritten);
            if (MegabytesPerSecondST.CalculateSpeed(&speed)) {
                MegabytesPerSecondQT.Add(now, speed);
            }
            ReadMegabytesPerSecondST.Add(now, TotalBytesRead);
            if (ReadMegabytesPerSecondST.CalculateSpeed(&speed)) {
                ReadMegabytesPerSecondQT.Add(now, speed);
            }
            WritesInFlightQT.Add(now, WritesInFlight);
            WriteBytesInFlightQT.Add(now, WriteBytesInFlight);
            ReadsInFlightQT.Add(now, ReadsInFlight);
            ReadBytesInFlightQT.Add(now, ReadBytesInFlight);
            if (now > LastLatencyTrackerUpdate + TDuration::Seconds(1)) {
                LastLatencyTrackerUpdate = now;
                ResponseQT->Update();
                ReadResponseQT->Update();
                if (WritesInFlightTimestamps) {
                    const auto& maxLatency = CyclesToDuration(GetCycleCountFast() - WritesInFlightTimestamps.front().second);
                    *MaxInFlightLatency = maxLatency.MicroSeconds();
                }
            }
        }

        static TString PercentileName(int value) {
            return Sprintf("%d.%04d", value / 10000, value % 10000);
        }

        void ExposeCounters(const TActorContext &ctx) {
            MegabytesPerSecondQT.CalculateQuantiles();
            ReadMegabytesPerSecondQT.CalculateQuantiles();

            WritesInFlightQT.CalculateQuantiles();
            WriteBytesInFlightQT.CalculateQuantiles();
            ReadsInFlightQT.CalculateQuantiles();
            ReadBytesInFlightQT.CalculateQuantiles();

            using namespace std::placeholders;
            WakeupQueue.Put(TActivationContext::Monotonic() + ExposePeriod,
                    std::bind(&TTabletWriter::ExposeCounters, this, _1), ctx);
        }

        void DumpState(IOutputStream& str, bool finalResult) {
#define DUMP_PARAM_IMPL(NAME, INCLUDE_IN_FINAL) \
            if (!finalResult || INCLUDE_IN_FINAL) { \
                TABLER() { \
                    TABLED() { str << #NAME; } \
                    TABLED() { str << NAME; } \
                } \
            }
#define DUMP_PARAM(NAME) DUMP_PARAM_IMPL(NAME, false)
#define DUMP_PARAM_FINAL(NAME) DUMP_PARAM_IMPL(NAME, true)

            HTML(str) {
                TDuration EarliestTimestamp = TDuration::Zero();
                const ui64 nowCycles = GetCycleCountFast();
                for (const auto& [writeId, issued] : WritesInFlightTimestamps) {
                    EarliestTimestamp = Max(EarliestTimestamp, CyclesToDuration(nowCycles - issued));
                }

                DUMP_PARAM(TabletId)
                DUMP_PARAM(Channel)
                DUMP_PARAM(Generation)
                DUMP_PARAM(GarbageCollectStep)
                DUMP_PARAM(WriteStep)
                DUMP_PARAM(Cookie)
                DUMP_PARAM(GroupId)
                DUMP_PARAM(PutHandleClass)
                DUMP_PARAM(GetHandleClass)
                if (EarliestTimestamp != TDuration::Zero()) {
                    DUMP_PARAM(EarliestTimestamp)
                }
                DUMP_PARAM(NextWriteTimestamp)
                DUMP_PARAM(WritesInFlight)
                DUMP_PARAM(WriteBytesInFlight)
                DUMP_PARAM_FINAL(WriteSettings.MaxRequestsInFlight)
                DUMP_PARAM_FINAL(WriteSettings.MaxBytesInFlight)
                DUMP_PARAM_FINAL(TotalBytesWritten)
                DUMP_PARAM_FINAL(WriteSettings.MaxTotalBytes)
                DUMP_PARAM_FINAL(TotalBytesRead)
                DUMP_PARAM(NextReadTimestamp)
                DUMP_PARAM(ReadsInFlight)
                DUMP_PARAM(ReadBytesInFlight)
                DUMP_PARAM_FINAL(ReadSettings.MaxRequestsInFlight)
                DUMP_PARAM_FINAL(ReadSettings.MaxBytesInFlight)
                DUMP_PARAM(ConfirmedBlobIds.size())

                static constexpr size_t count = 5;
                std::array<size_t, count> nums{{9000, 9900, 9990, 9999, 10000}};
                std::array<ui64, count> qSpeed;
                MegabytesPerSecondQT.CalculateQuantiles(count, nums.data(), 10000, qSpeed.data());

                TABLER() {
                    TABLED() { str << "Writes per second"; }
                    if (IssuedWriteTimestamp.size() > 1) {
                        const double rps = IssuedWriteTimestamp.size() /
                            (IssuedWriteTimestamp.back() - IssuedWriteTimestamp.front()).SecondsFloat();
                        TABLED() { str << Sprintf("%.2lf", rps); }
                    } else {
                        TABLED() { str << "no writes"; }
                    }
                }

                for (size_t i = 0; i < count; ++i) {
                    TABLER() {
                        TABLED() { str << Sprintf("WriteSpeed@ %d.%02d%%", int(nums[i] / 100), int(nums[i] % 100)); }
                        ui64 x = qSpeed[i] * 100 / 1048576;
                        TABLED() { str << Sprintf("%" PRIu64 ".%02d MB/s", x / 100, int(x % 100)); }
                    }
                }

                ReadMegabytesPerSecondQT.CalculateQuantiles(count, nums.data(), 10000, qSpeed.data());

                for (size_t i = 0; i < count; ++i) {
                    TABLER() {
                        TABLED() { str << Sprintf("ReadSpeed@ %d.%02d%%", int(nums[i] / 100), int(nums[i] % 100)); }
                        ui64 x = qSpeed[i] * 100 / 1048576;
                        TABLED() { str << Sprintf("%" PRIu64 ".%02d MB/s", x / 100, int(x % 100)); }
                    }
                }
            }
#undef DUMP_PARAM_IMPL
#undef DUMP_PARAM
#undef DUMP_PARAM_FINAL
        }

    private:
        void UpdateNextWakeups(const TActorContext& ctx, const TMonotonic& now) {
            if (now < NextWriteTimestamp && !NextWriteInQueue) {
                using namespace std::placeholders;
                WakeupQueue.Put(NextWriteTimestamp, std::bind(&TTabletWriter::IssueWriteIfPossible, this, _1), ctx);
                NextWriteInQueue = true;
            }

            if (now < NextReadTimestamp && !NextReadInQueue) {
                using namespace std::placeholders;
                WakeupQueue.Put(NextReadTimestamp, std::bind(&TTabletWriter::IssueReadIfPossible, this, _1), ctx);
                NextReadInQueue = true;
            }
        }

        void IssueWriteIfPossible(const TActorContext& ctx) {
            const TMonotonic now = TActivationContext::Monotonic();
            while ((WritesInFlight < WriteSettings.MaxRequestsInFlight || !WriteSettings.MaxRequestsInFlight) &&
                    (WriteBytesInFlight < WriteSettings.MaxBytesInFlight || !WriteSettings.MaxBytesInFlight) &&
                    (TotalBytesWritten + WriteBytesInFlight < WriteSettings.MaxTotalBytes || !WriteSettings.MaxTotalBytes) &&
                    now >= NextWriteTimestamp &&
                    (!ScriptedRequests || ScriptedRequests[ScriptedCounter].EvType == TEvBlobStorage::EvPut)) {
                IssueWriteRequest(ctx);
            }

            if (ScriptedRequests) {
                UpdateNextTimestemps(false);
            }
            UpdateNextWakeups(ctx, now);
        }

        void IssueWriteRequest(const TActorContext& ctx) {
            ui32 size;
            NKikimrBlobStorage::EPutHandleClass putHandleClass;
            if (ScriptedRequests) {
                const auto& req = ScriptedRequests[ScriptedCounter];
                size = req.Size;
                putHandleClass = req.PutHandleClass;
            } else {
                size = WriteSettings.SizeGen.Generate();
                putHandleClass = PutHandleClass;
            }
            const TLogoBlobID id(TabletId, Generation, WriteStep, Channel, size, Cookie);
            const TSharedData buffer = GenerateBuffer<TSharedData>(id);
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, buffer, TInstant::Max(), putHandleClass);
            const ui64 writeQueryId = ++WriteQueryId;

            auto writeCallback = [this, writeQueryId](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvPutResult *>(event);
                Y_ABORT_UNLESS(res);

                WriteSettings.DelayManager->CountResponse();
                if (!CheckStatus(ctx, res, {})) {
                    return;
                }

                const TLogoBlobID& id = res->Id;
                const ui32 size = id.BlobSize();

                // this blob has been confirmed -- update set
                if (!ConfirmedBlobIds || id > ConfirmedBlobIds.back()) {
                    ConfirmedBlobIds.push_back(id);
                } else {
                    // most likely inserted somewhere near the end
                    ConfirmedBlobIds.insert(std::lower_bound(ConfirmedBlobIds.begin(), ConfirmedBlobIds.end(), id), id);
                }

                Y_ABORT_UNLESS(WritesInFlight >= 1 && WriteBytesInFlight >= size);
                --WritesInFlight;
                WriteBytesInFlight -= size;

                TotalBytesWritten += size;

                auto it = SentTimestamp.find(writeQueryId);
                const auto sendCycles = it->second;
                Y_ABORT_UNLESS(it != SentTimestamp.end());
                const TDuration response = CyclesToDuration(GetCycleCountFast() - sendCycles);
                SentTimestamp.erase(it);

                // It's very likely that "writeQueryId" will be found at the start
                auto itInFlight = Find(WritesInFlightTimestamps, std::make_pair(writeQueryId, sendCycles));
                Y_ABORT_UNLESS(itInFlight != WritesInFlightTimestamps.end());
                WritesInFlightTimestamps.erase(itInFlight);

                ResponseQT->Increment(response.MicroSeconds());
                IssueWriteIfPossible(ctx);

                if (ConfirmedBlobIds.size() == 1) {
                    if (NextReadTimestamp == TMonotonic()) {
                        NextReadTimestamp = TActivationContext::Monotonic();
                    }
                    IssueReadIfPossible(ctx);
                }
            };
            SendToBSProxy(ctx, GroupId, ev.release(), QueryDispatcher.ObtainCookie(std::move(writeCallback)));
            const auto nowCycles = GetCycleCountFast();
            WritesInFlightTimestamps.emplace_back(writeQueryId, nowCycles);
            SentTimestamp.emplace(writeQueryId, nowCycles);
            IssuedWriteTimestamp.push_back(TActivationContext::Monotonic());
            while (IssuedWriteTimestamp.size() > 10000 || IssuedWriteTimestamp.back() - IssuedWriteTimestamp.front() >= TDuration::Seconds(5)) {
                IssuedWriteTimestamp.pop_front();
            }

            ++Cookie;

            ++WritesInFlight;
            WriteBytesInFlight += size;

            if (ScriptedRequests) {
                UpdateNextTimestemps(true);
            } else {
                // calculate time of next write request
                NextWriteTimestamp += WriteSettings.DelayManager->CalculateDelayForNextRequest(TActivationContext::Monotonic());
            }

            NextWriteInQueue = false;
        }

        // scripted requests
        void UpdateNextTimestemps(bool incrementCounter) {
            Y_ABORT_UNLESS(ScriptedRequests);

            if (incrementCounter) {
                if (++ScriptedCounter == ScriptedRequests.size()) {
                    ScriptedCounter = 0;
                    ++ScriptedRound;
                }
            }
            TDuration duration = ScriptedRequests[ScriptedCounter].SendTime;
            duration += ScriptedRound * ScriptedRoundDuration;

            switch (ScriptedRequests[ScriptedCounter].EvType) {
            case TEvBlobStorage::EvGet:
                NextReadTimestamp = StartTimestamp + duration;
                break;
            case TEvBlobStorage::EvPut:
                NextWriteTimestamp = StartTimestamp + duration;
                break;
            default:
                Y_FAIL_S("Unsupported request type# " << (ui64)ScriptedRequests[ScriptedCounter].EvType);
                break;
            }
        }

        void ScheduleGarbageCollect(const TActorContext& ctx) {
            TDuration duration = GarbageCollectIntervalGen.Generate();
            if (duration != TDuration()) {
                using namespace std::placeholders;
                WakeupQueue.Put(TActivationContext::Monotonic() + duration,
                        std::bind(&TTabletWriter::IssueGarbageCollectRequest, this, _1), ctx);
            }
        }

        void IssueGarbageCollectRequest(const TActorContext& ctx) {
            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(TabletId, Generation, GarbageCollectStep, Channel,
                    true, Generation, GarbageCollectStep, nullptr, nullptr, TInstant::Max(), false);
            auto callback = [](IEventBase *event, const TActorContext& /*ctx*/) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult *>(event);
                Y_ABORT_UNLESS(res);
            };
            SendToBSProxy(ctx, GroupId, ev.release(), QueryDispatcher.ObtainCookie(std::move(callback)));

            // just as we have sent this request, we have to trim all confirmed blobs which are going to be deleted
            const auto it = std::lower_bound(ConfirmedBlobIds.begin(), ConfirmedBlobIds.end(),
                TLogoBlobID(TabletId, Generation, GarbageCollectStep, Channel, TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId));
            ConfirmedBlobIds.erase(ConfirmedBlobIds.begin(), it);

            ++GarbageCollectStep;
            ++WriteStep;
            Cookie = 1;
            ScheduleGarbageCollect(ctx);
        }

        void IssueReadIfPossible(const TActorContext& ctx) {
            const TMonotonic now = TActivationContext::Monotonic();

            while (ReadsInFlight < ReadSettings.MaxRequestsInFlight &&
                    (ReadBytesInFlight < ReadSettings.MaxBytesInFlight || !ReadSettings.MaxBytesInFlight) &&
                    now >= NextReadTimestamp &&
                    ConfirmedBlobIds &&
                    (!ScriptedRequests || ScriptedRequests[ScriptedCounter].EvType == TEvBlobStorage::EvGet)) {
                IssueReadRequest(ctx);
            }

            if (ScriptedRequests) {
                UpdateNextTimestemps(false);
            }
            UpdateNextWakeups(ctx, now);
        }

        void IssueReadRequest(const TActorContext& ctx) {
            auto iter = ConfirmedBlobIds.begin();
            std::advance(iter, RandomNumber(ConfirmedBlobIds.size()));
            const TLogoBlobID &id = *iter;

            ui32 size;
            if (ScriptedRequests) {
                const auto& req = ScriptedRequests[ScriptedCounter];
                size = req.Size ? req.Size : id.BlobSize();
            } else {
                size = ReadSettings.SizeGen.Generate();
            }
            size = Min(size, id.BlobSize());

            const ui32 offset = RandomNumber<ui32>(id.BlobSize() - size + 1);
            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(id, offset, size, TInstant::Max(),
                GetHandleClass);
            const ui64 readQueryId = ++ReadQueryId;

            auto readCallback = [this, size, readQueryId](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvGetResult*>(event);
                Y_ABORT_UNLESS(res);

                ReadSettings.DelayManager->CountResponse();
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK})) {
                    return;
                }

                Y_ABORT_UNLESS(ReadsInFlight >= 1 && ReadBytesInFlight >= size);
                --ReadsInFlight;
                ReadBytesInFlight -= size;
                TotalBytesRead += size;

                auto it = ReadSentTimestamp.find(readQueryId);
                Y_ABORT_UNLESS(it != ReadSentTimestamp.end());
                const TDuration response = CyclesToDuration(GetCycleCountFast() - it->second);
                ReadSentTimestamp.erase(it);

                ReadResponseQT->Increment(response.MicroSeconds());
                IssueReadIfPossible(ctx);
            };

            SendToBSProxy(ctx, GroupId, ev.release(), QueryDispatcher.ObtainCookie(std::move(readCallback)));
            ReadSentTimestamp.emplace(readQueryId, GetCycleCountFast());

            ++ReadsInFlight;
            ReadBytesInFlight += size;

            // calculate time of next read request
            if (ScriptedRequests) {
                UpdateNextTimestemps(true);
            } else {
                NextReadTimestamp += ReadSettings.DelayManager->CalculateDelayForNextRequest(TActivationContext::Monotonic());
            }
            NextReadInQueue = false;
        }

        template <class ResultContainer = TString>
        static ResultContainer GenerateBuffer(const TLogoBlobID& id) {
            return GenDataForLZ4<ResultContainer>(id.BlobSize());
        }
    };

    TString ConfingString;
    const ui64 Tag;
    const TActorId Parent;

    TMaybe<TDuration> TestDuration;
    TMonotonic TestStartTime;
    bool EarlyStop = false;

    TVector<std::unique_ptr<TTabletWriter>> TabletWriters;

    TWakeupQueue WakeupQueue;
    TDeque<TMonotonic> WakeupScheduledAt;

    TQueryDispatcher QueryDispatcher;

    ::NMonitoring::TDynamicCounters::TCounterPtr ScheduleCounter;

    ui32 TestStoppedReceived = 0;

    ui64 WakeupsScheduled = 0;

    TMonotonic LastScheduleTime = TMonotonic::Max();
    TMonotonic LastWakeupTime = TMonotonic::Max();

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_ACTOR;
    }

    TLogWriterLoadTestActor(const NKikimr::TEvLoadTestRequest::TStorageLoad& cmd, const TActorId& parent,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
        : Tag(tag)
        , Parent(parent)
        , ScheduleCounter(counters->GetSubgroup("subsystem", "scheduler")->GetCounter("ScheduleCounter", true))
    {
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);
        if (cmd.HasDurationSeconds()) {
            TestDuration = TDuration::Seconds(cmd.GetDurationSeconds());
        }
        for (const auto& profile : cmd.GetTablets()) {
            if (!profile.TabletsSize()) {
                ythrow TLoadActorException() << "TPerTabletProfile.Tablets must have at least one item";
            }
            if (!profile.WriteSizesSize()) {
                ythrow TLoadActorException() << "TPerTabletProfile.Sizes must have at least one item";
            }
            if (!profile.WriteIntervalsSize() && !profile.HasWriteHardRateDispatcher()) {
                ythrow TLoadActorException() << "Either TPerTabletProfile.WriteHardRateDispatcher or "
                        "at least one item in TPerTabletProfile.WriteIntervals must be specified";
            }

            if (!profile.HasPutHandleClass()) {
                ythrow TLoadActorException() << "missing mandatory TPerTabletProfile.PutHandleClass";
            }

            NKikimrBlobStorage::EPutHandleClass putHandleClass = profile.GetPutHandleClass();

            // object may be shared across multiple writers
            std::shared_ptr<TRequestDelayManager> writeDelayManager;
            if (profile.HasWriteHardRateDispatcher()) {
                auto now = TActivationContext::Monotonic();
                const auto& dispatcherSettings = profile.GetWriteHardRateDispatcher();
                double atStart = dispatcherSettings.GetRequestsPerSecondAtStart();
                double onFinish = dispatcherSettings.GetRequestsPerSecondOnFinish();
                writeDelayManager = std::make_shared<THardRateDelayManager>(atStart, onFinish, now, TestDuration);
            } else {
                writeDelayManager = std::make_shared<TRandomIntervalDelayManager>(TIntervalGenerator(profile.GetWriteIntervals()));
            }

            TTabletWriter::TRequestDispatchingSettings writeSettings{
                .SizeGen = TSizeGenerator(profile.GetWriteSizes()),
                .DelayManager = std::move(writeDelayManager),
                .MaxRequestsInFlight = profile.GetMaxInFlightWriteRequests(),
                .MaxBytesInFlight = profile.GetMaxInFlightWriteBytes(),
                .MaxTotalBytes = profile.GetMaxTotalBytesWritten(),
            };

            NKikimrBlobStorage::EGetHandleClass getHandleClass = NKikimrBlobStorage::EGetHandleClass::FastRead;
            if (profile.HasGetHandleClass()) {
                getHandleClass = profile.GetGetHandleClass();
            }

            std::shared_ptr<TRequestDelayManager> readDelayManager;
            if (profile.HasReadHardRateDispatcher()) {
                auto now = TActivationContext::Monotonic();
                const auto& dispatcherSettings = profile.GetReadHardRateDispatcher();
                double atStart = dispatcherSettings.GetRequestsPerSecondAtStart();
                double onFinish = dispatcherSettings.GetRequestsPerSecondOnFinish();
                readDelayManager = std::make_shared<THardRateDelayManager>(atStart, onFinish, now, TestDuration);
            } else {
                readDelayManager = std::make_shared<TRandomIntervalDelayManager>(TIntervalGenerator(profile.GetReadIntervals()));
            }

            TTabletWriter::TRequestDispatchingSettings readSettings{
                .SizeGen = TSizeGenerator(profile.GetReadSizes()),
                .DelayManager = std::move(readDelayManager),
                .MaxRequestsInFlight = profile.GetMaxInFlightReadRequests(),
                .MaxBytesInFlight = profile.GetMaxInFlightReadBytes(),
                .MaxTotalBytes = ::Max<ui64>(),
            };

            TIntervalGenerator garbageCollectIntervalGen(profile.GetFlushIntervals());

            for (const auto& tablet : profile.GetTablets()) {
                auto scriptedRoundDuration = TDuration::MicroSeconds(tablet.GetScriptedCycleDurationSec() * 1e6);
                TVector<TReqInfo> scriptedRequests;
                for (const auto& req : tablet.GetRequests()) {
                    scriptedRequests.push_back(TReqInfo{
                            TDuration::Seconds(req.GetSendTime()),
                            static_cast<TEvBlobStorage::EEv>(req.GetType()),
                            req.GetSize(),
                            req.HasPutHandleClass() ? req.GetPutHandleClass() : NKikimrBlobStorage::EPutHandleClass::UserData
                            });
                }

                if (!tablet.HasTabletId() || !tablet.HasChannel() || !tablet.HasGroupId()) {
                    ythrow TLoadActorException() << "TTabletInfo.{TabletId,Channel,GroupId} fields are mandatory";
                }
                TabletWriters.emplace_back(std::make_unique<TTabletWriter>(Tag, counters, WakeupQueue, QueryDispatcher, tablet.GetTabletId(),
                    tablet.GetChannel(), tablet.HasGeneration() ?  TMaybe<ui32>(tablet.GetGeneration()) : TMaybe<ui32>(),
                    tablet.GetGroupId(), putHandleClass, writeSettings,
                    getHandleClass, readSettings,
                    garbageCollectIntervalGen,
                    scriptedRoundDuration, std::move(scriptedRequests)));
            }
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TLogWriterLoadTestActor::StateFunc);
        EarlyStop = false;
        TestStartTime = TActivationContext::Monotonic();
        if (TestDuration) {
            ctx.Schedule(*TestDuration, new TEvents::TEvPoisonPill());
        }
        for (auto& writer : TabletWriters) {
            writer->Bootstrap(ctx);
        }
        UpdateWakeupQueue(ctx);
        HandleUpdateQuantile(ctx);
    }

    void HandlePoison(const TActorContext& ctx) {
        if (TestDuration.Defined()) {
            EarlyStop = TActivationContext::Monotonic() - TestStartTime < TestDuration;
        }
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Load tablet recieved PoisonPill, going to die");
        for (auto& writer : TabletWriters) {
            writer->StopWorking(ctx); // Sends TEvStopTest then all garbage is collected
        }
    }

    void HandleStopTest(const TActorContext& ctx) {
        ++TestStoppedReceived;
        if (TestStoppedReceived == TabletWriters.size()) {
            DeathReport(ctx);
        }
    }

    void DeathReport(const TActorContext& ctx) {
        TIntrusivePtr<TEvLoad::TLoadReport> report = nullptr;
        TString errorReason;
        if (EarlyStop) {
            errorReason = "Abort, stop signal received";
        } else {
            report.Reset(new TEvLoad::TLoadReport());
            if (TestDuration.Defined()) {
                report->Duration = TestDuration.GetRef();
            }
            errorReason = "HandleStopTest";
        }

        auto* finishEv = new TEvLoad::TEvLoadTestFinished(Tag, report, errorReason);
        finishEv->LastHtmlPage = RenderHTML(true);
        ctx.Send(Parent, finishEv);
        Die(ctx);
    }

    void HandleUpdateQuantile(const TActorContext& ctx) {
        TMonotonic now = TActivationContext::Monotonic();
        for (auto& writer : TabletWriters) {
            writer->UpdateQuantile(now);
        }
        ctx.Schedule(TDuration::MilliSeconds(5), new TEvUpdateQuantile);
    }

    void HandleWakeup(const TActorContext& ctx) {
        --WakeupsScheduled;
        UpdateWakeupQueue(ctx);
    }

    void UpdateWakeupQueue(const TActorContext& ctx) {
        // erase all scheduled items before this time point, including it
        WakeupScheduledAt.erase(WakeupScheduledAt.begin(), std::upper_bound(WakeupScheduledAt.begin(),
            WakeupScheduledAt.end(), TActivationContext::Monotonic()));

        if (WakeupsScheduled < WakeupScheduledAt.size()) {
            // desynchronization with scheduler time provider occured
            WakeupScheduledAt.erase(WakeupScheduledAt.begin(), WakeupScheduledAt.begin() + WakeupScheduledAt.size() - WakeupsScheduled);
        }

        WakeupQueue.Wakeup(ctx);
        ScheduleWakeup(ctx);
    }

    // schedule next wakeup event according to wakeup queue; should be called in any event handler that can potentially
    // touch wakeup queue
    void ScheduleWakeup(const TActorContext& ctx) {
        TMaybe<TMonotonic> nextWakeupTime = WakeupQueue.GetNextWakeupTime();
        const TMonotonic scheduledWakeupTime = WakeupScheduledAt ? WakeupScheduledAt.front() : TMonotonic::Max();
        TMonotonic now = TActivationContext::Monotonic();
        LastWakeupTime = now;

        auto scheduleWakeup = [&] (TDuration delay) {
            WakeupScheduledAt.push_front(now + delay);
            LastScheduleTime = now;
            ++WakeupsScheduled;
            ctx.Schedule(delay, new TEvents::TEvWakeup);
            ++*ScheduleCounter;
        };

        if (nextWakeupTime && *nextWakeupTime < scheduledWakeupTime && *nextWakeupTime > now) {
            scheduleWakeup(*nextWakeupTime - now);
        } else if (WakeupsScheduled == 0 || WakeupScheduledAt.empty()) {
            scheduleWakeup(TDuration::MilliSeconds(10));
        }
    }

    template<typename TPtr>
    void HandleDispatcher(TPtr& ev, const TActorContext& ctx) {
        QueryDispatcher.ProcessEvent(ev, ctx);
        UpdateWakeupQueue(ctx);
    }

    TString RenderHTML(bool finalResult) {
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {
                            str << "Parameter";
                        }
                        TABLEH() {
                            str << "Value";
                        }
                    }
                }
                TABLEBODY() {
                    TABLER() {
                        TABLED() {
                            str << "Passed/Total, sec";
                        }
                        TABLED() {
                            str << (TActivationContext::Monotonic() - TestStartTime).Seconds() << " / ";
                            if (TestDuration.Defined()) {
                                str << TestDuration->Seconds();
                            } else {
                                str << "-";
                            }
                        }
                    }
                    for (auto& writer : TabletWriters) {
                        TABLER() {
                            str << "<td colspan=\"2\">" << "<b>Tablet</b>" << "</td>";
                        }
                        writer->DumpState(str, finalResult);
                    }
                }
            }
            COLLAPSED_BUTTON_CONTENT(Sprintf("configProtobuf%" PRIu64, Tag), "Config") {
                str << "<pre>" << ConfingString << "</pre>";
            }
        }
        return str.Str();
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TString html = RenderHTML(false);
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(html, ev->Get()->SubRequestId));
    }

    void Handle(TEvents::TEvUndelivered::TPtr ev, const TActorContext& /*ctx*/) {
        Y_ABORT("TEvUndelivered# 0x%08" PRIx32 " ActorId# %s", ev->Get()->SourceType, ev->Sender.ToString().data());
    }

    STRICT_STFUNC(StateFunc,
        CFunc(EvStopTest, HandleStopTest);
        CFunc(EvUpdateQuantile, HandleUpdateQuantile);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
        HFunc(TEvBlobStorage::TEvDiscoverResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvBlockResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvPutResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvGetResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleDispatcher);
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(TEvents::TEvUndelivered, Handle);
    )
};

IActor *CreateWriterLoadTest(const NKikimr::TEvLoadTestRequest::TStorageLoad& cmd, const TActorId& parent,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag) {
    return new TLogWriterLoadTestActor(cmd, parent, std::move(counters), tag);
}

} // NKikimr

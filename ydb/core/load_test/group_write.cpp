#include "service_actor.h"
#include "size_gen.h"
#include "interval_gen.h"
#include "quantile.h"
#include "speed.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <ydb/core/jaeger_tracing/throttler.h>

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

    struct TInFlightTracker {
    public:
        TInFlightTracker(ui32 maxRequestsInFlight = 0, ui64 maxBytesInFlight = 0)
            : MaxRequestsInFlight(maxRequestsInFlight)
            , MaxBytesInFlight(maxBytesInFlight)
            , RequestsInFlight(0)
            , BytesInFlight(0)
        {}

        bool LimitReached() const {
            return (MaxRequestsInFlight && RequestsInFlight >= MaxRequestsInFlight) ||
                    (MaxBytesInFlight && BytesInFlight >= MaxBytesInFlight);
        }

        void Request(ui64 size) {
            BytesInFlight += size;
            ++RequestsInFlight;
        }

        void Response(ui64 size) {
            Y_DEBUG_ABORT_UNLESS(BytesInFlight >= size && RequestsInFlight > 0);
            BytesInFlight -= size;
            --RequestsInFlight;
        }
    
        TString ToString()  const{
            return TStringBuilder() << "{"
                << " Requests# " << RequestsInFlight << "/" << MaxRequestsInFlight
                << " Bytes# " << BytesInFlight << "/" << MaxBytesInFlight
                << " }";
        }

        ui32 MaxRequestsInFlight;
        ui64 MaxBytesInFlight;
        ui32 RequestsInFlight;
        ui64 BytesInFlight;
    };

    class TInitialAllocation {
    public:
        using TProtoSettings = NKikimr::TEvLoadTestRequest::TStorageLoad::TInitialBlobAllocation;

        TInitialAllocation() = default;

        TInitialAllocation(const TProtoSettings& proto)
            : SizeGenerator(proto.GetBlobSizes())
            , SizeToWrite(proto.GetTotalSize())
            , BlobsToWrite(proto.GetBlobsNumber())
            , InFlightTracker(proto.GetMaxWritesInFlight(), proto.GetMaxWriteBytesInFlight())
        {
            if (proto.HasPutHandleClass()) {
                PutHandleClass = proto.GetPutHandleClass();
            }
        }

    public:
        bool ConfirmedSize() {
            return ConfirmedBlobs.size();
        }

        const TLogoBlobID& operator[](ui32 idx) const {
            return ConfirmedBlobs[idx];
        }

        bool IsEmpty() const {
            return SizeToWrite == 0 && BlobsToWrite == 0;
        }

        bool CanSendRequest() {
            if (InFlightTracker.LimitReached()) {
                return false;
            }
            return !EnoughBlobsWritten(true);
        }

        bool EnoughBlobsWritten(bool countPending = false) {
            if (SizeToWrite > 0) {
                return ConfirmedDataSize + InFlightTracker.BytesInFlight * countPending >= SizeToWrite;
            } else if (BlobsToWrite > 0) {
                return ConfirmedBlobs.size() + InFlightTracker.RequestsInFlight * countPending >= BlobsToWrite;
            }
            return true;
        }

        void ConfirmBlob(const TLogoBlobID& id, bool success) {
            InFlightTracker.Response(id.BlobSize());
            if (success) {
                ConfirmedBlobs.push_back(id);
                ConfirmedDataSize += id.BlobSize();
            }
        }

        std::unique_ptr<TEvBlobStorage::TEvPut> MakePutMessage(ui64 tabletId, ui32 gen, ui32 step, ui32 channel) {
            Y_DEBUG_ABORT_UNLESS(CanSendRequest());
            ui32 blobSize = SizeGenerator.Generate();
            const TLogoBlobID id(tabletId, gen, step, channel, blobSize, BlobCookie++);
            const TSharedData buffer = GenerateBuffer<TSharedData>(id);
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, buffer, TInstant::Max(), PutHandleClass);
            InFlightTracker.Request(blobSize);
            return std::move(ev);
        }

        std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> ManageKeepFlags(ui64 tabletId, ui32 gen, ui32 step,
                ui32 channel, bool keep) {
            auto blobsWritten = std::make_unique<TVector<TLogoBlobID>>(ConfirmedBlobs);

            if (keep) {
                return std::make_unique<TEvBlobStorage::TEvCollectGarbage>(tabletId, gen, step, channel,
                        false, gen, step, blobsWritten.release(), nullptr, TInstant::Max(), false);
            } else {
                ConfirmedDataSize = 0;
                ConfirmedBlobs.clear();
                return std::make_unique<TEvBlobStorage::TEvCollectGarbage>(tabletId, gen, step, channel,
                        true, gen, step, nullptr, blobsWritten.release(), TInstant::Max(), false);
            }
        }

        TLogoBlobID GetRandomBlobId() {
            Y_ABORT_UNLESS(!ConfirmedBlobs.empty());
            auto idx = RandomNumber(ConfirmedBlobs.size());
            return ConfirmedBlobs[idx];
        }

        TString ToString() {
            return TStringBuilder() << "TInitialAllocation# {"
                << " PutHandleClass# " << NKikimrBlobStorage::EPutHandleClass_Name(PutHandleClass)
                << " SizeToWrite# " << SizeToWrite
                << " BlobsToWrite# " << BlobsToWrite
                << " PendingWrites# " << InFlightTracker.ToString()
                << " ConfirmedSize# " << ConfirmedDataSize
                << " ConfirmedBlobs.size()# " << ConfirmedBlobs.size() << " }";
        }

    private:
        TSizeGenerator SizeGenerator;
        NKikimrBlobStorage::EPutHandleClass PutHandleClass = NKikimrBlobStorage::EPutHandleClass::UserData;

        uint64_t SizeToWrite = 0;
        uint64_t BlobsToWrite = 0;
        uint64_t ConfirmedDataSize = 0;
        TVector<TLogoBlobID> ConfirmedBlobs;

        TInFlightTracker InFlightTracker;

        ui64 BlobCookie = 0;
    };

    struct TRequestDelayManager {
        virtual ~TRequestDelayManager() = default;

        virtual void Start(TMonotonic now) = 0;
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

        void Start(TMonotonic/* now*/) override {
            return;
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
        THardRateDelayManager(double requestsPerSecondAtStart, double requestsPerSecondOnFinish, const TMaybe<TDuration>& duration)
            : EpochDuration(TDuration::MilliSeconds(100))
            , RequestRateAtStart((EpochDuration / TDuration::Seconds(1)) * requestsPerSecondAtStart)
            , RequestRateOnFinish((EpochDuration / TDuration::Seconds(1)) * requestsPerSecondOnFinish)
            , LoadDuration(duration) {
        }

        const TDuration EpochDuration;

        const double RequestRateAtStart;
        const double RequestRateOnFinish;

        double RequestsPerEpoch;
        TMonotonic CurrentEpochEnd;

        TDuration CurrentDelay = TDuration::Seconds(0);
        TMonotonic Now = TMonotonic::Max();

        TMonotonic LoadStart;
        const TMaybe<TDuration> LoadDuration;

        double PlannedForCurrentEpoch;
        ui32 ResponsesAwaiting = 0;

        void Start(TMonotonic now) override {
            LoadStart = now;
            CurrentEpochEnd = now;
            PlannedForCurrentEpoch = std::max(1., CalculateRequestRate(now));
            CalculateDelayForNextRequest(now);
        }

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
                << " ResponsesAwaiting# " << ResponsesAwaiting
                << " LoadStart# " << LoadStart
                << " Now# " << Now;

            return str.Str();
        }

        virtual TDuration GetDelayForCurrentState() const override {
            return CurrentDelay;
        }
    };

    friend class TTabletWriter;
    class TTabletWriter {
    public:
        struct TRequestDispatchingSettings {
            bool LoadEnabled = true;
            std::optional<TSizeGenerator> SizeGen;
            std::shared_ptr<TRequestDelayManager> DelayManager;
            TInFlightTracker InFlightTracker;
            const ui64 MaxTotalBytes;
        };

    private:
        using TLatencyTrackerUs = NMonitoring::TPercentileTrackerLg<5, 5, 10>;
        static_assert(TLatencyTrackerUs::TRACKER_LIMIT >= 100e6,
                "TLatencyTrackerUs must have limit grater than 100 second");

        const TVector<float> Percentiles{0.1, 0.15, 0.5, 0.9, 0.99, 0.999, 1.0};

        const TDuration ExposePeriod = TDuration::Seconds(10);

        TLogWriterLoadTestActor& Self;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> TagCounters;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
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
        ui64 TotalBytesWritten = 0;
        ui64 OkPutResults = 0;
        ui64 BadPutResults = 0;
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
        ui64 TotalBytesRead = 0;
        THashMap<ui64, ui64> ReadSentTimestamp;
        ui64 ReadQueryId = 0;
        bool NextReadInQueue = false;
        TSpeedTracker<ui64> ReadMegabytesPerSecondST;
        TQuantileTracker<ui64> ReadMegabytesPerSecondQT;
        std::unique_ptr<TLatencyTrackerUs> ReadResponseQT;
        TQuantileTracker<ui32> ReadsInFlightQT;
        TQuantileTracker<ui64> ReadBytesInFlightQT;

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

        // Initial allocation
        TInitialAllocation InitialAllocation;

        bool MainCycleStarted = false;
        // Blobs management
        TDeque<TLogoBlobID> ConfirmedBlobIds;

        // Garbage collection
        ui32 GarbageCollectionsInFlight = 0;
        TMonotonic NextGarbageCollectionTimestamp;
        bool NextGarbageCollectionInQueue = false;
        TIntervalGenerator GarbageCollectIntervalGen;
        // There is no point in having more than 1 active garbage collection request at the moment
        constexpr static ui32 MaxGarbageCollectionsInFlight = 1;

        TIntrusivePtr<NJaegerTracing::TThrottler> TracingThrottler;

    public:
        TTabletWriter(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                TLogWriterLoadTestActor& self, ui64 tabletId, ui32 channel,
                TMaybe<ui32> generation, ui32 groupId,
                NKikimrBlobStorage::EPutHandleClass putHandleClass, const TRequestDispatchingSettings& writeSettings,
                NKikimrBlobStorage::EGetHandleClass getHandleClass, const TRequestDispatchingSettings& readSettings,
                TIntervalGenerator garbageCollectIntervalGen,
                TDuration scriptedRoundDuration, TVector<TReqInfo>&& scriptedRequests,
                const TInitialAllocation& initialAllocation,
                const TIntrusivePtr<NJaegerTracing::TThrottler>& tracingThrottler)
            : Self(self)
            , TagCounters(counters->GetSubgroup("tag", Sprintf("%" PRIu64, Self.Tag)))
            , Counters(TagCounters->GetSubgroup("channel", Sprintf("%" PRIu32, channel)))
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
            , ScriptedRoundDuration(scriptedRoundDuration)
            , ScriptedCounter(0)
            , ScriptedRound(0)
            , ScriptedRequests(std::move(scriptedRequests))
            , InitialAllocation(initialAllocation)
            , GarbageCollectIntervalGen(garbageCollectIntervalGen)
            , TracingThrottler(tracingThrottler)
        {
            *Counters->GetCounter("tabletId") = tabletId;
            const auto& percCounters = Counters->GetSubgroup("subsystem", "latency");
            MaxInFlightLatency = percCounters->GetCounter("MaxInFlightLatencyUs");
            ResponseQT->Initialize(percCounters->GetSubgroup("sensor", "writeResponseUs"), Percentiles);
            ReadResponseQT->Initialize(percCounters->GetSubgroup("sensor", "readResponseUs"), Percentiles);
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
            NextGarbageCollectionTimestamp = TActivationContext::Monotonic();
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
            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
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
            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
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

                IssueTEvCollectGarbageOnce(ctx);
            };

            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void IssueTEvCollectGarbageOnce(const TActorContext& ctx) {
            auto ev = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(TabletId, Generation, GarbageCollectStep,
                    Channel, Generation, 0, TInstant::Max());
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " going to send " << ev->ToString());
            ++GarbageCollectStep;
            auto callback = [this] (IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult *>(event);
                Y_ABORT_UNLESS(res);
                --GarbageCollectionsInFlight;
                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK})) {
                    return;
                }
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());
                MakeInitialAllocation(ctx);
            };

            ++GarbageCollectionsInFlight;
            SendToBSProxy(ctx, GroupId, ev.Release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void StartWorking(const TActorContext& ctx) {
            MainCycleStarted = true;
            StartTimestamp = TActivationContext::Monotonic();
            if (Self.TestDuration) {
                ctx.Schedule(*Self.TestDuration, new TEvents::TEvPoisonPill());
            }
            InitializeTrackers(StartTimestamp);
            WriteSettings.DelayManager->Start(StartTimestamp);
            IssueWriteIfPossible(ctx);
            ReadSettings.DelayManager->Start(StartTimestamp);
            IssueReadIfPossible(ctx);
            IssueGarbageCollectionIfPossible(ctx);
            ExposeCounters(ctx);
        }

        void MakeInitialAllocation(const TActorContext& ctx) {
            if (InitialAllocation.IsEmpty()) {
                Self.InitialAllocationCompleted(ctx);
                return;
            }
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " going to make initial allocation,"
                    << InitialAllocation.ToString());
            while (InitialAllocation.CanSendRequest()) {
                IssueInitialPut(ctx);
            }
        }

        void IssueInitialPut(const TActorContext& ctx) {
            auto ev = InitialAllocation.MakePutMessage(TabletId, Generation, GarbageCollectStep, Channel);

            auto callback = [this](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvPutResult*>(event);
                Y_ABORT_UNLESS(res);

                InitialAllocation.ConfirmBlob(res->Id, CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK}));
                while (InitialAllocation.CanSendRequest()) {
                    IssueInitialPut(ctx);
                }
                if (InitialAllocation.EnoughBlobsWritten()) {
                    SetKeepFlagsOnInitialAllocation(ctx);
                }
            };
            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void SetKeepFlagsOnInitialAllocation(const TActorContext& ctx) {
            auto ev = InitialAllocation.ManageKeepFlags(TabletId, Generation, GarbageCollectStep, Channel, true);

            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " going to set keep flags on initally allocated blobs, ev#"
                    << ev->Print(false));
            auto callback = [this](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult*>(event);
                Y_ABORT_UNLESS(res);
                
                if (!MainCycleStarted) {
                    Self.InitialAllocationCompleted(ctx);
                }
            };

            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
        }

        void StopWorking(const TActorContext& ctx) {
            auto ev = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(TabletId, Generation, GarbageCollectStep,
                    Channel, Generation, Max<ui32>(), TInstant::Max());
            LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " end working, going to send " << ev->ToString());
            ++GarbageCollectStep;
            auto callback = [this](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult *>(event);
                Y_ABORT_UNLESS(res);
                --GarbageCollectionsInFlight;

                if (!CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK})) {
                    return;
                }
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, PrintMe() << " recieved " << res->ToString());

                if (IsWorkingNow) {
                    ctx.Send(ctx.SelfID, new TEvStopTest());
                }
            };
            SendToBSProxy(ctx, GroupId, ev.Release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));

            ++GarbageCollectionsInFlight;
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
            WritesInFlightQT.Add(now, WriteSettings.InFlightTracker.RequestsInFlight);
            WriteBytesInFlightQT.Add(now, WriteSettings.InFlightTracker.BytesInFlight);
            ReadsInFlightQT.Add(now, ReadSettings.InFlightTracker.RequestsInFlight);
            ReadBytesInFlightQT.Add(now, ReadSettings.InFlightTracker.BytesInFlight);
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
            Self.WakeupQueue.Put(TActivationContext::Monotonic() + ExposePeriod,
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
                DUMP_PARAM(WriteSettings.InFlightTracker.ToString())
                DUMP_PARAM_FINAL(TotalBytesWritten)
                DUMP_PARAM_FINAL(OkPutResults)
                DUMP_PARAM_FINAL(BadPutResults)
                DUMP_PARAM_FINAL(WriteSettings.MaxTotalBytes)
                DUMP_PARAM_FINAL(TotalBytesRead)
                DUMP_PARAM(NextReadTimestamp)
                DUMP_PARAM(ReadSettings.InFlightTracker.ToString())
                DUMP_PARAM(ConfirmedBlobIds.size())
                DUMP_PARAM(InitialAllocation.ToString())
                DUMP_PARAM(GarbageCollectionsInFlight)

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
                Self.WakeupQueue.Put(NextWriteTimestamp, std::bind(&TTabletWriter::IssueWriteIfPossible, this, _1), ctx);
                NextWriteInQueue = true;
            }

            if (now < NextReadTimestamp && !NextReadInQueue) {
                using namespace std::placeholders;
                Self.WakeupQueue.Put(NextReadTimestamp, std::bind(&TTabletWriter::IssueReadIfPossible, this, _1), ctx);
                NextReadInQueue = true;
            }

            if (now < NextGarbageCollectionTimestamp && !NextGarbageCollectionInQueue) {
                using namespace std::placeholders;
                Self.WakeupQueue.Put(NextGarbageCollectionTimestamp, std::bind(&TTabletWriter::IssueGarbageCollectionIfPossible, this, _1), ctx);
                NextGarbageCollectionInQueue = true;
            }
        }

        void IssueWriteIfPossible(const TActorContext& ctx) {
            const TMonotonic now = TActivationContext::Monotonic();
            while (WriteSettings.LoadEnabled && !WriteSettings.InFlightTracker.LimitReached() &&
                    (TotalBytesWritten + WriteSettings.InFlightTracker.BytesInFlight < WriteSettings.MaxTotalBytes || !WriteSettings.MaxTotalBytes) &&
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
                size = WriteSettings.SizeGen->Generate();
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
                const bool ok = CheckStatus(ctx, res, {NKikimrProto::EReplyStatus::OK});
                ++ (ok ? OkPutResults : BadPutResults);

                const TLogoBlobID& id = res->Id;
                const ui32 size = id.BlobSize();

                if (ok) {
                    // this blob has been confirmed -- update set
                    if (!ConfirmedBlobIds || id > ConfirmedBlobIds.back()) {
                        ConfirmedBlobIds.push_back(id);
                    } else {
                        // most likely inserted somewhere near the end
                        ConfirmedBlobIds.insert(std::lower_bound(ConfirmedBlobIds.begin(), ConfirmedBlobIds.end(), id), id);
                    }
                    TotalBytesWritten += size;
                }

                WriteSettings.InFlightTracker.Response(size);


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

                if (ConfirmedBlobIds.size() == 1 && InitialAllocation.IsEmpty()) {
                    if (NextReadTimestamp == TMonotonic()) {
                        NextReadTimestamp = TActivationContext::Monotonic();
                    }
                    IssueReadIfPossible(ctx);
                }
            };

            NWilson::TTraceId traceId = (TracingThrottler && !TracingThrottler->Throttle())
                    ? NWilson::TTraceId::NewTraceId(15, ::Max<ui32>())
                    : NWilson::TTraceId{};

            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(writeCallback)),
                    std::move(traceId));
            const auto nowCycles = GetCycleCountFast();
            WritesInFlightTimestamps.emplace_back(writeQueryId, nowCycles);
            SentTimestamp.emplace(writeQueryId, nowCycles);
            IssuedWriteTimestamp.push_back(TActivationContext::Monotonic());
            while (IssuedWriteTimestamp.size() > 10000 || IssuedWriteTimestamp.back() - IssuedWriteTimestamp.front() >= TDuration::Seconds(5)) {
                IssuedWriteTimestamp.pop_front();
            }

            ++Cookie;

            WriteSettings.InFlightTracker.Request(size);

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

        void IssueGarbageCollectionIfPossible(const TActorContext& ctx) {
            const TMonotonic now = TActivationContext::Monotonic();
            while (GarbageCollectionsInFlight < MaxGarbageCollectionsInFlight &&
                    NextGarbageCollectionTimestamp <= now) {
                IssueGarbageCollectRequest(ctx);
            }
            UpdateNextWakeups(ctx, now);
        }

        void IssueGarbageCollectRequest(const TActorContext& ctx) {
            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(TabletId, Generation, GarbageCollectStep, Channel,
                    true, Generation, GarbageCollectStep, nullptr, nullptr, TInstant::Max(), false);
            auto callback = [this](IEventBase *event, const TActorContext& ctx) {
                auto *res = dynamic_cast<TEvBlobStorage::TEvCollectGarbageResult *>(event);
                Y_ABORT_UNLESS(res);
                --GarbageCollectionsInFlight;
                IssueGarbageCollectionIfPossible(ctx);
            };
            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(callback)));
            ++GarbageCollectionsInFlight;

            // just as we have sent this request, we have to trim all confirmed blobs which are going to be deleted
            const auto it = std::lower_bound(ConfirmedBlobIds.begin(), ConfirmedBlobIds.end(),
                TLogoBlobID(TabletId, Generation, GarbageCollectStep, Channel, TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId));
            ConfirmedBlobIds.erase(ConfirmedBlobIds.begin(), it);

            ++GarbageCollectStep;
            ++WriteStep;
            Cookie = 1;

            NextGarbageCollectionTimestamp += GarbageCollectIntervalGen.Generate();
            NextGarbageCollectionInQueue = false;
        }

        void IssueReadIfPossible(const TActorContext& ctx) {
            const TMonotonic now = TActivationContext::Monotonic();

            while (ReadSettings.LoadEnabled && !ReadSettings.InFlightTracker.LimitReached() &&
                    now >= NextReadTimestamp &&
                    ConfirmedBlobIds.size() + InitialAllocation.ConfirmedSize() > 0 &&
                    (!ScriptedRequests || ScriptedRequests[ScriptedCounter].EvType == TEvBlobStorage::EvGet)) {
                IssueReadRequest(ctx);
            }

            if (ScriptedRequests) {
                UpdateNextTimestemps(false);
            }
            UpdateNextWakeups(ctx, now);
        }

        void IssueReadRequest(const TActorContext& ctx) {
            TLogoBlobID id;
            ui32 confirmedBlobs = ConfirmedBlobIds.size();
            ui32 initialBlobs = InitialAllocation.ConfirmedSize();
            Y_ABORT_UNLESS(confirmedBlobs + initialBlobs > 0);
            ui32 blobIdx = RandomNumber(confirmedBlobs + initialBlobs);
    
            if (blobIdx < confirmedBlobs) {
                auto iter = ConfirmedBlobIds.begin();
                std::advance(iter, blobIdx);
                id = *iter;
            } else {
                id = InitialAllocation[blobIdx - confirmedBlobs];
            }

            ui32 size = Max<ui32>();
            if (ScriptedRequests) {
                const auto& req = ScriptedRequests[ScriptedCounter];
                size = req.Size ? req.Size : id.BlobSize();
            } else if (ReadSettings.SizeGen) {
                size = ReadSettings.SizeGen->Generate();
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

                ReadSettings.InFlightTracker.Response(size);
                TotalBytesRead += size;

                auto it = ReadSentTimestamp.find(readQueryId);
                Y_ABORT_UNLESS(it != ReadSentTimestamp.end());
                const TDuration response = CyclesToDuration(GetCycleCountFast() - it->second);
                ReadSentTimestamp.erase(it);

                ReadResponseQT->Increment(response.MicroSeconds());
                IssueReadIfPossible(ctx);
            };

            NWilson::TTraceId traceId = (TracingThrottler && !TracingThrottler->Throttle())
                    ? NWilson::TTraceId::NewTraceId(15, ::Max<ui32>())
                    : NWilson::TTraceId{};

            SendToBSProxy(ctx, GroupId, ev.release(), Self.QueryDispatcher.ObtainCookie(std::move(readCallback)),
                    std::move(traceId));
            ReadSentTimestamp.emplace(readQueryId, GetCycleCountFast());

            ReadSettings.InFlightTracker.Request(size);

            // calculate time of next read request
            if (ScriptedRequests) {
                UpdateNextTimestemps(true);
            } else {
                NextReadTimestamp += ReadSettings.DelayManager->CalculateDelayForNextRequest(TActivationContext::Monotonic());
            }
            NextReadInQueue = false;
        }
    };

    enum EWakeupType : ui32 {
        MAIN_CYCLE = 0,
        DELAY_AFTER_INITIAL_WRITE,
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

    static constexpr ui64 DefaultTabletId = 5000;
    ui32 WorkersInInitialState = 0;

    ui32 DelayAfterInitialWrite = 0;

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

        std::unordered_map<TString, ui64> tabletIds;
        for (const auto& profile : cmd.GetTablets()) {
            if (!profile.TabletsSize()) {
                ythrow TLoadActorException() << "TPerTabletProfile.Tablets must have at least one item";
            }
            bool enableWrites = profile.WriteSizesSize() && profile.GetPutHandleClass() &&
                    (profile.WriteIntervalsSize() || profile.HasWriteHardRateDispatcher());

            TInitialAllocation initialAllocation;
            if (profile.HasInitialAllocation()) {
                auto initialAllocationProto = profile.GetInitialAllocation();
                initialAllocation = TInitialAllocation(profile.GetInitialAllocation());
                if (initialAllocationProto.HasDelayAfterCompletionSec()) {
                    DelayAfterInitialWrite = std::max(DelayAfterInitialWrite, initialAllocationProto.GetDelayAfterCompletionSec());
                }
            }

            NKikimrBlobStorage::EPutHandleClass putHandleClass = NKikimrBlobStorage::EPutHandleClass::UserData;
            if (profile.HasPutHandleClass()) {
                putHandleClass = profile.GetPutHandleClass();
            }

            // object may be shared across multiple writers
            std::shared_ptr<TRequestDelayManager> writeDelayManager;
            if (profile.HasWriteHardRateDispatcher()) {
                const auto& dispatcherSettings = profile.GetWriteHardRateDispatcher();
                double atStart = dispatcherSettings.GetRequestsPerSecondAtStart();
                double onFinish = dispatcherSettings.GetRequestsPerSecondOnFinish();
                writeDelayManager = std::make_shared<THardRateDelayManager>(atStart, onFinish, TestDuration);
            } else {
                writeDelayManager = std::make_shared<TRandomIntervalDelayManager>(TIntervalGenerator(profile.GetWriteIntervals()));
            }

            TTabletWriter::TRequestDispatchingSettings writeSettings{
                .LoadEnabled = enableWrites,
                .SizeGen = TSizeGenerator(profile.GetWriteSizes()),
                .DelayManager = std::move(writeDelayManager),
                .InFlightTracker = TInFlightTracker(profile.GetMaxInFlightWriteRequests(), profile.GetMaxInFlightWriteBytes()),
                .MaxTotalBytes = profile.GetMaxTotalBytesWritten(),
            };

            bool enableReads = profile.ReadIntervalsSize() || profile.HasReadHardRateDispatcher();
            NKikimrBlobStorage::EGetHandleClass getHandleClass = NKikimrBlobStorage::EGetHandleClass::FastRead;
            if (profile.HasGetHandleClass()) {
                getHandleClass = profile.GetGetHandleClass();
            }

            std::shared_ptr<TRequestDelayManager> readDelayManager;
            if (profile.HasReadHardRateDispatcher()) {
                const auto& dispatcherSettings = profile.GetReadHardRateDispatcher();
                double atStart = dispatcherSettings.GetRequestsPerSecondAtStart();
                double onFinish = dispatcherSettings.GetRequestsPerSecondOnFinish();
                readDelayManager = std::make_shared<THardRateDelayManager>(atStart, onFinish, TestDuration);
            } else {
                readDelayManager = std::make_shared<TRandomIntervalDelayManager>(TIntervalGenerator(profile.GetReadIntervals()));
            }

            std::optional<TSizeGenerator> readSizeGen;
            if (profile.ReadSizesSize() > 0) {
                readSizeGen.emplace(profile.GetReadSizes());
            }

            TTabletWriter::TRequestDispatchingSettings readSettings{
                .LoadEnabled = enableReads,
                .SizeGen = readSizeGen,
                .DelayManager = std::move(readDelayManager),
                .InFlightTracker = TInFlightTracker(profile.GetMaxInFlightReadRequests(), profile.GetMaxInFlightReadBytes()),
                .MaxTotalBytes = ::Max<ui64>(),
            };

            TIntervalGenerator garbageCollectIntervalGen(profile.GetFlushIntervals());

            TIntrusivePtr<NJaegerTracing::TThrottler> tracingThrottler;

            ui32 throttlerRate = profile.GetTracingThrottlerRate();
            if (throttlerRate) {
                tracingThrottler = MakeIntrusive<NJaegerTracing::TThrottler>(throttlerRate, profile.GetTracingThrottlerBurst(),
                        TAppData::TimeProvider);
            }

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

                if (!tablet.HasChannel() || !tablet.HasGroupId()) {
                    ythrow TLoadActorException() << "TTabletInfo.{Channel,GroupId} fields are mandatory";
                }
                if (!tablet.HasTabletName() && !tablet.HasTabletId()) {
                    ythrow TLoadActorException() << "One of TTabletInfo.{TabletName,TabletId} fields must be specified";
                }

                ui64 tabletId;
                if (tablet.HasTabletId()) {
                    tabletId = tablet.GetTabletId();
                } else if (tablet.HasTabletName()) {
                    TString name = tablet.GetTabletName();
                    auto it = tabletIds.find(name);
                    if (it != tabletIds.end()) {
                        tabletId = it->second;
                    } else {
                        tabletId = THash<TString>{}(name) & ((1 << 20) - 1);
                        tabletId = (tabletId << 10) + tag;
                        tabletId = (tabletId << 10) + Parent.NodeId();
                        tabletId &= (1ull << 44) - 1;
                        tabletId = MakeTabletID(false, tabletId);
                        tabletIds[name] = tabletId;
                    }
                } else {
                    Y_FAIL();
                }

                TabletWriters.emplace_back(std::make_unique<TTabletWriter>(counters, *this, tabletId,
                    tablet.GetChannel(), tablet.HasGeneration() ?  TMaybe<ui32>(tablet.GetGeneration()) : TMaybe<ui32>(),
                    tablet.GetGroupId(), putHandleClass, writeSettings,
                    getHandleClass, readSettings,
                    garbageCollectIntervalGen,
                    scriptedRoundDuration, std::move(scriptedRequests),
                    initialAllocation, tracingThrottler));

                WorkersInInitialState++;
            }
        }
    }

    void StartWorkers(const TActorContext& ctx) {
        if (TestDuration) {
            ctx.Schedule(*TestDuration, new TEvents::TEvPoisonPill());
        }
        for (auto& writer : TabletWriters) {
            writer->StartWorking(ctx);
        }
        TestStartTime = TActivationContext::Monotonic();
        UpdateWakeupQueue(ctx);
    }

    void InitialAllocationCompleted(const TActorContext& ctx) {
        WorkersInInitialState--;
        if (!WorkersInInitialState) {
            if (DelayAfterInitialWrite) {
                ctx.Schedule(TDuration::Seconds(DelayAfterInitialWrite),
                        new TEvents::TEvWakeup(DELAY_AFTER_INITIAL_WRITE));
            } else {
                StartWorkers(ctx);
            }
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TLogWriterLoadTestActor::StateFunc);
        EarlyStop = false;
        for (auto& writer : TabletWriters) {
            writer->Bootstrap(ctx);
        }
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

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->Tag) {
        case MAIN_CYCLE:
            --WakeupsScheduled;
            UpdateWakeupQueue(ctx);
            break;

        case DELAY_AFTER_INITIAL_WRITE:
            StartWorkers(ctx);
            break;

        default:
            Y_FAIL_S("Unexpected wakeup tag# " << ev->Get()->Tag);
        }
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
                            if (TestStartTime != TMonotonic()) {
                                str << (TActivationContext::Monotonic() - TestStartTime).Seconds();
                            } else {
                                str << "0";
                            }
                            str << " / ";
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

    template <class ResultContainer = TString>
    static ResultContainer GenerateBuffer(const TLogoBlobID& id) {
        return FastGenDataForLZ4<ResultContainer>(id.BlobSize());
    }

    STRICT_STFUNC(StateFunc,
        CFunc(EvStopTest, HandleStopTest);
        CFunc(EvUpdateQuantile, HandleUpdateQuantile);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
        HFunc(TEvBlobStorage::TEvDiscoverResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvBlockResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvPutResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvGetResult, HandleDispatcher);
        HFunc(TEvBlobStorage::TEvCollectGarbageResult, HandleDispatcher);
        HFunc(NMon::TEvHttpInfo, Handle);
        HFunc(TEvents::TEvUndelivered, Handle);
    )
};

IActor *CreateWriterLoadTest(const NKikimr::TEvLoadTestRequest::TStorageLoad& cmd, const TActorId& parent,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag) {
    return new TLogWriterLoadTestActor(cmd, parent, std::move(counters), tag);
}

} // NKikimr

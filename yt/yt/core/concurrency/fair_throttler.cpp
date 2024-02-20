#include "fair_throttler.h"

#include "private.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/finally.h>

#include <util/random/shuffle.h>

#include <util/folder/path.h>

#include <util/system/file_lock.h>
#include <util/system/file.h>
#include <util/system/filemap.h>
#include <util/system/mlock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

void TFairThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("total_limit", &TThis::TotalLimit)
        .Default(125_MB);

    registrar.Parameter("distribution_period", &TThis::DistributionPeriod)
        .Default(TDuration::MilliSeconds(100))
        .GreaterThan(TDuration::Zero());

    registrar.Parameter("bucket_accumulation_ticks", &TThis::BucketAccumulationTicks)
        .Default(5);

    registrar.Parameter("global_accumulation_ticks", &TThis::GlobalAccumulationTicks)
        .Default(5);

    registrar.Parameter("ipc_path", &TThis::IPCPath)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFairThrottlerBucketConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("weight", &TThis::Weight)
        .Default(1.0)
        .GreaterThanOrEqual(0.01)
        .LessThanOrEqual(100);

    registrar.Parameter("limit", &TThis::Limit)
        .Default();

    registrar.Parameter("relative_limit", &TThis::RelativeLimit)
        .Default();

    registrar.Parameter("guarantee", &TThis::Guarantee)
        .Default();

    registrar.Parameter("relative_guaratee", &TThis::RelativeGuarantee)
        .Default();
}

std::optional<i64> TFairThrottlerBucketConfig::GetLimit(i64 totalLimit)
{
    if (Limit) {
        return Limit;
    }

    if (RelativeLimit) {
        return totalLimit * *RelativeLimit;
    }

    return {};
}

std::optional<i64> TFairThrottlerBucketConfig::GetGuarantee(i64 totalLimit)
{
    if (Guarantee) {
        return Guarantee;
    }

    if (RelativeGuarantee) {
        return totalLimit * *RelativeGuarantee;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

static constexpr TStringBuf LockFileName = "lock";
static constexpr TStringBuf SharedFileName = "shared.v1";
static constexpr TStringBuf BucketsFileName = "buckets.v1";

////////////////////////////////////////////////////////////////////////////////

class TFileIPCBucket
    : public IIPCBucket
{
public:
    TFileIPCBucket(const TString& path, bool create)
        : File_(path, OpenAlways | RdWr)
    {
        if (create) {
            File_.Flock(LOCK_EX | LOCK_NB);
        }

        File_.Resize(sizeof(TBucket));

        Map_ = std::make_unique<TFileMap>(File_, TMemoryMapCommon::oRdWr);
        Map_->Map(0, Map_->Length());
        LockMemory(Map_->Ptr(), Map_->Length());
    }

    TBucket* State() override
    {
        return reinterpret_cast<TBucket*>(Map_->Ptr());
    }

private:
    TFile File_;
    std::unique_ptr<TFileMap> Map_;
};

DEFINE_REFCOUNTED_TYPE(TFileIPCBucket)

////////////////////////////////////////////////////////////////////////////////

class TFileThrottlerIPC
    : public IThrottlerIPC
{
public:
    explicit TFileThrottlerIPC(const TString& path)
        : Path_(path)
    {
        NFS::MakeDirRecursive(Path_);
        NFS::MakeDirRecursive(Path_ + "/" + BucketsFileName);

        Lock_ = std::make_unique<TFileLock>(Path_ + "/" + LockFileName);

        SharedBucketFile_ = TFile(Path_ + "/" + SharedFileName, OpenAlways | RdWr);
        SharedBucketFile_.Resize(sizeof(TSharedBucket));

        SharedBucketMap_ = std::make_unique<TFileMap>(SharedBucketFile_, TMemoryMapCommon::oRdWr);
        SharedBucketMap_->Map(0, SharedBucketMap_->Length());
        LockMemory(SharedBucketMap_->Ptr(), SharedBucketMap_->Length());
    }

    bool TryLock() override
    {
        try {
            SharedBucketFile_.Flock(LOCK_EX | LOCK_NB);
            return true;
        } catch (const TSystemError& e) {
            if (e.Status() != EWOULDBLOCK) {
                throw;
            }

            return false;
        }
    }

    TSharedBucket* State() override
    {
        return reinterpret_cast<TSharedBucket*>(SharedBucketMap_->Ptr());
    }

    std::vector<IIPCBucketPtr> ListBuckets() override
    {
        Lock_->Acquire();
        auto release = Finally([this] {
            Lock_->Release();
        });

        Reload();

        std::vector<IIPCBucketPtr> buckets;
        for (const auto& bucket : OpenBuckets_) {
            buckets.push_back(bucket.second);
        }
        return buckets;
    }

    IIPCBucketPtr AddBucket() override
    {
        Lock_->Acquire();
        auto release = Finally([this] {
            Lock_->Release();
        });

        auto id = TGuid::Create();
        OwnedBuckets_.insert(ToString(id));
        return New<TFileIPCBucket>(Path_ + "/" + BucketsFileName + "/" + ToString(id), true);
    }

private:
    const TString Path_;

    std::unique_ptr<TFileLock> Lock_;

    TFile SharedBucketFile_;
    std::unique_ptr<TFileMap> SharedBucketMap_;

    THashMap<TString, IIPCBucketPtr> OpenBuckets_;
    THashSet<TString> OwnedBuckets_;

    void Reload()
    {
        TVector<TString> currentBuckets;
        TFsPath{Path_ + "/" + BucketsFileName}.ListNames(currentBuckets);

        auto openBuckets = std::move(OpenBuckets_);
        for (const auto& fileName : currentBuckets) {
            if (OwnedBuckets_.find(fileName) != OwnedBuckets_.end()) {
                continue;
            }

            try {
                auto bucketPath = Path_ + "/" + BucketsFileName + "/" + fileName;

                TFileLock lockCheck(bucketPath);
                if (lockCheck.TryAcquire()) {
                    NFS::Remove(bucketPath);
                    continue;
                }

                if (auto it = openBuckets.find(fileName); it != openBuckets.end()) {
                    OpenBuckets_.emplace(fileName, it->second);
                    continue;
                }

                OpenBuckets_[fileName] = New<TFileIPCBucket>(bucketPath, false);
            } catch (const TSystemError& ex) {
                continue;
            }
        }
    }
};

IThrottlerIPCPtr CreateFileThrottlerIPC(const TString& path)
{
    return New<TFileThrottlerIPC>(path);
}

DEFINE_REFCOUNTED_TYPE(TFileThrottlerIPC)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBucketThrottleRequest)

struct TBucketThrottleRequest
    : public TRefCounted
{
    explicit TBucketThrottleRequest(i64 amount)
        : Pending(amount)
    { }

    i64 Pending;
    i64 Reserved = 0;
    TPromise<void> Promise = NewPromise<void>();
    std::atomic<bool> Cancelled = false;
    NProfiling::TCpuInstant StartTime = NProfiling::GetCpuInstant();

    void Cancel(const TError& /*error*/)
    {
        Cancelled = true;
        Promise.TrySet(TError(NYT::EErrorCode::Canceled, "Cancelled"));
    }
};

DEFINE_REFCOUNTED_TYPE(TBucketThrottleRequest)

////////////////////////////////////////////////////////////////////////////////

struct TLeakyCounter
{
    TLeakyCounter(int windowSize, NProfiling::TGauge quotaGauge)
        : Value(std::make_shared<std::atomic<i64>>(0))
        , Window(windowSize)
        , QuotaGauge(std::move(quotaGauge))
    { }

    std::shared_ptr<std::atomic<i64>> Value;

    std::vector<i64> Window;
    int WindowPosition = 0;

    NProfiling::TGauge QuotaGauge;

    i64 Increment(i64 delta)
    {
        return Increment(delta, delta);
    }

    i64 Increment(i64 delta, i64 limit)
    {
        auto maxValue = std::accumulate(Window.begin(), Window.end(), i64(0)) - Window[WindowPosition];

        auto currentValue = Value->load();
        do {
            if (currentValue <= maxValue) {
                break;
            }
        } while (!Value->compare_exchange_strong(currentValue, maxValue));

        Window[WindowPosition] = limit;

        WindowPosition++;
        if (WindowPosition >= std::ssize(Window)) {
            WindowPosition = 0;
        }

        *Value += delta;
        QuotaGauge.Update(Value->load());

        return std::max<i64>(currentValue - maxValue, 0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSharedBucket final
{
    TSharedBucket(int windowSize, const NProfiling::TProfiler& profiler)
        : Limit(windowSize, profiler.Gauge("/shared_quota"))
    { }

    TLeakyCounter Limit;
};

DEFINE_REFCOUNTED_TYPE(TSharedBucket)

////////////////////////////////////////////////////////////////////////////////

class TBucketThrottler
    : public IThroughputThrottler
{
public:
    TBucketThrottler(
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler,
        const TSharedBucketPtr& sharedBucket,
        TFairThrottlerConfigPtr config)
        : Logger(logger)
        , SharedBucket_(sharedBucket)
        , Value_(profiler.Counter("/value"))
        , Released_(profiler.Counter("/released"))
        , WaitTime_(profiler.Timer("/wait_time"))
        , Quota_(config->BucketAccumulationTicks, profiler.Gauge("/quota"))
        , DistributionPeriod_(config->DistributionPeriod)
    {
        profiler.AddFuncGauge("/queue_size", MakeStrong(this), [this] {
            return GetQueueTotalAmount();
        });

        profiler.AddFuncGauge("/throttled", MakeStrong(this), [this] {
            return IsOverdraft();
        });
    }

    TFuture<void> Throttle(i64 amount) override
    {
        if (TryAcquire(amount)) {
            return VoidFuture;
        }

        auto request = New<TBucketThrottleRequest>(amount);
        QueueSize_ += amount;

        request->Promise.OnCanceled(BIND(&TBucketThrottleRequest::Cancel, MakeWeak(request)));
        request->Promise.ToFuture().Subscribe(BIND(&TBucketThrottler::OnRequestComplete, MakeWeak(this), amount));

        YT_LOG_DEBUG("Started waiting for throttler (Amount: %v)", amount);

        auto guard = Guard(Lock_);
        Queue_.push_back(request);
        return request->Promise.ToFuture();
    }

    bool TryAcquire(i64 amount) override
    {
        YT_VERIFY(amount >= 0);

        auto available = Quota_.Value->load();
        auto globalAvailable = IsLimited()
            ? 0
            : std::max<i64>(0, SharedBucket_->Limit.Value->load());

        if (amount > available + globalAvailable) {
            return false;
        }

        auto globalConsumed = std::clamp<i64>(amount - available, 0, globalAvailable);
        *Quota_.Value -= amount - globalConsumed;
        *SharedBucket_->Limit.Value -= globalConsumed;

        Value_.Increment(amount);
        Usage_ += amount;

        return true;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        YT_VERIFY(amount >= 0);

        auto available = Quota_.Value->load();
        auto globalAvailable = IsLimited()
            ? 0
            : std::max<i64>(0, SharedBucket_->Limit.Value->load());

        auto consumed = std::min(amount, available + globalAvailable);

        auto globalConsumed = std::clamp<i64>(consumed - available, 0, globalAvailable);
        *Quota_.Value -= consumed - globalConsumed;
        *SharedBucket_->Limit.Value -= globalConsumed;

        Value_.Increment(consumed);
        Usage_ += consumed;

        return consumed;
    }

    void Acquire(i64 amount) override
    {
        YT_VERIFY(amount >= 0);

        auto available = Quota_.Value->load();
        // NB: Shared bucket limit can get below zero because resource acquisition is racy.
        auto globalAvailable = IsLimited()
            ? 0
            : std::max<i64>(0, SharedBucket_->Limit.Value->load());

        auto globalConsumed = std::clamp<i64>(amount - available, 0, globalAvailable);
        *Quota_.Value -= amount - globalConsumed;
        *SharedBucket_->Limit.Value -= globalConsumed;

        Value_.Increment(amount);
        Usage_ += amount;
    }

    void Release(i64 amount) override
    {
        YT_VERIFY(amount >= 0);

        if (amount == 0) {
            return;
        }

        *Quota_.Value += amount;
        Usage_ -= amount;

        Released_.Increment(amount);
    }

    bool IsOverdraft() override
    {
        return GetQueueTotalAmount() > 0;
    }

    i64 GetQueueTotalAmount() const override
    {
        return Max<i64>(-Quota_.Value->load(), 0) + QueueSize_.load();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        auto queueTotalAmount = GetQueueTotalAmount();
        if (queueTotalAmount == 0) {
            return TDuration::Zero();
        }

        auto limit = LastLimit_.load();
        auto distributionPeriod = DistributionPeriod_.load();
        if (limit == 0) {
            return distributionPeriod;
        }

        return queueTotalAmount / limit * distributionPeriod;
    }

    i64 GetAvailable() const override
    {
        auto available = Quota_.Value->load();
        auto globalAvailable = IsLimited() ? 0 : SharedBucket_->Limit.Value->load();
        return available + globalAvailable;
    }

    struct TBucketState
    {
        i64 Usage; // Quota usage on current iteration.
        i64 Quota;
        i64 Overdraft; // Unpaid overdraft from previous iterations.
        i64 QueueSize; // Total size of all queued requests.
    };

    TBucketState Peek()
    {
        auto quota = Quota_.Value->load();

        return TBucketState{
            .Usage = Usage_.exchange(0),
            .Quota = quota,
            .Overdraft = Max<i64>(-quota, 0),
            .QueueSize = QueueSize_.load(),
        };
    }

    i64 SatisfyRequests(i64 quota)
    {
        std::vector<TBucketThrottleRequestPtr> readyList;

        {
            auto guard = Guard(Lock_);
            while (!Queue_.empty()) {
                auto request = Queue_.front();
                if (request->Cancelled.load()) {
                    quota += request->Reserved;
                    Queue_.pop_front();
                    continue;
                }

                if (request->Pending <= quota) {
                    quota -= request->Pending;
                    Queue_.pop_front();
                    readyList.push_back(std::move(request));
                } else {
                    request->Pending -= quota;
                    request->Reserved += quota;
                    quota = 0;
                    break;
                }
            }
        }

        auto now = NProfiling::GetCpuInstant();
        for (const auto& request : readyList) {
            auto waitTime = NProfiling::CpuDurationToDuration(now - request->StartTime);

            WaitTime_.Record(waitTime);
            Value_.Increment(request->Pending + request->Reserved);
            Usage_ += request->Pending + request->Reserved;

            request->Promise.TrySet();
        }

        return quota;
    }

    i64 Refill(i64 quota)
    {
        LastLimit_ = quota;

        if (Quota_.Value->load() < 0) {
            auto remainingQuota = Quota_.Increment(quota);
            return SatisfyRequests(remainingQuota);
        } else {
            auto remainingQuota = SatisfyRequests(quota);
            return Quota_.Increment(remainingQuota, quota);
        }
    }

    void SetDistributionPeriod(TDuration distributionPeriod)
    {
        DistributionPeriod_.store(distributionPeriod);
    }

    void SetLimited(bool limited)
    {
        Limited_ = limited;
    }

    bool IsLimited() const
    {
        return Limited_.load();
    }

private:
    NLogging::TLogger Logger;

    TSharedBucketPtr SharedBucket_;

    NProfiling::TCounter Value_;
    NProfiling::TCounter Released_;
    NProfiling::TEventTimer WaitTime_;

    TLeakyCounter Quota_;
    std::atomic<i64> LastLimit_ = 0;
    std::atomic<i64> QueueSize_ = 0;
    std::atomic<i64> Usage_ = 0;

    std::atomic<bool> Limited_ = {false};

    std::atomic<TDuration> DistributionPeriod_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TBucketThrottleRequestPtr> Queue_;

    void OnRequestComplete(i64 amount, const TError& /*error*/)
    {
        QueueSize_ -= amount;
    }
};

DEFINE_REFCOUNTED_TYPE(TBucketThrottler)

////////////////////////////////////////////////////////////////////////////////

TFairThrottler::TFairThrottler(
    TFairThrottlerConfigPtr config,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
    : Logger(std::move(logger))
    , Profiler_(std::move(profiler))
    , SharedBucket_(New<TSharedBucket>(config->GlobalAccumulationTicks, Profiler_))
    , Config_(std::move(config))
{
    if (Config_->IPCPath) {
        IPC_ = New<TFileThrottlerIPC>(*Config_->IPCPath);

        SharedBucket_->Limit.Value = std::shared_ptr<std::atomic<i64>>(
            &IPC_->State()->Value,
            [ipc=IPC_] (auto /*ptr*/) { }
        );

        Profiler_.AddFuncGauge("/leader", MakeStrong(this), [this] {
            return IsLeader_.load();
        });
    } else {
        IsLeader_ = true;
    }

    ScheduleLimitUpdate(TInstant::Now());

    Profiler_.AddFuncGauge("/total_limit", MakeStrong(this), [this] {
        auto guard = Guard(Lock_);
        return Config_->TotalLimit;
    });
}

IThroughputThrottlerPtr TFairThrottler::CreateBucketThrottler(
    const TString& name,
    TFairThrottlerBucketConfigPtr config)
{
    if (!config) {
        config = New<TFairThrottlerBucketConfig>();
    }

    auto guard = Guard(Lock_);
    if (auto it = Buckets_.find(name); it != Buckets_.end()) {
        it->second.Throttler->SetLimited(config->Limit || config->RelativeLimit);

        it->second.Config = std::move(config);
        it->second.Throttler->SetDistributionPeriod(Config_->DistributionPeriod);
        return it->second.Throttler;
    }

    auto throttler = New<TBucketThrottler>(
        Logger.WithTag("Bucket: %v", name),
        Profiler_.WithTag("bucket", name),
        SharedBucket_,
        Config_);

    throttler->SetLimited(config->Limit || config->RelativeLimit);

    IIPCBucketPtr ipc;
    if (IPC_) {
        ipc = IPC_->AddBucket();
    }

    Buckets_[name] = TBucket{
        .Config = std::move(config),
        .Throttler = throttler,
        .IPC = ipc,
    };

    return throttler;
}

void TFairThrottler::Reconfigure(
    TFairThrottlerConfigPtr config,
    const THashMap<TString, TFairThrottlerBucketConfigPtr>& buckets)
{
    for (const auto& [name, config] : buckets) {
        CreateBucketThrottler(name, config);
    }

    auto guard = Guard(Lock_);
    Config_ = std::move(config);
}

void TFairThrottler::DoUpdateLeader()
{
    auto guard = Guard(Lock_);

    std::vector<double> weights;
    weights.reserve(Buckets_.size());

    std::vector<std::optional<i64>> limits;
    limits.reserve(Buckets_.size());

    std::vector<i64> demands;
    demands.reserve(Buckets_.size());

    std::vector<TBucketThrottler::TBucketState> states;
    states.reserve(Buckets_.size());

    THashMap<TString, i64> bucketDemands;
    for (const auto& [name, bucket] : Buckets_) {
        auto state = bucket.Throttler->Peek();

        weights.push_back(bucket.Config->Weight);

        if (auto limit = bucket.Config->GetLimit(Config_->TotalLimit)) {
            limits.push_back(*limit * Config_->DistributionPeriod.SecondsFloat());
        } else {
            limits.emplace_back();
        }

        auto guarantee = bucket.Config->GetGuarantee(Config_->TotalLimit);
        auto demand = state.Usage + state.Overdraft + state.QueueSize;
        if (guarantee && *guarantee > demand) {
            demand = *guarantee;
        }

        demands.push_back(demand);

        bucketDemands[name] = demands.back();
        states.push_back(state);
    }

    std::vector<IIPCBucketPtr> remoteBuckets;
    if (IPC_) {
        remoteBuckets = IPC_->ListBuckets();

        for (const auto& remote : remoteBuckets) {
            auto state = remote->State();

            weights.push_back(state->Weight.load());

            if (auto limit = state->Limit.load(); limit != -1) {
                limits.push_back(limit);
            } else {
                limits.emplace_back();
            }

            demands.push_back(state->Demand.load());
        }
    }

    auto tickLimit = i64(Config_->TotalLimit * Config_->DistributionPeriod.SecondsFloat());
    auto tickIncome = ComputeFairDistribution(tickLimit, weights, demands, limits);

    // Distribute remaining quota according to weights and limits.
    auto freeQuota = tickLimit - std::accumulate(tickIncome.begin(), tickIncome.end(), i64(0));
    for (int i = 0; i < std::ssize(tickIncome); i++) {
        demands[i] = freeQuota;

        if (limits[i]) {
            (*limits[i]) -= tickIncome[i];
        }
    }
    auto freeIncome = ComputeFairDistribution(freeQuota, weights, demands, limits);

    THashMap<TString, i64> bucketUsage;
    THashMap<TString, i64> bucketIncome;
    THashMap<TString, i64> bucketQuota;

    i64 leakedQuota = 0;
    int i = 0;
    for (const auto& [name, bucket] : Buckets_) {
        auto state = states[i];
        auto newLimit = tickIncome[i] + freeIncome[i];

        bucketUsage[name] = state.Usage;
        bucketQuota[name] = state.Quota;
        bucketIncome[name] = newLimit;

        leakedQuota += bucket.Throttler->Refill(newLimit);

        ++i;
    }

    for (const auto& remote : remoteBuckets) {
        auto state = remote->State();

        state->InFlow += tickIncome[i] + freeIncome[i];

        leakedQuota += state->OutFlow.exchange(0);

        ++i;
    }

    if (Buckets_.empty() && remoteBuckets.empty()) {
        leakedQuota = tickLimit;
    }

    i64 droppedQuota = SharedBucket_->Limit.Increment(leakedQuota);

    RefillFromSharedBucket();

    YT_LOG_DEBUG("Fair throttler tick (SharedBucket: %v, TickLimit: %v, FreeQuota: %v, DroppedQuota: %v)",
        SharedBucket_->Limit.Value->load(),
        tickLimit, // How many bytes was distributed?
        freeQuota, // How many bytes was left unconsumed?
        droppedQuota);

    YT_LOG_DEBUG("Fair throttler tick details (BucketIncome: %v, BucketUsage: %v, BucketDemands: %v, BucketQuota: %v)",
        bucketIncome,
        bucketUsage,
        bucketDemands,
        bucketQuota);
}

void TFairThrottler::DoUpdateFollower()
{
    auto guard = Guard(Lock_);

    THashMap<TString, i64> bucketIncome;
    THashMap<TString, i64> bucketUsage;
    THashMap<TString, i64> bucketDemands;
    THashMap<TString, i64> bucketQuota;

    i64 inFlow = 0;
    i64 outFlow = 0;

    for (const auto& [name, bucket] : Buckets_) {
        auto ipc = bucket.IPC->State();

        ipc->Weight = bucket.Config->Weight;
        if (auto limit = bucket.Config->GetLimit(Config_->TotalLimit)) {
            ipc->Limit = *limit;
        } else {
            ipc->Limit = -1;
        }

        auto state = bucket.Throttler->Peek();
        auto guarantee = bucket.Config->GetGuarantee(Config_->TotalLimit);
        auto demand = state.Usage + state.Overdraft + state.QueueSize;
        if (guarantee && *guarantee > demand) {
            demand = *guarantee;
        }

        ipc->Demand = demand;
        bucketDemands[name] = demand;

        auto in = ipc->InFlow.exchange(0);
        auto out = bucket.Throttler->Refill(in);
        ipc->OutFlow += out;

        bucketIncome[name] = in;
        bucketUsage[name] = state.Usage;
        bucketQuota[name] = state.Quota;

        inFlow += in;
        outFlow += out;
    }

    RefillFromSharedBucket();

    YT_LOG_DEBUG("Fair throttler tick (SharedBucket: %v, InFlow: %v, OutFlow: %v)",
        SharedBucket_->Limit.Value->load(),
        inFlow,
        outFlow);

    YT_LOG_DEBUG("Fair throttler tick details (BucketIncome: %v, BucketUsage: %v, BucketDemands: %v, BucketQuota: %v)",
        bucketIncome,
        bucketUsage,
        bucketDemands,
        bucketQuota);
}

void TFairThrottler::RefillFromSharedBucket()
{
    std::vector<TBucketThrottlerPtr> throttlers;
    for (const auto& [name, bucket] : Buckets_) {
        throttlers.push_back(bucket.Throttler);
    }
    Shuffle(throttlers.begin(), throttlers.end());

    for (const auto& throttler : throttlers) {
        auto limit = SharedBucket_->Limit.Value->load();
        if (limit <= 0) {
            break;
        }

        if (throttler->IsLimited()) {
            continue;
        }

        *SharedBucket_->Limit.Value -= limit - throttler->SatisfyRequests(limit);
    }
}

void TFairThrottler::UpdateLimits(TInstant at)
{
    if (!IsLeader_ && IPC_->TryLock()) {
        IsLeader_ = true;

        YT_LOG_DEBUG("Throttler is leader");
    }

    if (IsLeader_) {
        DoUpdateLeader();
    } else {
        DoUpdateFollower();
    }

    auto guard = Guard(Lock_);
    ScheduleLimitUpdate(at + Config_->DistributionPeriod);
}

void TFairThrottler::ScheduleLimitUpdate(TInstant at)
{
    TDelayedExecutor::Submit(
        BIND(&TFairThrottler::UpdateLimits, MakeWeak(this), at),
        at);
}

std::vector<i64> TFairThrottler::ComputeFairDistribution(
    i64 totalLimit,
    const std::vector<double>& weights,
    const std::vector<i64>& demands,
    const std::vector<std::optional<i64>>& limits)
{
    YT_VERIFY(weights.size() == demands.size() && weights.size() == limits.size());

    const auto& Logger = ConcurrencyLogger;

    if (weights.empty()) {
        return {};
    }

    std::vector<std::pair<double, int>> queue;
    for (int i = 0; i < std::ssize(weights); ++i) {
        queue.emplace_back(Min(demands[i], limits[i].value_or(Max<i64>())) / weights[i], i);
    }
    std::sort(queue.begin(), queue.end());

    std::vector<i64> totalLimits;
    totalLimits.resize(weights.size());

    double remainingWeight = std::accumulate(weights.begin(), weights.end(), 0.0);
    i64 remainingCapacity = totalLimit;

    int i = 0;
    for ( ; i < std::ssize(weights); ++i) {
        auto [targetShare, targetIndex] = queue[i];

        YT_LOG_TRACE("Examining bucket (Index: %v, TargetShare: %v, RemainingWeight: %v, RemainingCapacity: %v)",
            targetIndex,
            targetShare,
            remainingWeight,
            remainingCapacity);

        if (targetShare * remainingWeight >= static_cast<double>(remainingCapacity)) {
            break;
        }

        totalLimits[targetIndex] = Min(demands[targetIndex], limits[targetIndex].value_or(Max<i64>()));
        remainingCapacity -= totalLimits[targetIndex];
        remainingWeight -= weights[targetIndex];

        YT_LOG_TRACE("Satisfied demand (Index: %v, Capacity: %v)",
            targetIndex,
            totalLimits[targetIndex]);
    }

    auto finalShare = Max<double>(remainingCapacity, 0l) / Max(remainingWeight, 0.01);
    for (int j = i; j < std::ssize(weights); j++) {
        auto bucketIndex = queue[j].second;
        totalLimits[bucketIndex] = weights[bucketIndex] * finalShare;

        YT_LOG_TRACE("Distributed remains (Index: %v, Capacity: %v)",
            bucketIndex,
            totalLimits[bucketIndex]);
    }

    return totalLimits;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

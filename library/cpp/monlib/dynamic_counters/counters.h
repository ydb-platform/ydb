#pragma once

#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <library/cpp/threading/light_rw_lock/lightrwlock.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/cast.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>
#include <util/system/rwlock.h>

#include <functional>

namespace NMonitoring {
    struct TCounterForPtr;
    struct TDynamicCounters;
    struct ICountableConsumer;


    struct TCountableBase: public TAtomicRefCount<TCountableBase> {
        // Private means that the object must not be serialized unless the consumer
        // has explicitly specified this by setting its Visibility to Private.
        //
        // Works only for the methods that accept ICountableConsumer
        enum class EVisibility: ui8 {
            Unspecified,
            Public,
            Private,
        };

        virtual ~TCountableBase() {
        }

        virtual void Accept(
            const TString& labelName, const TString& labelValue,
            ICountableConsumer& consumer) const = 0;

        virtual EVisibility Visibility() const {
            return Visibility_;
        }

    protected:
        EVisibility Visibility_{EVisibility::Unspecified};
    };

    inline bool IsVisible(TCountableBase::EVisibility myLevel, TCountableBase::EVisibility consumerLevel) {
        if (myLevel == TCountableBase::EVisibility::Private
            && consumerLevel != TCountableBase::EVisibility::Private) {

            return false;
        }

        return true;
    }

    struct ICountableConsumer {
        virtual ~ICountableConsumer() {
        }

        virtual void OnCounter(
            const TString& labelName, const TString& labelValue,
            const TCounterForPtr* counter) = 0;

        virtual void OnHistogram(
            const TString& labelName, const TString& labelValue,
            IHistogramSnapshotPtr snapshot, bool derivative) = 0;

        virtual void OnGroupBegin(
            const TString& labelName, const TString& labelValue,
            const TDynamicCounters* group) = 0;

        virtual void OnGroupEnd(
            const TString& labelName, const TString& labelValue,
            const TDynamicCounters* group) = 0;

        virtual TCountableBase::EVisibility Visibility() const {
            return TCountableBase::EVisibility::Unspecified;
        }
    };

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4522) // multiple assignment operators specified
#endif                          // _MSC_VER

    struct TCounterForPtr: public TDeprecatedCounter, public TCountableBase {
        TCounterForPtr(bool derivative = false, EVisibility vis = EVisibility::Public)
            : TDeprecatedCounter(0ULL, derivative)
        {
            Visibility_ = vis;
        }

        TCounterForPtr(const TCounterForPtr&) = delete;
        TCounterForPtr& operator=(const TCounterForPtr& other) = delete;

        void Accept(
            const TString& labelName, const TString& labelValue,
            ICountableConsumer& consumer) const override {
            if (IsVisible(Visibility(), consumer.Visibility())) {
                consumer.OnCounter(labelName, labelValue, this);
            }
        }

        TCountableBase::EVisibility Visibility() const override {
            return Visibility_;
        }

        using TDeprecatedCounter::operator++;
        using TDeprecatedCounter::operator--;
        using TDeprecatedCounter::operator+=;
        using TDeprecatedCounter::operator-=;
        using TDeprecatedCounter::operator=;
        using TDeprecatedCounter::operator!;
    };

    struct TExpiringCounter: public TCounterForPtr {
        explicit TExpiringCounter(bool derivative = false, EVisibility vis = EVisibility::Public)
            : TCounterForPtr{derivative}
        {
            Visibility_ = vis;
        }

        void Reset() {
            TDeprecatedCounter::operator=(0);
        }
    };

    struct THistogramCounter: public TCountableBase {
        explicit THistogramCounter(
            IHistogramCollectorPtr collector, bool derivative = true, EVisibility vis = EVisibility::Public)
            : Collector_(std::move(collector))
            , Derivative_(derivative)
        {
            Visibility_ = vis;
        }

        void Collect(i64 value) {
            Collector_->Collect(value);
        }

        void Collect(i64 value, ui64 count) {
            Collector_->Collect(value, count);
        }

        void Collect(double value, ui64 count) {
            Collector_->Collect(value, count);
        }

        void Collect(const IHistogramSnapshot& snapshot) {
            Collector_->Collect(snapshot);
        }

        void Accept(
            const TString& labelName, const TString& labelValue,
            ICountableConsumer& consumer) const override
        {
            if (IsVisible(Visibility(), consumer.Visibility())) {
                consumer.OnHistogram(labelName, labelValue, Collector_->Snapshot(), Derivative_);
            }
        }

        void Reset() {
            Collector_->Reset();
        }

        IHistogramSnapshotPtr Snapshot() const {
            return Collector_->Snapshot();
        }

    private:
        IHistogramCollectorPtr Collector_;
        bool Derivative_;
    };

    struct TExpiringHistogramCounter: public THistogramCounter {
        using THistogramCounter::THistogramCounter;
    };

    using THistogramPtr = TIntrusivePtr<THistogramCounter>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

    struct TDynamicCounters;

    typedef TIntrusivePtr<TDynamicCounters> TDynamicCounterPtr;
    struct TDynamicCounters: public TCountableBase {
    public:
        using TCounterPtr = TIntrusivePtr<TCounterForPtr>;
        using TOnLookupPtr = void (*)(const char *methodName, const TString &name, const TString &value);

    private:
        TRWMutex Lock;
        TCounterPtr LookupCounter; // Counts lookups by name
        TOnLookupPtr OnLookup = nullptr; // Called on each lookup if not nullptr, intended for lightweight tracing.

        typedef TIntrusivePtr<TCountableBase> TCountablePtr;

        struct TChildId {
            TString LabelName;
            TString LabelValue;
            TChildId() {
            }
            TChildId(const TString& labelName, const TString& labelValue)
                : LabelName(labelName)
                , LabelValue(labelValue)
            {
            }
            auto AsTuple() const {
                return std::make_tuple(std::cref(LabelName), std::cref(LabelValue));
            }
            friend bool operator <(const TChildId& x, const TChildId& y) {
                return x.AsTuple() < y.AsTuple();
            }
            friend bool operator ==(const TChildId& x, const TChildId& y) {
                return x.AsTuple() == y.AsTuple();
            }
            friend bool operator !=(const TChildId& x, const TChildId& y) {
                return x.AsTuple() != y.AsTuple();
            }
        };

        using TCounters = TMap<TChildId, TCountablePtr>;
        using TLabels = TVector<TChildId>;

        /// XXX: hack for deferred removal of expired counters. Remove once Output* functions are not used for serialization
        mutable TCounters Counters;
        mutable TAtomic ExpiringCount = 0;

    public:
        TDynamicCounters(TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        TDynamicCounters(const TDynamicCounters *origin)
            : LookupCounter(origin->LookupCounter)
            , OnLookup(origin->OnLookup)
        {}

        ~TDynamicCounters() override;

        // This counter allows to track lookups by name within the whole subtree
        void SetLookupCounter(TCounterPtr lookupCounter) {
            TWriteGuard g(Lock);
            LookupCounter = lookupCounter;
        }

        void SetOnLookup(TOnLookupPtr onLookup) {
            TWriteGuard g(Lock);
            OnLookup = onLookup;
        }

        TWriteGuard LockForUpdate(const char *method, const TString& name, const TString& value) {
            auto res = TWriteGuard(Lock);
            if (LookupCounter) {
                ++*LookupCounter;
            }
            if (OnLookup) {
                OnLookup(method, name, value);
            }
            return res;
        }

        TStackVec<TCounters::value_type, 256> ReadSnapshot() const {
            RemoveExpired();
            TReadGuard g(Lock);
            TStackVec<TCounters::value_type, 256> items(Counters.begin(), Counters.end());
            return items;
        }

        TCounterPtr GetCounter(
            const TString& value,
            bool derivative = false,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        TCounterPtr GetNamedCounter(
            const TString& name,
            const TString& value,
            bool derivative = false,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        THistogramPtr GetHistogram(
            const TString& value,
            IHistogramCollectorPtr collector,
            bool derivative = true,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        THistogramPtr GetNamedHistogram(
            const TString& name,
            const TString& value,
            IHistogramCollectorPtr collector,
            bool derivative = true,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        // These counters will be automatically removed from the registry
        // when last reference to the counter expires.
        TCounterPtr GetExpiringCounter(
            const TString& value,
            bool derivative = false,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        TCounterPtr GetExpiringNamedCounter(
            const TString& name,
            const TString& value,
            bool derivative = false,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        THistogramPtr GetExpiringHistogram(
            const TString& value,
            IHistogramCollectorPtr collector,
            bool derivative = true,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        THistogramPtr GetExpiringNamedHistogram(
            const TString& name,
            const TString& value,
            IHistogramCollectorPtr collector,
            bool derivative = true,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public);

        TCounterPtr FindCounter(const TString& value) const;
        TCounterPtr FindNamedCounter(const TString& name, const TString& value) const;

        THistogramPtr FindHistogram(const TString& value) const;
        THistogramPtr FindNamedHistogram(const TString& name,const TString& value) const;

        void RemoveCounter(const TString &value);
        bool RemoveNamedCounter(const TString& name, const TString &value);
        void RemoveSubgroupChain(const std::vector<std::pair<TString, TString>>& chain);

        TIntrusivePtr<TDynamicCounters> GetSubgroup(const TString& name, const TString& value);
        TIntrusivePtr<TDynamicCounters> FindSubgroup(const TString& name, const TString& value) const;
        bool RemoveSubgroup(const TString& name, const TString& value);
        void ReplaceSubgroup(const TString& name, const TString& value, TIntrusivePtr<TDynamicCounters> subgroup);

        // Move all counters from specified subgroup and remove the subgroup.
        void MergeWithSubgroup(const TString& name, const TString& value);
        // Recursively reset all/deriv counters to 0.
        void ResetCounters(bool derivOnly = false);

        void RegisterSubgroup(const TString& name,
            const TString& value,
            TIntrusivePtr<TDynamicCounters> subgroup);

        void OutputHtml(IOutputStream& os) const;
        void EnumerateSubgroups(const std::function<void(const TString& name, const TString& value)>& output) const;

        // mostly for debugging purposes -- use accept with encoder instead
        void OutputPlainText(IOutputStream& os, const TString& indent = "") const;

        void Accept(
            const TString& labelName, const TString& labelValue,
            ICountableConsumer& consumer) const override;

    private:
        TCounters Resign() {
            TCounters counters;
            TWriteGuard g(Lock);
            Counters.swap(counters);
            return counters;
        }

        void RegisterCountable(const TString& name, const TString& value, TCountablePtr countable);
        void RemoveExpired() const;

        template <bool expiring, class TCounterType, class... TArgs>
        TCountablePtr GetNamedCounterImpl(const TString& name, const TString& value, TArgs&&... args);

        template <class TCounterType>
        TCountablePtr FindNamedCounterImpl(const TString& name, const TString& value) const;
    };

}

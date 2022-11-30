#pragma once

#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/string/strip.h>
#include <util/system/rwlock.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/unistat/idl/stats.pb.h>

#include <functional>

#include "types.h"

/*
   Agregator of Search Statistics
   see https://wiki.yandex-team.ru/jandekspoisk/sepe/monitoring/stat-handle
   see https://st.yandex-team.ru/SEARCH-948
*/

namespace NUnistat {
    struct TPriority {
        constexpr explicit TPriority(int priority)
            : Priority(priority)
        {
        }
        const int Priority;
    };

    struct TStartValue {
        constexpr explicit TStartValue(double startValue)
            : StartValue(startValue)
        {
        }

        const double StartValue;
    };

    class IHole {
    public:
        virtual int GetPriority() const = 0;
        virtual TString GetName() const = 0;
        virtual TString GetDescription() const = 0;
        virtual TString GetTagsStr() const = 0;
        virtual TMap<TString, TString> GetTags() const = 0;
        virtual void PrintValue(NJsonWriter::TBuf& json, bool check = true) const = 0;
        virtual void PrintInfo(NJsonWriter::TBuf& json) const = 0;
        virtual void PrintToPush(NJsonWriter::TBuf& json, bool check = true) const = 0;
        virtual void ExportToProto(TInstanceStats& stats) const = 0;
        virtual void PushSignal(double signal) = 0;
        virtual void ResetSignal() = 0;
        virtual void AddTag(const TString& tagName, const TString& tagValue) = 0;
        virtual ~IHole() = default;
    };

    class TBaseHole: public IHole {
    public:
        void AddTag(const TString& tagName, const TString& tagValue) override {
            TWriteGuard guard(Mutex);
            Tags[tagName] = tagValue;
            RebuildTagString();
        }

    protected:
        TBaseHole(const TString& name, const TString& description, const TString& suffix, int priority)
            : Name(name)
            , Description(description)
            , Suffix(suffix)
            , Priority(priority)

        {
            ParseTagsFromName(name);
        }

        int GetPriority() const override {
            return Priority;
        }

        TString GetName() const override {
            return Name;
        }

        TString GetDescription() const override {
            return Description;
        }

        TString GetTagsStr() const override {
            TReadGuard guard(Mutex);
            return TagsJoined;
        }

        TMap<TString, TString> GetTags() const override {
            TReadGuard guard(Mutex);
            return Tags;
        }

        TString Name;
        const TString Description;
        const TString Suffix;
        int Priority = 1;

        TRWMutex Mutex;
        TMap<TString, TString> Tags;
        TString TagsJoined;

    private:
        void RebuildTagString() {
            TagsJoined.clear();
            TStringOutput ss{TagsJoined};
            for (const auto& tag : Tags) {
                ss << tag.first << "=" << tag.second << ";";
            }
        }

        void ParseTagsFromName(TStringBuf name) {
            TStringBuf tmp = name;
            while (TStringBuf tag = tmp.NextTok(';')) {
                TStringBuf name, value;
                if (tag.TrySplit('=', name, value)) {
                    TStringBuf nameStr = StripString(name);
                    TStringBuf valueStr = StripString(value);
                    if (nameStr) {
                        Tags[ToString(nameStr)] = ToString(valueStr);
                    }
                }
                else {
                    Name = ToString(StripString(tag));
                }
            }

            RebuildTagString();
        }
    };

    class TTypedHole: public TBaseHole {
    protected:
        TTypedHole(const TString& name,
                   const TString& description,
                   const TString& suffix,
                   EAggregationType type,
                   int priority,
                   bool alwaysVisible = false)
            : TBaseHole(name, description, suffix, priority)
            , Type(type)
            , Pushed(alwaysVisible)
        {
        }

        const EAggregationType Type;
        TAtomic Pushed;
    };

    class TFloatHole: public TTypedHole {
    public:
        TFloatHole(const TString& name,
                   const TString& description,
                   const TString& suffix,
                   EAggregationType type,
                   int priority,
                   double startValue,
                   bool alwaysVisible)
            : TTypedHole(name, description, suffix, type, priority, alwaysVisible)
        {
            Value.Value = startValue;
        }

        void PrintValue(NJsonWriter::TBuf& json, bool check = true) const override;
        void PrintToPush(NJsonWriter::TBuf& json, bool check) const override;
        void PrintInfo(NJsonWriter::TBuf& json) const override;
        void ExportToProto(TInstanceStats& stats) const override;

        void PushSignal(double signal) override;
        virtual void ResetSignal() override;

    private:
        union TValue {
            double Value;
            TAtomic Atomic;
        };

        TValue Value;
    };

    using TIntervals = TVector<double>;

    const size_t MAX_HISTOGRAM_SIZE = 50;

    //
    // Examples:
    // Append(7).AppendFlat(2, 3) -> {7, 10, 13}
    // FlatRange(4, 1, 5).FlatRange(3, 5, 20) -> {1, 2, 3, 4, 5, 10, 15}
    // ExpRange(4, 1, 16).ExpRange(3, 16, 1024) -> {1, 2, 4, 8, 16, 64, 256}
    //
    class TIntervalsBuilder {
    public:
        // Append single point
        TIntervalsBuilder& Append(double value);

        // Appends @count points at equal intervals:
        // [last + step, last + 2 * step, ..., last + count * step]
        TIntervalsBuilder& AppendFlat(size_t count, double step);

        // Appends @count points at exponential intervals:
        // [last * base, last * base^2, ..., last * base^count]
        TIntervalsBuilder& AppendExp(size_t count, double base);

        // Adds @count points at equal intervals, @stop is not included:
        // [start, start + d, start + 2d, ..., start + count * d = stop)
        TIntervalsBuilder& FlatRange(size_t count, double start, double stop);

        // Adds @count points at exponential intervals, @stop is not included:
        // [start, start * q, start * q^2, ..., start * q^count = stop)
        TIntervalsBuilder& ExpRange(size_t count, double start, double stop);

        TIntervals Build();
    private:
        TIntervals Intervals_;
    };

    class THistogramHole: public TTypedHole {
    public:
        THistogramHole(const TString& name, const TString& description, const TString& suffix, EAggregationType type, int priority, TIntervals intervals, bool alwaysVisible)
            : TTypedHole(name, description, suffix, type, priority, alwaysVisible)
            , Intervals(std::move(intervals))
        {
            Weights.resize(Intervals.size());
        }

        void PrintValue(NJsonWriter::TBuf& json, bool check = true) const override;
        void PrintToPush(NJsonWriter::TBuf& json, bool check) const override;
        void PrintInfo(NJsonWriter::TBuf& json) const override;
        void ExportToProto(TInstanceStats& stats) const override;

        void PushSignal(double signal) override;
        virtual void ResetSignal() override;

        // for tests only
        void SetWeight(ui32 index, TAtomicBase value);

    private:
        void PrintWeights(NJsonWriter::TBuf& json) const;

        const TIntervals Intervals;
        TVector<TAtomicBase> Weights;
    };

    struct THolePriorityComparator {
        bool operator()(IHole* lhs, IHole* rhs) const {
            // Priorities are sorted in descending order, tags and names in ascending order.
            auto lp = lhs->GetPriority();
            auto rp = rhs->GetPriority();
            if (lp != rp) {
                return lp > rp;
            }

            const auto& lt = lhs->GetTagsStr();
            const auto& rt = rhs->GetTagsStr();
            int cmp = lt.compare(rt);
            if (cmp) {
                return cmp < 0;
            }

            return lhs->GetName() < rhs->GetName();
        }
    };

    using IHolePtr = TAtomicSharedPtr<NUnistat::IHole>;
    using TTags = TMap<TString, TString>;
}

class TUnistat {
public:
    static TUnistat& Instance() {
        return *Singleton<TUnistat>();
    }

    NUnistat::IHolePtr DrillFloatHole(const TString& name,
                                      const TString& description,
                                      const TString& suffix,
                                      NUnistat::TPriority priority,
                                      NUnistat::TStartValue startValue = NUnistat::TStartValue(0),
                                      EAggregationType type = EAggregationType::Sum,
                                      bool alwaysVisible = false);

    NUnistat::IHolePtr DrillFloatHole(const TString& name,
                                      const TString& suffix,
                                      NUnistat::TPriority priority,
                                      NUnistat::TStartValue startValue = NUnistat::TStartValue(0),
                                      EAggregationType type = EAggregationType::Sum,
                                      bool alwaysVisible = false);


    NUnistat::IHolePtr DrillHistogramHole(const TString& name,
                                          const TString& description,
                                          const TString& suffix,
                                          NUnistat::TPriority priority,
                                          const NUnistat::TIntervals& intervals,
                                          EAggregationType type = EAggregationType::HostHistogram,
                                          bool alwaysVisible = false);

    NUnistat::IHolePtr DrillHistogramHole(const TString& name,
                                          const TString& suffix,
                                          NUnistat::TPriority priority,
                                          const NUnistat::TIntervals& intervals,
                                          EAggregationType type = EAggregationType::HostHistogram,
                                          bool alwaysVisible = false);

    void AddGlobalTag(const TString& tagName, const TString& tagValue);

    /*It called Unsafe, because assumed, that all holes are initilized before
      first usage. Underlying hash is not locked, when invoked */
    template <typename T>
    bool PushSignalUnsafe(const T& holename, double signal) {
        return PushSignalUnsafeImpl(GetHolename(holename), signal);
    }

    template <typename T>
    TMaybe<TInstanceStats::TMetric> GetSignalValueUnsafe(const T& holename) {
        return GetSignalValueUnsafeImpl(GetHolename(holename));
    }

    template <typename T>
    bool ResetSignalUnsafe(const T& holename) {
        return ResetSignalUnsafeImpl(GetHolename(holename));
    }

    TString CreateJsonDump(int level, bool allHoles = true) const;
    TString CreatePushDump(int level, const NUnistat::TTags& tags = NUnistat::TTags(), ui32 ttl = 0, bool allHoles = false) const;
    TString CreateInfoDump(int level) const;
    void ExportToProto(TInstanceStats& stats, int level) const;
    TString GetSignalDescriptions() const;
    TVector<TString> GetHolenames() const;

    /* Erase hole from TUnistat internal state
       returns false if name wasn't found */
    bool EraseHole(const TString& name);

    void Reset();
    void ResetSignals();

private:
    template <typename T>
    std::enable_if_t<std::is_same<T, TStringBuf>::value ||
                         std::is_same<T, TString>::value ||
                         std::is_same<T, char*>::value ||
                         std::is_same<T, const char*>::value,
                     const T&>
    GetHolename(const T& holename) {
        return holename;
    }

    template <typename T>
    std::enable_if_t<std::is_enum<T>::value, TString>
    GetHolename(const T holename) {
        return ToString(holename);
    }

    template <typename T>
    std::enable_if_t<(std::is_same<char, std::remove_all_extents_t<T>>::value &&
                      std::is_array<T>::value && std::rank<T>::value == 1 && std::extent<T>::value > 1),
                     TStringBuf>
    GetHolename(const T& holename) {
        return {holename, std::extent<T>::value - 1};
    }

    bool PushSignalUnsafeImpl(const TStringBuf holename, double signal);
    TMaybe<TInstanceStats::TMetric> GetSignalValueUnsafeImpl(const TStringBuf holename);
    bool ResetSignalUnsafeImpl(const TStringBuf holename);

private:
    TRWMutex Mutex;
    THashMap<TString, NUnistat::IHolePtr> Holes;
    TSet<NUnistat::IHole*, NUnistat::THolePriorityComparator> HolesByPriorityAndTags;
    TMap<TString, TString> GlobalTags;
};

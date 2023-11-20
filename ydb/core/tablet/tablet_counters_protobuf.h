#pragma once

#include "tablet_counters.h"
#include "tablet_counters_aggregator.h"
#include <ydb/core/tablet_flat/defs.h>
#include <util/string/vector.h>
#include <util/string/split.h>

namespace NKikimr {

namespace NAux {

// Class that incapsulates protobuf options parsing for app counters
template <const NProtoBuf::EnumDescriptor* AppCountersDesc()>
struct TAppParsedOpts {
public:
    const size_t Size;
protected:
    TVector<TString> NamesStrings;
    TVector<const char*> Names;
    TVector<TVector<TTabletPercentileCounter::TRangeDef>> Ranges;
    TVector<TTabletPercentileCounter::TRangeDef> AppGlobalRanges;
    TVector<bool> Integral;
public:
    explicit TAppParsedOpts(const size_t diff = 0)
        : Size(AppCountersDesc()->value_count() + diff)
    {
        const NProtoBuf::EnumDescriptor* appDesc = AppCountersDesc();
        NamesStrings.reserve(Size);
        Names.reserve(Size);
        Ranges.reserve(Size);

        // Parse protobuf options for enum values for app counters
        for (int i = 0; i < appDesc->value_count(); i++) {
            const NProtoBuf::EnumValueDescriptor* vdesc = appDesc->value(i);
            Y_ABORT_UNLESS(vdesc->number() == vdesc->index(), "counter '%s' number (%d) != index (%d)",
                   vdesc->full_name().c_str(), vdesc->number(), vdesc->index());
            if (!vdesc->options().HasExtension(CounterOpts)) {
                NamesStrings.emplace_back(); // empty name
                Ranges.emplace_back(); // empty ranges
                Integral.push_back(false);
                continue;
            }
            const TCounterOptions& co = vdesc->options().GetExtension(CounterOpts);
            TString cntName = co.GetName();
            Y_ABORT_UNLESS(!cntName.empty(), "counter '%s' number (%d) cannot have an empty counter name",
                    vdesc->full_name().c_str(), vdesc->number());
            TString nameString;
            if (IsHistogramAggregateSimpleName(cntName)) {
                nameString = cntName;
            } else {
                nameString = GetFilePrefix(appDesc->file()) + cntName;
            }
            NamesStrings.emplace_back(nameString);
            Ranges.push_back(ParseRanges(co));
            Integral.push_back(co.GetIntegral());
        }

        // Make plain strings out of Strokas to fullfil interface of TTabletCountersBase
        for (const TString& s : NamesStrings) {
            Names.push_back(s.empty() ? nullptr : s.c_str());
        }

        // Parse protobuf options for enums itself
        AppGlobalRanges = ParseRanges(appDesc->options().GetExtension(GlobalCounterOpts));
    }
    virtual ~TAppParsedOpts()
    {}

    const char* const * GetNames() const
    {
        return Names.begin();
    }

    virtual const TVector<TTabletPercentileCounter::TRangeDef>& GetRanges(size_t idx) const
    {
        Y_ABORT_UNLESS(idx < Size);
        if (!Ranges[idx].empty()) {
            return Ranges[idx];
        } else {
            if (!AppGlobalRanges.empty())
                return AppGlobalRanges;
        }
        Y_ABORT("Ranges for percentile counter '%s' are not defined", AppCountersDesc()->value(idx)->full_name().c_str());
    }

    virtual bool GetIntegral(size_t idx) const {
        Y_ABORT_UNLESS(idx < Size);
        return Integral[idx];
    }

protected:
    TString GetFilePrefix(const NProtoBuf::FileDescriptor* desc) {
        if (desc->options().HasExtension(TabletTypeName)) {
            return desc->options().GetExtension(TabletTypeName) + "/";
        } else {
            return TString();
        }
    }

    TVector<TTabletPercentileCounter::TRangeDef> ParseRanges(const TCounterOptions& co)
    {
        TVector<TTabletPercentileCounter::TRangeDef> ranges;
        ranges.reserve(co.RangesSize());
        for (size_t j = 0; j < co.RangesSize(); j++) {
            const TRange& r = co.GetRanges(j);
            ranges.push_back(TTabletPercentileCounter::TRangeDef{r.GetValue(), r.GetName().c_str()});
        }
        return ranges;
    }
};

// Class that incapsulates protobuf options parsing for tx types and app counters
template <const NProtoBuf::EnumDescriptor* AppCountersDesc(),
          const NProtoBuf::EnumDescriptor* TxCountersDesc(),
          const NProtoBuf::EnumDescriptor* TxTypesDesc()>
struct TParsedOpts : public TAppParsedOpts<AppCountersDesc> {
typedef TAppParsedOpts<AppCountersDesc> TBase;
public:
    const size_t TxOffset;
    const size_t TxCountersSize;
    using TBase::Size;
private:
    using TBase::NamesStrings;
    using TBase::Names;
    using TBase::Ranges;
    using TBase::Integral;
    using TBase::AppGlobalRanges;
    TVector<TTabletPercentileCounter::TRangeDef> TxGlobalRanges;
public:
    TParsedOpts()
        : TAppParsedOpts<AppCountersDesc>(TxCountersDesc()->value_count() * TxTypesDesc()->value_count())
        , TxOffset(AppCountersDesc()->value_count())
        , TxCountersSize(TxCountersDesc()->value_count())
    {
        const NProtoBuf::EnumDescriptor* txDesc = TxCountersDesc();
        const NProtoBuf::EnumDescriptor* typesDesc = TxTypesDesc();

        // Parse protobuf options for enum values for tx counters
        // Create a group of tx counters for each tx type
        for (int j = 0; j < typesDesc->value_count(); j++) {
            const NProtoBuf::EnumValueDescriptor* tt = typesDesc->value(j);
            TTxType txType = tt->number();
            Y_ABORT_UNLESS((int)txType == tt->index(), "tx type '%s' number (%d) != index (%d)",
                   tt->full_name().c_str(), txType, tt->index());
            Y_ABORT_UNLESS(tt->options().HasExtension(TxTypeOpts), "tx type '%s' number (%d) is missing TxTypeOpts",
                    tt->full_name().c_str(), txType);
            const TTxTypeOptions& tto = tt->options().GetExtension(TxTypeOpts);
            TString txPrefix = tto.GetName() + "/";
            for (int i = 0; i < txDesc->value_count(); i++) {
                const NProtoBuf::EnumValueDescriptor* v = txDesc->value(i);
                Y_ABORT_UNLESS(v->number() == v->index(), "counter '%s' number (%d) != index (%d)",
                       v->full_name().c_str(), v->number(), v->index());
                if (!v->options().HasExtension(CounterOpts)) {
                    NamesStrings.emplace_back(); // empty name
                    Ranges.emplace_back(); // empty ranges
                    Integral.push_back(false);
                    continue;
                }
                const TCounterOptions& co = v->options().GetExtension(CounterOpts);
                Y_ABORT_UNLESS(!co.GetName().empty(), "counter '%s' number (%d) has an empty name",
                        v->full_name().c_str(), v->number());
                TVector<TTabletPercentileCounter::TRangeDef> ranges = TBase::ParseRanges(co);
                NamesStrings.push_back(TBase::GetFilePrefix(typesDesc->file()) + txPrefix + co.GetName());
                Ranges.push_back(TBase::ParseRanges(co));
                Integral.push_back(co.GetIntegral());
            }
        }
        // Make plain strings out of Strokas to fullfil interface of TTabletCountersBase
        for (size_t i = TxOffset; i < Size; ++i) {
            const TString& s = NamesStrings[i];
            Names.push_back(s.empty() ? nullptr : s.c_str());
        }

        // Parse protobuf options for enums itself
        TxGlobalRanges = TBase::ParseRanges(txDesc->options().GetExtension(GlobalCounterOpts));
    }

    virtual ~TParsedOpts()
    {}

    virtual const TVector<TTabletPercentileCounter::TRangeDef>& GetRanges(size_t idx) const
    {
        Y_ABORT_UNLESS(idx < Size);
        if (!Ranges[idx].empty()) {
            return Ranges[idx];
        } else {
            if (idx < TxOffset) {
                if (!AppGlobalRanges.empty())
                    return AppGlobalRanges;
            } else if (!TxGlobalRanges.empty()) {
                return TxGlobalRanges;
            }
        }
        if (idx < TxOffset) {
            Y_ABORT("Ranges for percentile counter '%s' are not defined", AppCountersDesc()->value(idx)->full_name().c_str());
        } else {
            size_t idx2 = (idx - TxOffset) % TxCountersSize;
            Y_ABORT("Ranges for percentile counter '%s' are not defined", TxCountersDesc()->value(idx2)->full_name().c_str());
        }
    }
};


template <class T1, class T2>
struct TParsedOptsPair {
private:
    T1 Opts1;
    T2 Opts2;
    TVector<const char*> Names;
public:
    const size_t Size;
public:
    TParsedOptsPair()
        : Opts1()
        , Opts2()
        , Size(Opts1.Size + Opts2.Size)
    {
        Names.reserve(Size);
        for (size_t i = 0; i < Opts1.Size; ++i) {
            Names.push_back(Opts1.GetNames()[i]);
        }
        for (size_t i = 0; i < Opts2.Size; ++i) {
            Names.push_back(Opts2.GetNames()[i]);
        }
    }

    const char* const * GetNames() const
    {
        return Names.begin();
    }

    const TVector<TTabletPercentileCounter::TRangeDef>& GetRanges(size_t idx) const
    {
        Y_ABORT_UNLESS(idx < Size);
        if (idx < Opts1.Size)
            return Opts1.GetRanges(idx);
        return Opts2.GetRanges(idx - Opts1.Size);
    }
};

template <const NProtoBuf::EnumDescriptor* AppCountersDesc(),
          const NProtoBuf::EnumDescriptor* TxCountersDesc(),
          const NProtoBuf::EnumDescriptor* TxTypesDesc()>
TParsedOpts<AppCountersDesc, TxCountersDesc, TxTypesDesc>* GetOpts() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TParsedOpts<AppCountersDesc, TxCountersDesc, TxTypesDesc>>();
}

template <const NProtoBuf::EnumDescriptor* AppCountersDesc()>
TAppParsedOpts<AppCountersDesc>* GetAppOpts() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TAppParsedOpts<AppCountersDesc>>();
}

template <class T1, class T2>
TParsedOptsPair<T1,T2>* GetOptsPair() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TParsedOptsPair<T1,T2>>();

}


// Class that incapsulates protobuf options parsing for user counters
template <const NProtoBuf::EnumDescriptor* LabeledCountersDesc()>
struct TLabeledCounterParsedOpts {
public:
    const size_t Size;
protected:
    TVector<TString> NamesStrings;
    TVector<const char*> Names;
    TVector<TString> SVNamesStrings;
    TVector<const char*> SVNames;
    TVector<ui8> AggregateFuncs;
    TVector<ui8> Types;
    TVector<TString> GroupNamesStrings;
    TVector<const char*> GroupNames;
public:
    explicit TLabeledCounterParsedOpts()
        : Size(LabeledCountersDesc()->value_count())
    {
        const NProtoBuf::EnumDescriptor* labeledCounterDesc = LabeledCountersDesc();
        NamesStrings.reserve(Size);
        Names.reserve(Size);
        SVNamesStrings.reserve(Size);
        SVNames.reserve(Size);
        AggregateFuncs.reserve(Size);
        Types.reserve(Size);

        // Parse protobuf options for enum values for app counters
        for (ui32 i = 0; i < Size; ++i) {
            const NProtoBuf::EnumValueDescriptor* vdesc = labeledCounterDesc->value(i);
            Y_ABORT_UNLESS(vdesc->number() == vdesc->index(), "counter '%s' number (%d) != index (%d)",
                   vdesc->full_name().data(), vdesc->number(), vdesc->index());
            const TLabeledCounterOptions& co = vdesc->options().GetExtension(LabeledCounterOpts);

            NamesStrings.push_back(GetFilePrefix(labeledCounterDesc->file()) + co.GetName());
            SVNamesStrings.push_back(co.GetSVName());
            AggregateFuncs.push_back(co.GetAggrFunc());
            Types.push_back(co.GetType());
        }

        // Make plain strings out of Strokas to fullfil interface of TTabletCountersBase
        std::transform(NamesStrings.begin(), NamesStrings.end(),
                  std::back_inserter(Names), [](auto& string) { return string.data(); } );

        std::transform(SVNamesStrings.begin(), SVNamesStrings.end(),
                  std::back_inserter(SVNames), [](auto& string) { return string.data(); } );

        //parse types for counter groups;
        const TLabeledCounterGroupNamesOptions& gn = labeledCounterDesc->options().GetExtension(GlobalGroupNamesOpts);
        ui32 size = gn.NamesSize();
        GroupNamesStrings.reserve(size);
        GroupNames.reserve(size);
        for (ui32 i = 0; i < size; ++i) {
            GroupNamesStrings.push_back(gn.GetNames(i));
        }

        std::transform(GroupNamesStrings.begin(), GroupNamesStrings.end(),
                  std::back_inserter(GroupNames), [](auto& string) { return string.data(); } );

    }
    virtual ~TLabeledCounterParsedOpts()
    {}

    const char* const * GetNames() const
    {
        return Names.begin();
    }

    const char* const * GetSVNames() const
    {
        return SVNames.begin();
    }

    const ui8* GetCounterTypes() const
    {
        return Types.begin();
    }

    const char* const * GetGroupNames() const
    {
        return GroupNames.begin();
    }

    size_t GetGroupNamesSize() const
    {
        return GroupNames.size();
    }

    const ui8* GetAggregateFuncs() const
    {
        return AggregateFuncs.begin();
    }

protected:
    TString GetFilePrefix(const NProtoBuf::FileDescriptor* desc) {
        if (desc->options().HasExtension(TabletTypeName)) {
            return desc->options().GetExtension(TabletTypeName) + "/";
        } else {
            return TString();
        }
    }
};

template <const NProtoBuf::EnumDescriptor* LabeledCountersDesc()>
TLabeledCounterParsedOpts<LabeledCountersDesc>* GetLabeledCounterOpts() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TLabeledCounterParsedOpts<LabeledCountersDesc>>();
}

} // NAux

// Base class for all tablet counters classes with tx type counters
// (Needed just to distinguish them in executor code using dynamic_cast)
class TTabletCountersWithTxTypes : public TTabletCountersBase {
protected:
    enum ECounterType {
        CT_SIMPLE,
        CT_CUMULATIVE,
        CT_PERCENTILE,
        CT_MAX
    };
    size_t Size[CT_MAX];
    size_t TxOffset[CT_MAX];
    size_t TxCountersSize[CT_MAX];
public:
    TTabletCountersWithTxTypes() {}

    template <class... TArgs>
    explicit TTabletCountersWithTxTypes(TArgs... args)
        : TTabletCountersBase(args...)
    {}

    TTabletSimpleCounter& TxSimple(TTxType txType, ui32 txCounter) {
        return Simple()[IndexOf<CT_SIMPLE>(txType, txCounter)];
    }

    const TTabletSimpleCounter& TxSimple(TTxType txType, ui32 txCounter) const {
        return Simple()[IndexOf<CT_SIMPLE>(txType, txCounter)];
    }

    TTabletCumulativeCounter& TxCumulative(TTxType txType, ui32 txCounter) {
        return Cumulative()[IndexOf<CT_CUMULATIVE>(txType, txCounter)];
    }

    const TTabletCumulativeCounter& TxCumulative(TTxType txType, ui32 txCounter) const {
        return Cumulative()[IndexOf<CT_CUMULATIVE>(txType, txCounter)];
    }

    TTabletPercentileCounter& TxPercentile(TTxType txType, ui32 txCounter) {
        return Percentile()[IndexOf<CT_PERCENTILE>(txType, txCounter)];
    }

    const TTabletPercentileCounter& TxPercentile(TTxType txType, ui32 txCounter) const {
        return Percentile()[IndexOf<CT_PERCENTILE>(txType, txCounter)];
    }
protected:
    template <ECounterType counterType>
    size_t IndexOf(TTxType txType, ui32 txCounter) const {
        // Note that enum values are used only inside a process, not on disc/messages
        // so there are no backward compatibility issues
        Y_ABORT_UNLESS(txCounter < TxCountersSize[counterType]);
        size_t ret = TxOffset[counterType] + txType * TxCountersSize[counterType] + txCounter;
        Y_ABORT_UNLESS(ret < Size[counterType]);
        return ret;
    }
};

// Tablet counters with app counters (SimpleDesc, CumulativeDesc, PercentileDesc) and counters per each tx type (TxTypeDesc)
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc(),
          const NProtoBuf::EnumDescriptor* TxTypeDesc()>
class TProtobufTabletCounters : public TTabletCountersWithTxTypes {
public:
    typedef NAux::TParsedOpts<SimpleDesc, ETxTypeSimpleCounters_descriptor, TxTypeDesc> TSimpleOpts;
    typedef NAux::TParsedOpts<CumulativeDesc, ETxTypeCumulativeCounters_descriptor, TxTypeDesc> TCumulativeOpts;
    typedef NAux::TParsedOpts<PercentileDesc, ETxTypePercentileCounters_descriptor, TxTypeDesc> TPercentileOpts;

    static TSimpleOpts* SimpleOpts() {
        return NAux::GetOpts<SimpleDesc, ETxTypeSimpleCounters_descriptor, TxTypeDesc>();
    }

    static TCumulativeOpts* CumulativeOpts() {
        return NAux::GetOpts<CumulativeDesc, ETxTypeCumulativeCounters_descriptor, TxTypeDesc>();
    }

    static TPercentileOpts* PercentileOpts() {
        return NAux::GetOpts<PercentileDesc, ETxTypePercentileCounters_descriptor, TxTypeDesc>();
    }

    TProtobufTabletCounters()
        : TTabletCountersWithTxTypes(
              SimpleOpts()->Size,       CumulativeOpts()->Size,       PercentileOpts()->Size,
              SimpleOpts()->GetNames(), CumulativeOpts()->GetNames(), PercentileOpts()->GetNames()
        )
    {
        FillOffsets();
        InitCounters();
    }

    //constructor from external counters
    TProtobufTabletCounters(const ui32 simpleOffset, const ui32 cumulativeOffset, const ui32 percentileOffset, TTabletCountersBase* counters)
        : TTabletCountersWithTxTypes(simpleOffset, cumulativeOffset, percentileOffset, counters)
    {
        FillOffsets();
        InitCounters();
    }

private:
    void FillOffsets()
    {
        // Initialize stuff for counter addressing
        Size[CT_SIMPLE] = SimpleOpts()->Size;
        TxOffset[CT_SIMPLE] = SimpleOpts()->TxOffset;
        TxCountersSize[CT_SIMPLE] = SimpleOpts()->TxCountersSize;
        Size[CT_CUMULATIVE] = CumulativeOpts()->Size;
        TxOffset[CT_CUMULATIVE] = CumulativeOpts()->TxOffset;
        TxCountersSize[CT_CUMULATIVE] = CumulativeOpts()->TxCountersSize;
        Size[CT_PERCENTILE] = PercentileOpts()->Size;
        TxOffset[CT_PERCENTILE] = PercentileOpts()->TxOffset;
        TxCountersSize[CT_PERCENTILE] = PercentileOpts()->TxCountersSize;
    }

    void InitCounters()
    {
        // Initialize percentile counters
        const auto* opts = PercentileOpts();
        for (size_t i = 0; i < opts->Size; i++) {
            if (!opts->GetNames()[i]) {
                continue;
            }
            const auto& vec = opts->GetRanges(i);
            Percentile()[i].Initialize(vec.size(), vec.begin(), opts->GetIntegral(i));
        }
    }
};

// Tablet counters with app counters (SimpleDesc, CumulativeDesc, PercentileDesc) only
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc()>
class TAppProtobufTabletCounters : public TTabletCountersBase {
public:
    typedef NAux::TAppParsedOpts<SimpleDesc> TSimpleOpts;
    typedef NAux::TAppParsedOpts<CumulativeDesc> TCumulativeOpts;
    typedef NAux::TAppParsedOpts<PercentileDesc> TPercentileOpts;

    static TSimpleOpts* SimpleOpts() {
        return NAux::GetAppOpts<SimpleDesc>();
    }

    static TCumulativeOpts* CumulativeOpts() {
        return NAux::GetAppOpts<CumulativeDesc>();
    }

    static TPercentileOpts* PercentileOpts() {
        return NAux::GetAppOpts<PercentileDesc>();
    }

    TAppProtobufTabletCounters()
        : TTabletCountersBase(
              SimpleOpts()->Size,       CumulativeOpts()->Size,       PercentileOpts()->Size,
              SimpleOpts()->GetNames(), CumulativeOpts()->GetNames(), PercentileOpts()->GetNames()
        )
    {
        InitCounters();
    }

    //constructor from external counters
    TAppProtobufTabletCounters(const ui32 simpleOffset, const ui32 cumulativeOffset, const ui32 percentileOffset, TTabletCountersBase* counters)
        : TTabletCountersBase(simpleOffset, cumulativeOffset, percentileOffset, counters)
    {
        InitCounters();
    }

private:
    void InitCounters()
    {
        // Initialize percentile counters
        const auto* opts = PercentileOpts();
        for (size_t i = 0; i < opts->Size; i++) {
            if (!opts->GetNames()[i]) {
                continue;
            }
            const auto& vec = opts->GetRanges(i);
            Percentile()[i].Initialize(vec.size(), vec.begin(), opts->GetIntegral(i));
        }
    }
};


// Will store all counters for both types in T1 and itself. It's mean that
// FirstTabletCounters will be of type T1, but as base class (TTabletCountersBase) will contail ALL COUNTERS from T1 and T2.
// T1 and T2 can be obtained with GetFirstTabletCounters and GetSecondTabletCounters() methods, and counters can be changed separetly.
// T1 object and TProtobufTabletCountersPair itself will contail all couters with all changes.
// Of course, T1 and T2 are not thread safe - they must be accessed only from one thread both.
// You can construct Pair<T1, Pair<T2,T3>> and so on if you need it.
template <class T1, class T2>
class TProtobufTabletCountersPair : public TTabletCountersBase {
private:
    TAutoPtr<T1> FirstTabletCounters;
    TAutoPtr<T2> SecondTabletCounters;

public:
    typedef NAux::TParsedOptsPair<typename T1::TSimpleOpts, typename T2::TSimpleOpts> TSimpleOpts;
    typedef NAux::TParsedOptsPair<typename T1::TCumulativeOpts, typename T2::TCumulativeOpts> TCumulativeOpts;
    typedef NAux::TParsedOptsPair<typename T1::TPercentileOpts, typename T2::TPercentileOpts> TPercentileOpts;


    static TSimpleOpts* SimpleOpts() {
        return NAux::GetOptsPair<typename T1::TSimpleOpts, typename T2::TSimpleOpts>();
    }

    static TCumulativeOpts* CumulativeOpts() {
        return NAux::GetOptsPair<typename T1::TCumulativeOpts, typename T2::TCumulativeOpts>();
    }

    static TPercentileOpts* PercentileOpts() {
        return NAux::GetOptsPair<typename T1::TPercentileOpts, typename T2::TPercentileOpts>();
    }

    TProtobufTabletCountersPair()
        : TTabletCountersBase(
              SimpleOpts()->Size,       CumulativeOpts()->Size,       PercentileOpts()->Size,
              SimpleOpts()->GetNames(), CumulativeOpts()->GetNames(), PercentileOpts()->GetNames()
          )
        , FirstTabletCounters(new T1(0, 0, 0, dynamic_cast<TTabletCountersBase*>(this)))
        , SecondTabletCounters(new T2(T1::SimpleOpts()->Size, T1::CumulativeOpts()->Size, T1::PercentileOpts()->Size,
                               dynamic_cast<TTabletCountersBase*>(this)))
    {
    }

    //constructor from external counters
    TProtobufTabletCountersPair(const ui32 simpleOffset, const ui32 cumulativeOffset, const ui32 percentileOffset, TTabletCountersBase* counters)
        : TTabletCountersBase(simpleOffset, cumulativeOffset, percentileOffset, counters)
        , FirstTabletCounters(new T1(0, 0, 0, dynamic_cast<TTabletCountersBase*>(this)))
        , SecondTabletCounters(new T2(T1::SimpleOpts()->Size, T1::CumulativeOpts()->Size, T1::PercentileOpts()->Size,
                               dynamic_cast<TTabletCountersBase*>(this)))
    {
    }


    TAutoPtr<T1>& GetFirstTabletCounters()
    {
        return FirstTabletCounters;
    }

    const TAutoPtr<T1>& GetFirstTabletCounters() const
    {
        return FirstTabletCounters;
    }

    TAutoPtr<T2>& GetSecondTabletCounters()
    {
        return SecondTabletCounters;
    }

    const TAutoPtr<T2>& GetSecondTabletCounters() const
    {
        return SecondTabletCounters;
    }
};



// Tablet app user counters
template <const NProtoBuf::EnumDescriptor* SimpleDesc()>
class TProtobufTabletLabeledCounters : public TTabletLabeledCountersBase {
public:
    typedef NAux::TLabeledCounterParsedOpts<SimpleDesc> TLabeledCounterOpts;

    static TLabeledCounterOpts* SimpleOpts() {
        return NAux::GetLabeledCounterOpts<SimpleDesc>();
    }

    TProtobufTabletLabeledCounters(const TString& group, const ui64 id)
        : TTabletLabeledCountersBase(
              SimpleOpts()->Size, SimpleOpts()->GetNames(), SimpleOpts()->GetCounterTypes(),
              SimpleOpts()->GetAggregateFuncs(), group, SimpleOpts()->GetGroupNames(), id, Nothing())
    {
        TVector<TString> groups;
        StringSplitter(group).Split('/').SkipEmpty().Collect(&groups);

        Y_ABORT_UNLESS(SimpleOpts()->GetGroupNamesSize() == groups.size());
    }

    TProtobufTabletLabeledCounters(const TString& group, const ui64 id,
                                   const TString& databasePath)
        : TTabletLabeledCountersBase(
              SimpleOpts()->Size, SimpleOpts()->GetSVNames(), SimpleOpts()->GetCounterTypes(),
              SimpleOpts()->GetAggregateFuncs(), group, SimpleOpts()->GetGroupNames(), id, databasePath)
    {
        TVector<TString> groups;
        StringSplitter(group).Split('|').Collect(&groups);

        Y_ABORT_UNLESS(SimpleOpts()->GetGroupNamesSize() == groups.size());
    }
};



} // end of NKikimr

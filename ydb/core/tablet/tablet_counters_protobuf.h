#pragma once

#include "tablet_counters.h"
#include "tablet_counters_aggregator.h"
#include <ydb/core/tablet_flat/defs.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/string/vector.h>

namespace NKikimr {

namespace NAux {

/**
 * The holder for parsed application counter definitions from .proto files.
 * The type-independent part of TAppParsedOpts.
 */
struct TAppParsedOptsBase {
public:
    const size_t Size;
protected:
    const NProtoBuf::EnumDescriptor* const AppDesc;
    TVector<TString> NamesStrings;
    TVector<const char*> Names;
    TVector<TVector<TTabletPercentileCounter::TRangeDef>> Ranges;
    TVector<TTabletPercentileCounter::TRangeDef> AppGlobalRanges;
    TVector<bool> Integral;

    TVector<bool> LeaderOnly;

    /**
     * The list of source counters for each enum value.
     *
     * @note Each entry is guaranteed to be not empty, if ParseSourceCounters is true.
     */
    TVector<TVector<TSourceCounter>> SourceCounters;

public:
    /**
     * @param appDesc The enum description to parse
     * @param parseSourceCounters Indicates whether to parse the SourceCounters fields
     * @param diff The number of additional counters reserved after the app counters
     */
    TAppParsedOptsBase(const NProtoBuf::EnumDescriptor* appDesc, bool parseSourceCounters, size_t diff);
    virtual ~TAppParsedOptsBase();

    const char* const * GetNames() const
    {
        return Names.data();
    }

    virtual const TVector<TTabletPercentileCounter::TRangeDef>& GetRanges(size_t idx) const;

    virtual bool GetIntegral(size_t idx) const;

    /**
     * @return Whether the counter at idx is meaningful only on leaders
     *         (TCounterOptions::LeaderOnly, step 09.5)
     */
    virtual bool GetLeaderOnly(size_t idx) const;

protected:
    static TString GetFilePrefix(const NProtoBuf::FileDescriptor* desc);

    static TVector<TTabletPercentileCounter::TRangeDef> ParseRanges(const TCounterOptions& co);
};

/**
 * The holder for parsed application counter definitions from .proto files.
 *
 * @tparam AppCountersDesc The function, which returns the enum description to parse
 * @tparam ParseSourceCounters Indicates whether to parse the SourceCounters fields
 */
template <
    const NProtoBuf::EnumDescriptor* AppCountersDesc(),
    bool ParseSourceCounters = false
>
struct TAppParsedOpts : public TAppParsedOptsBase {
public:
    explicit TAppParsedOpts(const size_t diff = 0)
        : TAppParsedOptsBase(AppCountersDesc(), ParseSourceCounters, diff)
    {}

    /**
     * Return the source counters for the given enum index.
     *
     * @warning This function can be called only if ParseSourceCounters is set.
     *
     * @param index The enum index for which to retrieve the source counters
     *
     * @return The corresponding source counters
     */
    const TVector<TSourceCounter>& GetSourceCounters(size_t index) const {
        Y_ABORT_UNLESS(ParseSourceCounters);
        Y_ABORT_UNLESS(index < Size);

        return SourceCounters[index];
    }
};

// The type-independent part of TParsedOpts
struct TParsedOptsBase : public TAppParsedOptsBase {
typedef TAppParsedOptsBase TBase;
public:
    const size_t TxOffset;
    const size_t TxCountersSize;
    using TBase::Size;
private:
    const NProtoBuf::EnumDescriptor* const TxDesc;
    TVector<TTabletPercentileCounter::TRangeDef> TxGlobalRanges;
public:
    TParsedOptsBase(const NProtoBuf::EnumDescriptor* appDesc,
                    const NProtoBuf::EnumDescriptor* txDesc,
                    const NProtoBuf::EnumDescriptor* typesDesc);

    virtual ~TParsedOptsBase();

    const TVector<TTabletPercentileCounter::TRangeDef>& GetRanges(size_t idx) const override;
};

// Class that incapsulates protobuf options parsing for tx types and app counters
template <const NProtoBuf::EnumDescriptor* AppCountersDesc(),
          const NProtoBuf::EnumDescriptor* TxCountersDesc(),
          const NProtoBuf::EnumDescriptor* TxTypesDesc()>
struct TParsedOpts : public TParsedOptsBase {
public:
    TParsedOpts()
        : TParsedOptsBase(AppCountersDesc(), TxCountersDesc(), TxTypesDesc())
    {}
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

/**
 * Create the singleton, which holds the parsed options for the given counters enum.
 *
 * @tparam AppCountersDesc The function, which returns the enum description to parse
 * @tparam ParseSourceCounters Indicates whether to parse the SourceCounters fields
 *
 * @return The singleton with the parsed options for the given counters enum
 */
template <
    const NProtoBuf::EnumDescriptor* AppCountersDesc(),
    bool ParseSourceCounters = false
>
TAppParsedOpts<AppCountersDesc, ParseSourceCounters>* GetAppOpts() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TAppParsedOpts<AppCountersDesc, ParseSourceCounters>>();
}

template <class T1, class T2>
TParsedOptsPair<T1,T2>* GetOptsPair() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TParsedOptsPair<T1,T2>>();

}


// Class that incapsulates protobuf options parsing for user counters
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
    TString Groups;
public:
    explicit TLabeledCounterParsedOpts(const NProtoBuf::EnumDescriptor* labeledCountersDesc);

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

    const TString& GetGroups() const
    {
        return Groups;
    }

    const TVector<ui8>& GetTypes() const
    {
        return Types;
    }
};

template <const NProtoBuf::EnumDescriptor* LabeledCountersDesc()>
struct TLabeledCounterParsedOptsProvider {
    TLabeledCounterParsedOptsProvider()
        : ParsedOpts(LabeledCountersDesc())
    {
    }

    TLabeledCounterParsedOpts ParsedOpts;

    TLabeledCounterParsedOpts* Get() {
        return &ParsedOpts;
    }
};

template<const NProtoBuf::EnumDescriptor* LabeledCountersDesc()>
TLabeledCounterParsedOpts* GetLabeledCounterOpts() {
    // Use singleton to avoid thread-safety issues and parse enum descriptor once
    return Singleton<TLabeledCounterParsedOptsProvider<LabeledCountersDesc>>()->Get();
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

void VerifyGroups(const NAux::TLabeledCounterParsedOpts* simpleOpts, const TString& group, const char delimiter);
void VerifyGroupsSkipEmpty(const NAux::TLabeledCounterParsedOpts* simpleOpts, const TString& group, const char delimiter);

// Tablet app user counters
template <const NProtoBuf::EnumDescriptor* SimpleDesc()>
TTabletLabeledCountersBase CreateProtobufTabletLabeledCounters(TMaybe<TString> databasePath = Nothing(), const ui64 id = 0) {
    const auto* simpleOpts = NAux::GetLabeledCounterOpts<SimpleDesc>();
    return TTabletLabeledCountersBase(simpleOpts->Size, simpleOpts->GetSVNames(), simpleOpts->GetCounterTypes(),
            simpleOpts->GetAggregateFuncs(), simpleOpts->GetGroups(), simpleOpts->GetGroupNames(),
             id, databasePath);
}

template <const NProtoBuf::EnumDescriptor* SimpleDesc()>
TTabletLabeledCountersBase CreateProtobufTabletLabeledCounters(const TString& group, const ui64 id) {
    const auto* simpleOpts = NAux::GetLabeledCounterOpts<SimpleDesc>();

    VerifyGroupsSkipEmpty(simpleOpts, group, '/');

    return TTabletLabeledCountersBase(simpleOpts->Size, simpleOpts->GetNames(), simpleOpts->GetCounterTypes(),
        simpleOpts->GetAggregateFuncs(), group, simpleOpts->GetGroupNames(), id, Nothing());
}

template <const NProtoBuf::EnumDescriptor* SimpleDesc()>
TTabletLabeledCountersBase CreateProtobufTabletLabeledCounters(const TString& group, const ui64 id, const TString& databasePath) {
    const auto* simpleOpts = NAux::GetLabeledCounterOpts<SimpleDesc>();

    VerifyGroups(simpleOpts, group, '|');

    return TTabletLabeledCountersBase(simpleOpts->Size, simpleOpts->GetSVNames(), simpleOpts->GetCounterTypes(),
        simpleOpts->GetAggregateFuncs(), group, simpleOpts->GetGroupNames(), id, databasePath);
}

} // end of NKikimr

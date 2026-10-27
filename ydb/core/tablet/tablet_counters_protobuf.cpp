#include "tablet_counters_protobuf.h"

namespace NKikimr {

namespace {

TString GetFilePrefix(const NProtoBuf::FileDescriptor* desc) {
    if (desc->options().HasExtension(TabletTypeName)) {
        return desc->options().GetExtension(TabletTypeName) + "/";
    } else {
        return TString();
    }
}

}

namespace NAux {

TAppParsedOptsBase::TAppParsedOptsBase(const NProtoBuf::EnumDescriptor* appDesc, bool parseSourceCounters, size_t diff)
    : Size(appDesc->value_count() + diff)
    , AppDesc(appDesc)
{
    const bool ParseSourceCounters = parseSourceCounters;

    NamesStrings.reserve(Size);
    Names.reserve(Size);
    Ranges.reserve(Size);
    Integral.reserve(Size);
    LeaderOnly.reserve(Size);

    if (ParseSourceCounters) {
        SourceCounters.reserve(Size);
    }

    // Parse protobuf options for enum values for app counters
    for (int i = 0; i < appDesc->value_count(); i++) {
        const NProtoBuf::EnumValueDescriptor* vdesc = appDesc->value(i);
        Y_ABORT_UNLESS(vdesc->number() == vdesc->index(), "counter '%s' number (%d) != index (%d)",
               vdesc->full_name().c_str(), vdesc->number(), vdesc->index());
        if (!vdesc->options().HasExtension(CounterOpts)) {
            NamesStrings.emplace_back(); // empty name
            Ranges.emplace_back(); // empty ranges
            Integral.push_back(false);
            LeaderOnly.push_back(false);

            Y_ABORT_UNLESS(
                !ParseSourceCounters,
                "ParseSourceCounters is set, but the counter '%s' (value %d) is not defined using CounterOpts",
                vdesc->full_name().c_str(),
                vdesc->number()
            );

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
        LeaderOnly.push_back(co.GetLeaderOnly());

        if (ParseSourceCounters) {
            // Parse SourceCounters but make sure there is always at least one
            Y_ABORT_UNLESS(
                co.SourceCountersSize() != 0,
                "ParseSourceCounters is set, but the counter '%s' (value %d) does not define SourceCounters",
                vdesc->full_name().c_str(),
                vdesc->number()
            );

            TVector<TSourceCounter> allSourceCounters;
            allSourceCounters.reserve(co.SourceCountersSize());

            for (const auto& counter : co.GetSourceCounters()) {
                allSourceCounters.emplace_back(counter);
            }

            SourceCounters.emplace_back(std::move(allSourceCounters));
        }
    }

    // Make plain strings out of Strokas to fullfil interface of TTabletCountersBase
    for (const TString& s : NamesStrings) {
        Names.push_back(s.empty() ? nullptr : s.c_str());
    }

    // Parse protobuf options for enums itself
    AppGlobalRanges = ParseRanges(appDesc->options().GetExtension(GlobalCounterOpts));
}

TAppParsedOptsBase::~TAppParsedOptsBase()
{}

const TVector<TTabletPercentileCounter::TRangeDef>& TAppParsedOptsBase::GetRanges(size_t idx) const
{
    Y_ABORT_UNLESS(idx < Size);
    if (!Ranges[idx].empty()) {
        return Ranges[idx];
    } else {
        if (!AppGlobalRanges.empty())
            return AppGlobalRanges;
    }
    Y_ABORT("Ranges for percentile counter '%s' are not defined", AppDesc->value(idx)->full_name().c_str());
}

bool TAppParsedOptsBase::GetIntegral(size_t idx) const {
    Y_ABORT_UNLESS(idx < Size);
    return Integral[idx];
}

bool TAppParsedOptsBase::GetLeaderOnly(size_t idx) const {
    Y_ABORT_UNLESS(idx < Size);
    return LeaderOnly[idx];
}

TString TAppParsedOptsBase::GetFilePrefix(const NProtoBuf::FileDescriptor* desc) {
    return NKikimr::GetFilePrefix(desc);
}

TVector<TTabletPercentileCounter::TRangeDef> TAppParsedOptsBase::ParseRanges(const TCounterOptions& co)
{
    TVector<TTabletPercentileCounter::TRangeDef> ranges;
    ranges.reserve(co.RangesSize());
    for (size_t j = 0; j < co.RangesSize(); j++) {
        const TRange& r = co.GetRanges(j);
        ranges.push_back(TTabletPercentileCounter::TRangeDef{r.GetValue(), r.GetName().c_str()});
    }
    return ranges;
}

TParsedOptsBase::TParsedOptsBase(const NProtoBuf::EnumDescriptor* appDesc,
                                 const NProtoBuf::EnumDescriptor* txDesc,
                                 const NProtoBuf::EnumDescriptor* typesDesc)
    : TAppParsedOptsBase(appDesc, false, txDesc->value_count() * typesDesc->value_count())
    , TxOffset(appDesc->value_count())
    , TxCountersSize(txDesc->value_count())
    , TxDesc(txDesc)
{
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
                LeaderOnly.push_back(false);
                continue;
            }
            const TCounterOptions& co = v->options().GetExtension(CounterOpts);
            Y_ABORT_UNLESS(!co.GetName().empty(), "counter '%s' number (%d) has an empty name",
                    v->full_name().c_str(), v->number());
            NamesStrings.push_back(TBase::GetFilePrefix(typesDesc->file()) + txPrefix + co.GetName());
            Ranges.push_back(TBase::ParseRanges(co));
            Integral.push_back(co.GetIntegral());
            LeaderOnly.push_back(co.GetLeaderOnly());
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

TParsedOptsBase::~TParsedOptsBase()
{}

const TVector<TTabletPercentileCounter::TRangeDef>& TParsedOptsBase::GetRanges(size_t idx) const
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
        Y_ABORT("Ranges for percentile counter '%s' are not defined", AppDesc->value(idx)->full_name().c_str());
    } else {
        size_t idx2 = (idx - TxOffset) % TxCountersSize;
        Y_ABORT("Ranges for percentile counter '%s' are not defined", TxDesc->value(idx2)->full_name().c_str());
    }
}

TLabeledCounterParsedOpts::TLabeledCounterParsedOpts(const NProtoBuf::EnumDescriptor* labeledCountersDesc)
        : Size(labeledCountersDesc->value_count())
{
    NamesStrings.reserve(Size);
    Names.reserve(Size);
    SVNamesStrings.reserve(Size);
    SVNames.reserve(Size);
    AggregateFuncs.reserve(Size);
    Types.reserve(Size);

    // Parse protobuf options for enum values for app counters
    for (ui32 i = 0; i < Size; ++i) {
        const NProtoBuf::EnumValueDescriptor* vdesc = labeledCountersDesc->value(i);
        Y_ABORT_UNLESS(vdesc->number() == vdesc->index(), "counter '%s' number (%d) != index (%d)",
            vdesc->full_name().data(), vdesc->number(), vdesc->index());
        const TLabeledCounterOptions& co = vdesc->options().GetExtension(LabeledCounterOpts);

        NamesStrings.push_back(GetFilePrefix(labeledCountersDesc->file()) + co.GetName());
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
    const TLabeledCounterGroupNamesOptions& gn = labeledCountersDesc->options().GetExtension(GlobalGroupNamesOpts);
    ui32 size = gn.NamesSize();
    GroupNamesStrings.reserve(size);
    GroupNames.reserve(size);
    for (ui32 i = 0; i < size; ++i) {
        GroupNamesStrings.push_back(gn.GetNames(i));
    }

    std::transform(GroupNamesStrings.begin(), GroupNamesStrings.end(),
            std::back_inserter(GroupNames), [](auto& string) { return string.data(); } );

    Groups = JoinRange("|", GroupNamesStrings.begin(), GroupNamesStrings.end());
}

}

void VerifyGroups(const NAux::TLabeledCounterParsedOpts* simpleOpts, const TString& group, const char delimiter) {
    const size_t groups = StringSplitter(group).Split(delimiter).Count();
    Y_ABORT_UNLESS(simpleOpts->GetGroupNamesSize() == groups, "%zu != %zu; group=%s", simpleOpts->GetGroupNamesSize(), groups, group.Quote().c_str());
}

void VerifyGroupsSkipEmpty(const NAux::TLabeledCounterParsedOpts* simpleOpts, const TString& group, const char delimiter) {
    const size_t groups = StringSplitter(group).Split(delimiter).SkipEmpty().Count();
    Y_ABORT_UNLESS(simpleOpts->GetGroupNamesSize() == groups, "%zu != %zu; group=%s", simpleOpts->GetGroupNamesSize(), groups, group.Quote().c_str());
}

}

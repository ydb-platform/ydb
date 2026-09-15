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

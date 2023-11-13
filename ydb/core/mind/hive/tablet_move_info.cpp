#include "hive_impl.h"
#include <library/cpp/random_provider/random_provider.h>
#include <random>

namespace NKikimr {
namespace NHive {

THive::TTabletMoveInfo::TTabletMoveInfo(TInstant timestamp, const TTabletInfo& tablet, TNodeId from, TNodeId to) 
    : Timestamp(timestamp)
    , Tablet(tablet.GetFullTabletId())
    , From(from)
    , To(to)
    , TabletType(tablet.GetTabletType())
{
    // Priority is used to sample random moves, while prioritising system tablets
    auto& randGen = *TAppData::RandomProvider.Get();
    Priority = std::uniform_real_distribution{}(randGen);
    if (THive::IsSystemTablet(TabletType)) {
        Priority += 1;
    }
}

TString THive::TTabletMoveInfo::ToHTML() const {
    TStringBuilder str;
    str << "<tr><td>" << Timestamp << "</td><td>" << Tablet
        << "</td><td>" << From << "&rarr;" << To << "</td><tr>";
    return str;
}

std::weak_ordering THive::TTabletMoveInfo::operator<=>(const TTabletMoveInfo& other) const {
    return std::weak_order(Priority, other.Priority);
}

}
}

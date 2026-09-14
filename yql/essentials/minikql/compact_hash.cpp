#include "compact_hash.h"

#include <util/generic/map.h>
#include <util/stream/str.h>

namespace NKikimr::NCHash {

void TListPoolBase::FreeListPage(TListHeader* p) {
    Y_ASSERT(TAlignedPagePool::GetPageStart(p) == p);
    p->~TListHeader();
    PagePool_.ReturnPage(p);
}

size_t TListPoolBase::TUsedPages::PrintStat(const TStringBuf& header, IOutputStream& out) const {
    TMap<ui32, ui64> counts;
    size_t pages = 0;
    for (auto& p : FullPages) {
        ++pages;
        ++counts[p.As<TListHeader>()->ListSize];
    }
    for (auto& c : counts) {
        out << header << "Count of full pages<" << c.first << ">: " << c.second << Endl;
    }
    for (const auto& smallPage : SmallPages) {
        if (size_t size = smallPage.Size()) {
            pages += size;
            out << header << "Count of partially free pages<" << smallPage.Front()->As<TListHeader>()->ListSize << ">: " << size << Endl;
        }
    }
    for (const auto& mediumPage : MediumPages) {
        if (size_t size = mediumPage.Size()) {
            pages += size;
            out << header << "Count of partially free pages<" << mediumPage.Front()->As<TListHeader>()->ListSize << ">: " << size << Endl;
        }
    }
    return pages;
}

TString TListPoolBase::TUsedPages::DebugInfo() const {
    TStringStream out;
    TMap<ui32, ui64> counts;
    for (auto& p : FullPages) {
        out << "Full page<" << p.As<TListHeader>()->ListSize << ">: " << p.As<TListHeader>()->FreeLists << Endl;
    }
    for (const auto& smallPage : SmallPages) {
        for (auto& p : smallPage) {
            out << "Partially free page<" << p.As<TListHeader>()->ListSize << ">: " << p.As<TListHeader>()->FreeLists << Endl;
        }
    }
    for (const auto& mediumPage : MediumPages) {
        for (auto& p : mediumPage) {
            out << "Partially free page<" << p.As<TListHeader>()->ListSize << ">: " << p.As<TListHeader>()->FreeLists << Endl;
        }
    }
    return out.Str();
}

} // namespace NKikimr::NCHash

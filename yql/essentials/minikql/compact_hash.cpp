#include "compact_hash.h"

#include <util/generic/map.h>
#include <util/stream/str.h>


namespace NKikimr {

namespace NCHash {

void TListPoolBase::FreeListPage(TListHeader* p) {
    Y_ASSERT(TAlignedPagePool::GetPageStart(p) == p);
    p->~TListHeader();
    PagePool_.ReturnPage(p);
}

size_t TListPoolBase::TUsedPages::PrintStat(const TStringBuf& header, IOutputStream& out) const {
    TMap<ui32, ui64> counts;
    size_t pages = 0;
    for (auto& p: FullPages) {
        ++pages;
        ++counts[p.As<TListHeader>()->ListSize];
    }
    for (auto& c: counts) {
        out << header << "Count of full pages<" << c.first << ">: " << c.second << Endl;
    }
    for (size_t i = 0; i < SmallPages.size(); ++i) {
        if (size_t size = SmallPages[i].Size()) {
            pages += size;
            out << header << "Count of partially free pages<" << SmallPages[i].Front()->As<TListHeader>()->ListSize << ">: " << size << Endl;
        }
    }
    for (size_t i = 0; i < MediumPages.size(); ++i) {
        if (size_t size = MediumPages[i].Size()) {
            pages += size;
            out << header << "Count of partially free pages<" << MediumPages[i].Front()->As<TListHeader>()->ListSize << ">: " << size << Endl;
        }
    }
    return pages;
}

TString TListPoolBase::TUsedPages::DebugInfo() const {
    TStringStream out;
    TMap<ui32, ui64> counts;
    for (auto& p: FullPages) {
        out << "Full page<" << p.As<TListHeader>()->ListSize << ">: " << p.As<TListHeader>()->FreeLists << Endl;
    }
    for (size_t i = 0; i < SmallPages.size(); ++i) {
        for (auto& p: SmallPages[i]) {
            out << "Partially free page<" << p.As<TListHeader>()->ListSize << ">: " << p.As<TListHeader>()->FreeLists << Endl;
        }
    }
    for (size_t i = 0; i < MediumPages.size(); ++i) {
        for (auto& p: MediumPages[i]) {
            out << "Partially free page<" << p.As<TListHeader>()->ListSize << ">: " << p.As<TListHeader>()->FreeLists << Endl;
        }
    }
    return out.Str();
}

} // NCHash

} // NKikimr

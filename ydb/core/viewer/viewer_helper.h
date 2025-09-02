#pragma once
#include <util/generic/algorithm.h>

template<>
struct std::hash<NKikimr::TSubDomainKey> {
    std::size_t operator ()(const NKikimr::TSubDomainKey& s) const {
        return s.Hash();
    }
};

template <>
struct std::equal_to<NKikimrBlobStorage::TVDiskID> {
    static decltype(auto) make_tuple(const NKikimrBlobStorage::TVDiskID& id) {
        return std::make_tuple(
                    id.GetGroupID(),
                    id.GetGroupGeneration(),
                    id.GetRing(),
                    id.GetDomain(),
                    id.GetVDisk()
                    );
    }

    bool operator ()(const NKikimrBlobStorage::TVDiskID& a, const NKikimrBlobStorage::TVDiskID& b) const {
        return make_tuple(a) == make_tuple(b);
    }
};

template <>
struct std::less<NKikimrBlobStorage::TVDiskID> {
    bool operator ()(const NKikimrBlobStorage::TVDiskID& a, const NKikimrBlobStorage::TVDiskID& b) const {
        return std::equal_to<NKikimrBlobStorage::TVDiskID>::make_tuple(a) < std::equal_to<NKikimrBlobStorage::TVDiskID>::make_tuple(b);
    }
};

template <>
struct std::hash<NKikimrBlobStorage::TVDiskID> {
    size_t operator ()(const NKikimrBlobStorage::TVDiskID& a) const {
        auto tp = std::equal_to<NKikimrBlobStorage::TVDiskID>::make_tuple(a);
        return hash<decltype(tp)>()(tp);
    }
};

namespace NKikimr::NViewer {
    template<typename TCollection, typename TFunc>
    void SortCollection(TCollection& collection, TFunc&& func, bool ReverseSort = false) {
        using TItem = typename TCollection::value_type;
        bool reverse = ReverseSort;
        ::Sort(collection, [reverse, func](const TItem& a, const TItem& b) {
            auto valueA = func(a);
            auto valueB = func(b);
            return valueA == valueB ? false : reverse ^ (valueA < valueB);
        });
    }
}

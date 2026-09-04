#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/str_stl.h>

#include <memory>
#include <tuple>
#include <type_traits>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeLabels
{
    TString DiskId;
    TString CloudId;
    TString FolderId;
};

using TVolumeLabelsPtr = std::shared_ptr<TVolumeLabels>;
using TVolumeLabelsConstPtr = std::shared_ptr<const TVolumeLabels>;

inline bool operator==(const TVolumeLabels& lhs, const TVolumeLabels& rhs)
{
    return std::tie(lhs.DiskId, lhs.CloudId, lhs.FolderId) ==
           std::tie(rhs.DiskId, rhs.CloudId, rhs.FolderId);
}

inline bool operator<(const TVolumeLabels& lhs, const TVolumeLabels& rhs)
{
    return std::tie(lhs.DiskId, lhs.CloudId, lhs.FolderId) <
           std::tie(rhs.DiskId, rhs.CloudId, rhs.FolderId);
}

TVolumeLabelsPtr MakeVolumeLabels(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId);

}   // namespace NYdb::NBS::NBlockStore

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYdb::NBS::NBlockStore::TVolumeLabels>
{
    size_t operator()(const NYdb::NBS::NBlockStore::TVolumeLabels& val) const
    {
        auto a = std::tie(val.DiskId, val.CloudId, val.FolderId);
        return THash<std::decay_t<decltype(a)>>{}(a);
    }
};

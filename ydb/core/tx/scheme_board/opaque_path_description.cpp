#include "helpers.h"
#include "two_part_description.h"
#include "opaque_path_description.h"

namespace NKikimr::NSchemeBoard {

bool TOpaquePathDescription::IsEmpty() const {
    return DescribeSchemeResultSerialized.empty();
}

TString TOpaquePathDescription::ToString() const {
    // fields are reordered to be more useful to people reading logs
    return TStringBuilder() << "{"
        << "Status " << Status
        << ", Path " << Path
        << ", PathId " << PathId
        << ", PathVersion " << PathVersion
        << ", SubdomainPathId " << SubdomainPathId
        << ", PathAbandonedTenantsSchemeShards size " << PathAbandonedTenantsSchemeShards.size()
        << ", DescribeSchemeResultSerialized size " << DescribeSchemeResultSerialized.size()
        << "}"
    ;
}

TOpaquePathDescription MakeOpaquePathDescription(const TString& preSerializedPart, const NKikimrScheme::TEvDescribeSchemeResult& protoPart) {
    return TOpaquePathDescription{
        .DescribeSchemeResultSerialized = SerializeDescribeSchemeResult(preSerializedPart, protoPart),
        .Status = protoPart.GetStatus(),
        .PathId = GetPathId(protoPart),
        .Path = protoPart.GetPath(),
        .PathVersion = GetPathVersion(protoPart),
        .SubdomainPathId = GetDomainId(protoPart),
        .PathAbandonedTenantsSchemeShards = GetAbandonedSchemeShardIds(protoPart),
    };
}

TOpaquePathDescription MakeOpaquePathDescription(const TTwoPartDescription& twoPartDescription) {
    return MakeOpaquePathDescription(twoPartDescription.PreSerialized, twoPartDescription.Record);
}

}  // NKikimr::NSchemeBoard

#include "resource_profile.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {

TResourceProfiles::TResourceProfiles()
    : DefaultProfile(std::make_shared<TResourceProfile>())
{}

void TResourceProfiles::AddProfile(const TResourceProfile &profile)
{
    auto key = std::make_pair(profile.GetTabletType(), profile.GetName());
    Profiles[key] = std::make_shared<TResourceProfile>(profile);
}

const TResourceProfiles::TPtr TResourceProfiles::GetProfile(TTabletType type, const TString &name) const
{
    auto it = Profiles.find(std::make_pair(type, name));
    if (it != Profiles.end())
        return it->second;

    if (name != DefaultProfileName) {
        it = Profiles.find(std::make_pair(type, DefaultProfileName));
        if (it != Profiles.end())
            return it->second;
    }

    if (type != DefaultTabletType)
        return GetProfile(DefaultTabletType, name);

    return DefaultProfile;
}

void TResourceProfiles::StoreProfiles(::google::protobuf::RepeatedPtrField<TResourceProfile> &profiles) const
{
    for (auto &entry : Profiles)
        profiles.Add()->CopyFrom(*entry.second);
}

void TResourceProfiles::LoadProfiles(const ::google::protobuf::RepeatedPtrField<TResourceProfile> &profiles)
{
    Profiles.clear();
    for (auto &profile : profiles)
        AddProfile(profile);
}

} // namespace NKikimr

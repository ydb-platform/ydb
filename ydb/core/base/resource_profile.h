#pragma once

#include "defs.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tablet.pb.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr {

class TResourceProfiles : public TThrRefBase {
public:
    using TResourceProfile = NKikimrSchemeOp::TResourceProfile;
    using TPtr = std::shared_ptr<TResourceProfile>;

private:
    using TTabletType = NKikimrTabletBase::TTabletTypes::EType;

    using TKey = std::pair<TTabletType, TString>;
    using TProfiles = THashMap<TKey, TPtr>;

    TProfiles Profiles;
    const TPtr DefaultProfile = std::make_shared<TResourceProfile>();

    const TString DefaultProfileName = "default";
    const TTabletType DefaultTabletType = NKikimrTabletBase::TTabletTypes::Unknown;

public:
    TResourceProfiles() {}

    void AddProfile(const TResourceProfile &profile)
    {
        auto key = std::make_pair(profile.GetTabletType(), profile.GetName());
        Profiles[key] = std::make_shared<TResourceProfile>(profile);
    }

    const TPtr GetProfile(TTabletType type, const TString &name) const
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

    void StoreProfiles(::google::protobuf::RepeatedPtrField<TResourceProfile> &profiles) const {
        for (auto &entry : Profiles)
            profiles.Add()->CopyFrom(*entry.second);
    }

    void LoadProfiles(const ::google::protobuf::RepeatedPtrField<TResourceProfile> &profiles) {
        Profiles.clear();
        for (auto &profile : profiles)
            AddProfile(profile);
    }
};

using TResourceProfilesPtr = TIntrusivePtr<TResourceProfiles>;


} // namespace NKikimr

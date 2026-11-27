#pragma once

#include <ydb/core/protos/tablet.pb.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>
#include <utility>

namespace google::protobuf {
    template <typename T>
    class RepeatedPtrField;
}

namespace NKikimrSchemeOp {
    class TResourceProfile;
}

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
    const TPtr DefaultProfile;

    const TString DefaultProfileName = "default";
    const TTabletType DefaultTabletType = NKikimrTabletBase::TTabletTypes::Unknown;

public:
    TResourceProfiles();

    void AddProfile(const TResourceProfile &profile);
    const TPtr GetProfile(TTabletType type, const TString &name) const;
    void StoreProfiles(::google::protobuf::RepeatedPtrField<TResourceProfile> &profiles) const;
    void LoadProfiles(const ::google::protobuf::RepeatedPtrField<TResourceProfile> &profiles);
};

using TResourceProfilesPtr = TIntrusivePtr<TResourceProfiles>;


} // namespace NKikimr

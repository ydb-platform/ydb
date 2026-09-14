#include "group_layout_checker.h"

Y_DECLARE_OUT_SPEC(, NKikimr::NBsController::NLayoutChecker::TEntityId, stream, value) { value.Output(stream); }

namespace NKikimr::NBsController::NLayoutChecker {

TPDiskLayoutPosition::TPDiskLayoutPosition(TDomainMapper& mapper, const TNodeLocation& location,
        const std::optional<TString>& diskScope, TPDiskId pdiskId, const TGroupGeometryInfo& geom) {
    TStringStream realmGroup, realm, domain, device;
    ui32 diskScopeLevelEnd = TNodeLocation::TKeys::E::Unit + 10;
    ui32 deviceLevelEnd = diskScopeLevelEnd + 1;
    const std::pair<int, TStringStream*> levels[] = {
        {geom.GetRealmLevelBegin(), &realmGroup},
        {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelBegin()), &realm},
        {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelEnd()), &domain},
        {Max(geom.GetRealmLevelEnd(), geom.GetDomainLevelEnd(), deviceLevelEnd), &device}
    };
    auto addLevel = [&](int key, const TString& value) {
        for (const auto& [reference, stream] : levels) {
            if (key < reference) {
                Save(stream, std::make_tuple(key, value));
            }
        }
    };
    for (const auto& [key, value] : location.GetItems()) {
        addLevel(key, value);
    }
    if (diskScope) {
        addLevel(diskScopeLevelEnd, *diskScope);
    }
    addLevel(255, pdiskId.ToString()); // ephemeral level to distinguish between PDisks on the same node
    RealmGroup = mapper(realmGroup.Str());
    Realm = mapper(realm.Str());
    Domain = mapper(domain.Str());
    Device = mapper(device.Str());
}

}

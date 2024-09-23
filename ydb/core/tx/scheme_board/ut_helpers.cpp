#include "ut_helpers.h"

namespace NKikimr {
namespace NSchemeBoard {

NKikimrScheme::TEvDescribeSchemeResult GenerateDescribe(
    const TString& path,
    TPathId pathId,
    ui64 version,
    TDomainId domainId
) {
    NKikimrScheme::TEvDescribeSchemeResult describeSchemeResult;

    describeSchemeResult.SetPath(path);
    describeSchemeResult.SetPathId(pathId.LocalPathId);
    describeSchemeResult.SetPathOwnerId(pathId.OwnerId);
    describeSchemeResult.MutablePathDescription()->MutableSelf()->SetPathVersion(version);

    if (domainId) {
        auto domainKey = describeSchemeResult.MutablePathDescription()->MutableDomainDescription()->MutableDomainKey();
        domainKey->SetSchemeShard(domainId.OwnerId);
        domainKey->SetPathId(domainId.LocalPathId);
    }

    return describeSchemeResult;
}

NInternalEvents::TEvUpdate* GenerateUpdate(
    const NKikimrScheme::TEvDescribeSchemeResult& describe,
    ui64 owner,
    ui64 generation,
    bool isDeletion
) {
    const auto& pathDescription = MakeOpaquePathDescription("", describe);
    auto* update = new NInternalEvents::TEvUpdateBuilder(owner, generation, pathDescription, isDeletion);

    if (!isDeletion) {
        update->SetDescribeSchemeResultSerialized(std::move(pathDescription.DescribeSchemeResultSerialized));
    }

    return update;
}

TVector<TCombinationsArgs> GenerateCombinationsDomainRoot(TString path, ui64 gssOwnerID, TVector<ui64> tenantsOwners) {
    TVector<TCombinationsArgs> combinations;

    for (ui64 tssOwnerId: tenantsOwners) {
        const ui64 domainLocalPathId = FindIndex(tenantsOwners, tssOwnerId) + 2;
        const TPathId domainId = TPathId(gssOwnerID, domainLocalPathId);
        const TPathId tenantRoot = TPathId(tssOwnerId, 1);

        //firts
        combinations.emplace_back(
                    TCombinationsArgs{
                        path, domainId, 1, domainId,
                        gssOwnerID, 1, false});
        combinations.push_back(
                    TCombinationsArgs{
                        path, tenantRoot, 1, domainId,
                        tssOwnerId, 1, false});

        //update it
        combinations.push_back(
                    TCombinationsArgs{
                        path, domainId, 2, domainId,
                        gssOwnerID, 1, false});
        combinations.push_back(
                    TCombinationsArgs{
                        path, tenantRoot, 2, domainId,
                        tssOwnerId, 1, false});

        //delte it
        combinations.push_back(
                    TCombinationsArgs{
                        path, domainId, Max<ui64>(), domainId,
                        gssOwnerID, 1, true});
        //    fromTss.push_back( // we do not delete tenants from cache
        //        GenerateUpdate(
        //            path, tenantRoot, Max<ui64>, tenantRoot,
        //            tssOwnerId, 1, true));
    }

    return combinations;
}

TVector<TCombinationsArgs> GenerateCombinationsMigratedPath(TString path,
                                                            ui64 gssID, TVector<ui64> tssIDs,
                                                            ui64 gssLocalPathId, ui64 tssLocalPathId)
{
    TVector<TCombinationsArgs> combinations;

    TPathId domainId = TPathId(gssID, 2);
    auto migratedPath = TPathId(gssID, gssLocalPathId);

    //migratedPath from GSS
    combinations.emplace_back(
        TCombinationsArgs{
            path, migratedPath, 1, domainId,
            tssIDs[0], 1, false});

    //migratedPath from TSS
    combinations.push_back(
        TCombinationsArgs{
            path, migratedPath, 2, domainId,
            tssIDs[0], 1, false});

    //update it on TSS
    combinations.push_back(
        TCombinationsArgs{
            path, migratedPath, 3, domainId,
            tssIDs[0], 1, false});

    //remove it on TSS
    combinations.push_back(
        TCombinationsArgs{
            path, migratedPath, Max<ui64>(), domainId,
            tssIDs[0], 1, true});

    for (size_t i = 0; i < tssIDs.size(); ++i) {
        auto tssID = tssIDs[i];

        auto recreatedPath = TPathId(tssID, tssLocalPathId);

        //recreate it on TSS
        combinations.push_back(
            TCombinationsArgs{
                path, recreatedPath, 1, domainId,
                tssID, 1, false});

        //update it on TSS
        combinations.push_back(
            TCombinationsArgs{
                path, recreatedPath, 2, domainId,
                tssID, 1, false});

        //remove it on TSS
        combinations.push_back(
            TCombinationsArgs{
                path, recreatedPath, Max<ui64>(), domainId,
                tssID, 1, true});

        domainId.LocalPathId += 331;
    }

    return combinations;
}

} // NSchemeBoard
} // NKikimr

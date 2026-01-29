#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDefineDDiskPool& cmd, TStatus& /*status*/) {
        TBoxId boxId = cmd.GetBoxId();
        if (!boxId && Boxes.Get().size() == 1) {
            boxId = Boxes.Get().begin()->first;
        }

        ui64 storagePoolId = 0;
        ui64 maxPoolId = 0;
        const auto& pools = StoragePools.Get();
        for (auto it = pools.lower_bound({boxId, 0}); it != pools.end() && std::get<0>(it->first) == boxId; ++it) {
            const ui64 id = std::get<1>(it->first);
            const TStoragePoolInfo &info = it->second;

            if (info.Name == cmd.GetName()) {
                if (!id) {
                    throw TExError() << "StoragePoolId# 0 for Name# " << info.Name;
                } else if (storagePoolId) {
                    throw TExError() << "Storage pool Name# " << info.Name << " is ambiguous";
                } else {
                    storagePoolId = id;
                }
            }

            maxPoolId = Max(maxPoolId, id);
        }
        if (!storagePoolId) {
            storagePoolId = maxPoolId + 1;
        }

        const TBoxStoragePoolId id(boxId, storagePoolId);
        const ui64 nextGen = CheckGeneration(cmd, StoragePools.Get(), id);

        TStoragePoolInfo storagePool;
        storagePool.Name = cmd.GetName();
        storagePool.Generation = nextGen;
        storagePool.ErasureSpecies = TBlobStorageGroupType::ErasureNone;

        if (!cmd.HasGeometry()) {
            throw TExError() << "missing Geometry for DDisk pool definition";
        }
        const auto& geom = cmd.GetGeometry();
        storagePool.RealmLevelBegin = geom.GetRealmLevelBegin();
        storagePool.RealmLevelEnd = geom.GetRealmLevelEnd();
        storagePool.DomainLevelBegin = geom.GetDomainLevelBegin();
        storagePool.DomainLevelEnd = geom.GetDomainLevelEnd();
        storagePool.NumFailRealms = geom.GetNumFailRealms();
        storagePool.NumFailDomainsPerFailRealm = geom.GetNumFailDomainsPerFailRealm();
        storagePool.NumVDisksPerFailDomain = geom.GetNumVDisksPerFailDomain();
        storagePool.VDiskKind = NKikimrBlobStorage::TVDiskKind::Default;
        storagePool.NumGroups = cmd.GetNumDDiskGroups();
        storagePool.DefaultGroupSizeInUnits = 1; // cmd.GetDefaultGroupSizeInUnits();
        storagePool.DDisk = true;

        for (const auto &item : cmd.GetPDiskFilter()) {
            TStoragePoolInfo::TPDiskFilter filter;
            filter.BoxId = boxId;
            filter.StoragePoolId = storagePoolId;

            bool hasTypeProperty = false;

            for (const auto &property : item.GetProperty()) {
                switch (property.GetPropertyCase()) {
#define PROPERTY(NAME, PTR) \
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::k ## NAME: { \
                        if (filter.NAME) { \
                            throw TExError() << #NAME << " already set"; \
                        } else { \
                            filter.NAME = property.Get ## NAME(); \
                            if (bool *p = (PTR)) { \
                                *p = true; \
                            } \
                            continue; \
                        } \
                    }

                    PROPERTY(Type, &hasTypeProperty)
                    PROPERTY(SharedWithOs, nullptr)
                    PROPERTY(ReadCentric, nullptr)
                    PROPERTY(Kind, nullptr)

                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::PROPERTY_NOT_SET:
                        throw TExError() << "TPDiskFilter.TRequiredProperty.Property not set";
                }

                throw TExError() << "TPDiskFilter.TRequiredProperty.Property has invalid case";
            }

            if (!hasTypeProperty) {
                throw TExError() << "TPDiskFilter did not specify Type property";
            }

            storagePool.PDiskFilters.insert(std::move(filter));
        }

        //if (cmd.HasScopeId()) {
        //    const auto& pb = cmd.GetScopeId();
        //    const TScopeId scopeId(pb.GetX1(), pb.GetX2());
        //    TKikimrScopeId x(scopeId);
        //    storagePool.SchemeshardId = x.GetSchemeshardId();
        //    storagePool.PathItemId = x.GetPathItemId();
        //}

        auto& storagePools = StoragePools.Unshare();
        if (const auto [spIt, inserted] = storagePools.try_emplace(id, std::move(storagePool)); !inserted) {
            TStoragePoolInfo& cur = spIt->second;
            if (!cur.DDisk) {
                throw TExError() << "can't invoke DefineDDiskPool against storage pool";
            }
            //if (cur.SchemeshardId != storagePool.SchemeshardId || cur.PathItemId != storagePool.PathItemId) {
            //    for (auto it = r.first; it != r.second; ++it) {
            //        GroupContentChanged.insert(it->second);
            //    }
            //}
            //// retain some fields
            //storagePool.BridgeMode = cur.BridgeMode;
            cur = std::move(storagePool); // update existing storage pool
        } else {
            //// enable bridge mode by default for new pools (when bridge mode is enabled cluster-wide)
            //const bool bridgeMode = AppData()->BridgeModeEnabled;
            //Y_DEBUG_ABORT_UNLESS(bridgeMode == static_cast<bool>(BridgeInfo));
            //spIt->second.BridgeMode = bridgeMode;
        }

        Fit.PoolsAndGroups.emplace(id, std::nullopt);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadDDiskPool& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteDDiskPool& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TMoveDDisk& /*cmd*/, TStatus& /*status*/) {
        throw TExError() << "not implemented";
    }

} // NKikimr::NBsController

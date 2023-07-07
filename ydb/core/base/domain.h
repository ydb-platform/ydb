#pragma once
#include "defs.h"
#include "tabletid.h"
#include "localdb.h"

#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimrBlobStorage {
    class TDefineStoragePool;
}

namespace NKikimr {

struct TDomainsInfo : public TThrRefBase {
    static const ui32 FirstUserTag = 32;
    static const ui32 FakeRootTag = 0xFFFFE;
    static const ui32 MaxUserTag = 0xFFFFF;
    static const ui32 BadDomainId = 0xFFFFFFFFu;
    static const ui64 BadTabletId = 0xFFFFFFFFFFFFFFFFull;
    static const ui32 DomainBits = 5;
    static const ui32 MaxDomainId = (1 << DomainBits) - 1;

    // it's very sad mistake
    // MakeTabletID should be called with hiveUid == 0 for all domain's static tablets
    // but we do it with hiveUid == domain, and collision with dynamic tablets occurs
    // use AvoidBrokenUniqPartsBySystemTablets to avoid this mistake
    static ui64 MakeTxCoordinatorID(ui32 domain, ui32 uid) {
        Y_VERIFY_DEBUG(domain < 32 && uid < 256);
        const ui64 uniqPart = 0x800000 | (ui64)uid;
        return MakeTabletID(domain, domain, uniqPart);
    }

    static ui64 MakeTxCoordinatorIDFixed(ui32 domain, ui32 uid) {
        Y_VERIFY_DEBUG(domain < 32 && uid < 256);
        const ui64 uniqPart = 0x800000 | (ui64)uid;
        return MakeTabletID(domain, 0, uniqPart);
    }

    static ui64 MakeTxMediatorID(ui32 domain, ui32 uid) {
        Y_VERIFY_DEBUG(domain < 32 && uid < 256);
        const ui64 uniqPart = 0x810000 | (ui64)uid;
        return MakeTabletID(domain, domain, uniqPart);
    }

    static ui64 MakeTxMediatorIDFixed(ui32 domain, ui32 uid) {
        Y_VERIFY_DEBUG(domain < 32 && uid < 256);
        const ui64 uniqPart = 0x810000 | (ui64)uid;
        return MakeTabletID(domain, 0, uniqPart);
    }

    static ui64 MakeTxAllocatorID(ui32 domain, ui32 uid) {
        Y_VERIFY_DEBUG(domain < 32 && uid > 0 && uid < 4096);
        const ui64 uniqPart = 0x820000 | (ui64)uid;
        return MakeTabletID(domain, domain, uniqPart);
    }

    static ui64 MakeTxAllocatorIDFixed(ui32 domain, ui32 uid) {
        Y_VERIFY_DEBUG(domain < 32 && uid > 0 && uid < 4096);
        const ui64 uniqPart = 0x820000 | (ui64)uid;
        return MakeTabletID(domain, 0, uniqPart);
    }

    static constexpr const char* SystemTableDefaultPoicyName() {
        return "SystemTableDefault";
    }

    static constexpr const char* UserTableDefaultPoicyName() {
        return "UserTableDefault";
    }

    typedef THashMap<TString, TIntrusiveConstPtr<NLocalDb::TCompactionPolicy>> TNamedCompactionPolicies;

    struct TDomain : public TThrRefBase {
        using TPtr = TIntrusivePtr<TDomain>;

        using TVectorUi64 = TVector<ui64>;
        using TVectorUi32 = TVector<ui32>;
        using TStoragePoolKinds = THashMap<TString, NKikimrBlobStorage::TDefineStoragePool>;

        const ui32 DomainUid;
        const ui32 DefaultStateStorageGroup;
        const ui32 DefaultSchemeBoardGroup;
        const ui64 SchemeRoot;
        const TString Name;
        const TVector<ui64> Coordinators;
        const TVector<ui64> Mediators;
        const TVector<ui64> TxAllocators;
        const TVector<ui32> StateStorageGroups;
        const ui32 DefaultHiveUid;
        const TVector<ui32> HiveUids;
        const ui64 DomainPlanResolution;
        const TStoragePoolKinds StoragePoolTypes;

        static constexpr ui32 TimecastBucketsPerMediator = 2; // <- any sense in making this configurable? may be for debug?..

    private:
        //don't reinterpret any data
        TDomain(const TString &name, ui32 domainUid, ui64 schemeRootId,
                ui32 defaultStateStorageGroup, ui32 defaultSchemeBoardGroup,
                TVectorUi32 stateStorageGroup,
                TVectorUi64 coordinators, TVectorUi64 mediators, TVectorUi64 allocators,
                ui32 defaultHiveUid, TVectorUi32 hivesUids,
                ui64 domainPlanResolution, const TStoragePoolKinds *poolTypes);

     public:
        ~TDomain();

        //interpret coordinatorUids, mediatorUids and allocatorUids as vector uids and call proper MakeTabletId for each
        template <typename TUidsContainerUi32, typename TUidsContainerUi64>
        static TDomain::TPtr ConstructDomain(const TString &name, ui32 domainUid, ui64 schemeRoot,
                                             ui32 defaultStateStorageGroup, ui32 defaultSchemeBoardGroup,
                                            const TUidsContainerUi32 &stateStorageGroups,
                                             ui32 defaultHiveUid,const TUidsContainerUi32 &hiveUids,
                                             ui64 planResolution,
                                             const TUidsContainerUi64 &coordinatorUids,
                                             const TUidsContainerUi64 &mediatorUids,
                                             const TUidsContainerUi64 &allocatorUids,
                                             const TStoragePoolKinds &poolTypes)
        {
            return new TDomain(name, domainUid, schemeRoot,
                            defaultStateStorageGroup, defaultSchemeBoardGroup,
                            TVectorUi32(stateStorageGroups.begin(), stateStorageGroups.end()),
                            MakeCoordinatorsIds(TVectorUi64(coordinatorUids.begin(), coordinatorUids.end()), domainUid),
                            MakeMediatrosIds(TVectorUi64(mediatorUids.begin(), mediatorUids.end()), domainUid),
                            MakeAllocatorsIds(TVectorUi64(allocatorUids.begin(), allocatorUids.end()), domainUid),
                            defaultHiveUid, TVectorUi32(hiveUids.begin(), hiveUids.end()),
                            planResolution, &poolTypes);
        }

        //interpret coordinatorUids, mediatorUids and allocatorUids as vector uids and call proper MakeTabletId for each
        template <typename TUidsContainerUi32, typename TUidsContainerUi64>
        static TDomain::TPtr ConstructDomain(const TString &name, ui32 domainUid, ui64 schemeRoot,
                                             ui32 defaultStateStorageGroup, ui32 defaultSchemeBoardGroup,
                                            const TUidsContainerUi32 &stateStorageGroups,
                                             ui32 defaultHiveUid,const TUidsContainerUi32 &hiveUids,
                                             ui64 planResolution,
                                             const TUidsContainerUi64 &coordinatorUids,
                                             const TUidsContainerUi64 &mediatorUids,
                                             const TUidsContainerUi64 &allocatorUids)
        {
            return new TDomain(name, domainUid, schemeRoot,
                            defaultStateStorageGroup, defaultSchemeBoardGroup,
                            TVectorUi32(stateStorageGroups.begin(), stateStorageGroups.end()),
                            MakeCoordinatorsIds(TVectorUi64(coordinatorUids.begin(), coordinatorUids.end()), domainUid),
                            MakeMediatrosIds(TVectorUi64(mediatorUids.begin(), mediatorUids.end()), domainUid),
                            MakeAllocatorsIds(TVectorUi64(allocatorUids.begin(), allocatorUids.end()), domainUid),
                            defaultHiveUid, TVectorUi32(hiveUids.begin(), hiveUids.end()),
                            planResolution, nullptr);
        }

        //no any tablets setted
        static TDomain::TPtr ConstructEmptyDomain(const TString &name, ui32 domainId = 0)
        {
            const ui64 schemeRoot = 0;
            const ui32 stateStorageGroup = domainId;
            const ui32 defHiveUid = domainId;
            ui64 planResolution = 500;
            return new TDomain(name, domainId, schemeRoot,
                            stateStorageGroup, stateStorageGroup,
                            TVectorUi32(1, stateStorageGroup),
                            TVectorUi64(),
                            TVectorUi64(),
                            TVectorUi64(),
                            defHiveUid, TVectorUi32(1, defHiveUid),
                            planResolution, nullptr);
        }

        template <typename TUidsContainerUi32, typename TUidsContainerUi64>
        static TDomain::TPtr ConstructDomainWithExplicitTabletIds(const TString &name, ui32 domainUid, ui64 schemeRoot,
                                            ui32 defaultStateStorageGroup, ui32 defaultSchemeBoardGroup,
                                            const TUidsContainerUi32 &stateStorageGroups,
                                            ui32 defaultHiveUid,const TUidsContainerUi32 &hiveUids,
                                            ui64 planResolution,
                                            const TUidsContainerUi64 &coordinatorUids,
                                            const TUidsContainerUi64 &mediatorUids,
                                            const TUidsContainerUi64 &allocatorUids,
                                            const TStoragePoolKinds &poolTypes)
        {
            return new TDomain(name, domainUid, schemeRoot,
                            defaultStateStorageGroup, defaultSchemeBoardGroup,
                            TVectorUi32(stateStorageGroups.begin(), stateStorageGroups.end()),
                            TVectorUi64(coordinatorUids.begin(), coordinatorUids.end()),
                            TVectorUi64(mediatorUids.begin(), mediatorUids.end()),
                            TVectorUi64(allocatorUids.begin(), allocatorUids.end()),
                            defaultHiveUid, TVectorUi32(hiveUids.begin(), hiveUids.end()),
                            planResolution, &poolTypes);
        }

        template <typename TUidsContainerUi32, typename TUidsContainerUi64>
        static TDomain::TPtr ConstructDomainWithExplicitTabletIds(const TString &name, ui32 domainUid, ui64 schemeRoot,
                                            ui32 defaultStateStorageGroup, ui32 defaultSchemeBoardGroup,
                                            const TUidsContainerUi32 &stateStorageGroups,
                                            ui32 defaultHiveUid,const TUidsContainerUi32 &hiveUids,
                                            ui64 planResolution,
                                            const TUidsContainerUi64 &coordinatorUids,
                                            const TUidsContainerUi64 &mediatorUids,
                                            const TUidsContainerUi64 &allocatorUids)
        {
            return new TDomain(name, domainUid, schemeRoot,
                            defaultStateStorageGroup, defaultSchemeBoardGroup,
                            TVectorUi32(stateStorageGroups.begin(), stateStorageGroups.end()),
                            TVectorUi64(coordinatorUids.begin(), coordinatorUids.end()),
                            TVectorUi64(mediatorUids.begin(), mediatorUids.end()),
                            TVectorUi64(allocatorUids.begin(), allocatorUids.end()),
                            defaultHiveUid, TVectorUi32(hiveUids.begin(), hiveUids.end()),
                            planResolution, nullptr);
        }

        ui32 DomainRootTag() const {
            return DomainUid + FirstUserTag;
        }

        static TVector<ui64> TransformUids(TVector<ui64> &&uids, std::function<ui64 (ui32)> func) {
            TVector<ui64> result(std::move(uids));
            for (ui32 i = 0; i < result.size(); ++i) {
                result[i] = func(result[i]);
            }
            return result;
        }

        static TVector<ui64> TransformIntoVectorUids(ui32 count) {
            TVector<ui64> result;
            result.reserve(count);
            for (ui32 i = 1; i <= count; ++i) {
                result.push_back(i);
            }
            return result;
        }

        static TVector<ui64> MakeCoordinatorsIds(TVector<ui64> &&uids, ui32 domainUid) {
            return TransformUids(std::move(uids), [&domainUid](ui32 uid) { return MakeTxCoordinatorID(domainUid, uid); });
        }

        static TVector<ui64> MakeCoordinatorsIds(ui32 count, ui32 domainUid) {
            return MakeCoordinatorsIds(TransformIntoVectorUids(count), domainUid);
        }

        static TVector<ui64> MakeMediatrosIds(TVector<ui64> &&uids, ui32 domainUid) {
            return TransformUids(std::move(uids), [&domainUid](ui32 uid) { return MakeTxMediatorID(domainUid, uid); });
        }

        static TVector<ui64> MakeMediatrosIds(ui32 count, ui32 domainUid) {
            return MakeMediatrosIds(TransformIntoVectorUids(count), domainUid);
        }

        static TVector<ui64> MakeAllocatorsIds(TVector<ui64> &&uids, ui32 domainUid) {
            return TransformUids(std::move(uids), [&domainUid](ui32 uid) { return MakeTxAllocatorID(domainUid, uid); });
        }

        static TVector<ui64> MakeAllocatorsIds(ui32 count, ui32 domainUid) {
            return MakeAllocatorsIds(TransformIntoVectorUids(count), domainUid);
        }

        ui32 GetHiveUidByIdx(ui32 idx) const {
            if (idx == Max<ui32>()) {
                return DefaultHiveUid;
            }

            return HiveUids.at(idx);
        }
    };

    TMap<ui32, TIntrusivePtr<TDomain>> Domains;
    THashMap<TString, TIntrusivePtr<TDomain>> DomainByName;
    TMap<ui32, TIntrusivePtr<TDomain>> DomainByStateStorageGroup;
    TMap<ui32, TIntrusivePtr<TDomain>> DomainByHiveUid;
    TMap<ui32, ui64> HivesByHiveUid;
    TNamedCompactionPolicies NamedCompactionPolicies;

    TDomainsInfo() {
        // Add default configs. They can be overriden by user
        NamedCompactionPolicies[SystemTableDefaultPoicyName()] = NLocalDb::CreateDefaultTablePolicy();
        NamedCompactionPolicies[UserTableDefaultPoicyName()] = NLocalDb::CreateDefaultUserTablePolicy();
    }

    TIntrusiveConstPtr<NLocalDb::TCompactionPolicy> GetDefaultSystemTablePolicy() const {
        return *NamedCompactionPolicies.FindPtr(SystemTableDefaultPoicyName());
    }

    TIntrusiveConstPtr<NLocalDb::TCompactionPolicy> GetDefaultUserTablePolicy() const {
        return *NamedCompactionPolicies.FindPtr(UserTableDefaultPoicyName());
    }

    void AddCompactionPolicy(TString name, TIntrusiveConstPtr<NLocalDb::TCompactionPolicy> policy) {
        NamedCompactionPolicies[name] = policy;
    }

    void AddDomain(TDomain *domain) {
        Y_VERIFY(domain->DomainUid <= MaxDomainId);
        Domains[domain->DomainUid] = domain;
        DomainByName[domain->Name] = domain;
        Y_VERIFY(Domains.size() == DomainByName.size());
        for (auto group: domain->StateStorageGroups) {
            DomainByStateStorageGroup[group] = domain;
        }

        for (auto hiveUid : domain->HiveUids) {
            DomainByHiveUid[hiveUid] = domain;
        }
    }

    void AddHive(ui32 hiveUid, ui64 hive) {
        HivesByHiveUid[hiveUid] = hive;
    }

    void ClearDomainsAndHive() {
        Domains.clear();
        DomainByName.clear();
        DomainByStateStorageGroup.clear();
        DomainByHiveUid.clear();
        HivesByHiveUid.clear();
    }

    ui32 GetDefaultStateStorageGroup(ui32 domainUid) const {
        auto it = Domains.find(domainUid);
        Y_VERIFY(it != Domains.end(), "domainUid = %" PRIu32, domainUid);
        return it->second->DefaultStateStorageGroup;
    }

    ui32 GetDefaultHiveUid(ui32 domainUid) const {
        auto it = Domains.find(domainUid);
        Y_VERIFY(it != Domains.end(), "domainUid = %" PRIu32, domainUid);
        return it->second->DefaultHiveUid;
    }

    ui32 GetStateStorageGroupDomainUid(ui32 stateStorageGroup) const {
        auto it = DomainByStateStorageGroup.find(stateStorageGroup);
        Y_VERIFY(it != DomainByStateStorageGroup.end(), "stateStorageGroup = %" PRIu32, stateStorageGroup);
        return it->second->DomainUid;
    }

    ui32 GetDomainUidByTabletId(ui64 tabletId) const {
        const ui32 ssid = StateStorageGroupFromTabletID(tabletId);
        if (const auto *x = DomainByStateStorageGroup.FindPtr(ssid))
            return x->Get()->DomainUid;
        else
            return BadDomainId;
    }

    const TDomain& GetDomain(ui32 domainUid) const {
        auto it = Domains.find(domainUid);
        Y_VERIFY(it != Domains.end(), "domainUid = %" PRIu32, domainUid);
        return *(it->second);
    }

    const TDomain* GetDomainByName(TStringBuf name) const {
        auto it = DomainByName.find(name);
        if (it != DomainByName.end())
            return it->second.Get();
        return nullptr;
    }

    ui64 GetHive(ui32 hiveUid) const {
        auto it = HivesByHiveUid.find(hiveUid);
        if (it != HivesByHiveUid.end())
            return it->second;
        else
            return BadTabletId;
    }

    ui32 GetHiveDomainUid(ui32 hiveUid) const {
        auto it = DomainByHiveUid.find(hiveUid);
        if (it != DomainByHiveUid.end())
            return it->second->DomainUid;
        else
            return BadDomainId;
    }

    ui32 GetHiveUidByHiveId(ui64 hiveTabletId) const {
        for (const auto &xpair : HivesByHiveUid)
            if (xpair.second == hiveTabletId)
                return xpair.first;
        return Max<ui32>();
    }
};

}

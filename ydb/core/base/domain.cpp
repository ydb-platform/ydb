#include "domain.h"
#include <ydb/core/protos/blobstorage_config.pb.h>

namespace NKikimr {

    TDomainsInfo::TDomain::TDomain(const TString &name, ui32 domainUid, ui64 schemeRootId,
            ui32 defaultStateStorageGroup, ui32 defaultSchemeBoardGroup,
            TVectorUi32 stateStorageGroup,
            TVectorUi64 coordinators, TVectorUi64 mediators, TVectorUi64 allocators,
            ui32 defaultHiveUid, TVectorUi32 hivesUids,
            ui64 domainPlanResolution, const TStoragePoolKinds *poolTypes)
        : DomainUid(domainUid)
        , DefaultStateStorageGroup(defaultStateStorageGroup)
        , DefaultSchemeBoardGroup(defaultSchemeBoardGroup)
        , SchemeRoot(schemeRootId)
        , Name(name)
        , Coordinators(std::move(coordinators))
        , Mediators(std::move(mediators))
        , TxAllocators(std::move(allocators))
        , StateStorageGroups(std::move(stateStorageGroup))
        , DefaultHiveUid(defaultHiveUid)
        , HiveUids(std::move(hivesUids))
        , DomainPlanResolution(domainPlanResolution)
        , StoragePoolTypes(poolTypes ? *poolTypes : TStoragePoolKinds())
    {}

    TDomainsInfo::TDomain::~TDomain()
    {}

} // NKikimr

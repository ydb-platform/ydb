#include "domain.h"
#include <ydb/core/protos/blobstorage_config.pb.h>

namespace NKikimr {

    TDomainsInfo::TDomain::TDomain(const TString &name, ui32 domainUid, ui64 schemeRootId,
            TVectorUi64 coordinators, TVectorUi64 mediators, TVectorUi64 allocators,
            ui64 domainPlanResolution, ui32 timecastBucketsPerMediator,
            const TStoragePoolKinds *poolTypes)
        : DomainUid(domainUid)
        , SchemeRoot(schemeRootId)
        , Name(name)
        , Coordinators(std::move(coordinators))
        , Mediators(std::move(mediators))
        , TxAllocators(std::move(allocators))
        , DomainPlanResolution(domainPlanResolution)
        , TimecastBucketsPerMediator(timecastBucketsPerMediator)
        , StoragePoolTypes(poolTypes ? *poolTypes : TStoragePoolKinds())
    {}

    TDomainsInfo::TDomain::~TDomain()
    {}

} // NKikimr

#include "domain.h"
#include "storage_pool_kinds.h"

namespace NKikimr {

struct TDomainsInfo::TDomain::TImpl {
    TStoragePoolKinds StoragePoolTypes;

    explicit TImpl(const TStoragePoolKinds* poolTypes)
        : StoragePoolTypes(poolTypes ? *poolTypes : TStoragePoolKinds())
    {}
};

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
    , Impl(MakeHolder<TImpl>(poolTypes))
    , StoragePoolTypes(Impl->StoragePoolTypes)
{}

TDomainsInfo::TDomain::~TDomain() = default;

TDomainsInfo::TDomain::TPtr TDomainsInfo::TDomain::ConstructEmptyDomain(const TString &name, ui32 domainId)
{
    const ui64 schemeRoot = 0;
    return new TDomain(name, domainId, schemeRoot, {}, {}, {},
            DefaultPlanResolution, DefaultTimecastBucketsPerMediator, nullptr);
}

} // NKikimr

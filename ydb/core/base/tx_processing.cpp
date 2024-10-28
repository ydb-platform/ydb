#include "tx_processing.h"

#include "defs.h"

NKikimr::TCoordinators::TCoordinators(const NKikimrSubDomains::TProcessingParams &processing)
    : Coordinators(processing.GetCoordinators().begin(), processing.GetCoordinators().end())
{}

NKikimr::TCoordinators::TCoordinators(const NKikimr::TDomainsInfo::TDomain &domain)
    : Coordinators(domain.Coordinators)
{}

NKikimr::TCoordinators::TCoordinators(const TVector<ui64> &coordinators)
    : Coordinators(coordinators)
{}

NKikimr::TCoordinators::TCoordinators(TVector<ui64> &&coordinators)
    : Coordinators(std::move(coordinators))
{}


ui64 NKikimr::TCoordinators::Select(ui64 txId) const {
    if (Coordinators.size()) {
        return Coordinators[txId % Coordinators.size()];
    }
    return 0;
}

const TVector<ui64> &NKikimr::TCoordinators::List() const {
    return Coordinators;
}

NKikimr::TMediators::TMediators(const NKikimrSubDomains::TProcessingParams &processing)
    :  Mediators(processing.GetMediators().begin(), processing.GetMediators().end())
{}

NKikimr::TMediators::TMediators(const NKikimr::TDomainsInfo::TDomain &domain)
    : Mediators(domain.Mediators)
{}

NKikimr::TMediators::TMediators(const TVector<ui64> &mediators)
    : Mediators(mediators)
{}

NKikimr::TMediators::TMediators(TVector<ui64> &&mediators)
    : Mediators(std::move(mediators))
{}

ui64 NKikimr::TMediators::Select(ui64 tabletId) const {
    return Mediators[Hash64to32(tabletId) % Mediators.size()];
}

const TVector<ui64> &NKikimr::TMediators::List() const {
    return Mediators;
}

NKikimrSubDomains::TProcessingParams NKikimr::ExtractProcessingParams(const NKikimr::TDomainsInfo::TDomain &domain) {
    NKikimrSubDomains::TProcessingParams params;
    params.SetVersion(0);
    params.SetPlanResolution(domain.DomainPlanResolution);
    for (ui64 coordinator: domain.Coordinators) {
        params.AddCoordinators(coordinator);
    }
    params.SetTimeCastBucketsPerMediator(domain.TimecastBucketsPerMediator);
    for (ui64 mediator: domain.Mediators) {
        params.AddMediators(mediator);
    }
    return params;
}

NKikimr::TTimeCastBuckets::TTimeCastBuckets()
    : TimecastBucketsPerMediator(TDomainsInfo::TDomain::DefaultTimecastBucketsPerMediator)
{}

NKikimr::TTimeCastBuckets::TTimeCastBuckets(ui32 timecastBuckets)
    : TimecastBucketsPerMediator(timecastBuckets)
{
    Y_ABORT_UNLESS(TimecastBucketsPerMediator);
}

NKikimr::TTimeCastBuckets::TTimeCastBuckets(const NKikimrSubDomains::TProcessingParams &processing)
    : TTimeCastBuckets(processing.GetTimeCastBucketsPerMediator())
{}

ui32 NKikimr::TTimeCastBuckets::Select(ui64 tabletId) const {
    return tabletId % TimecastBucketsPerMediator;
}

ui32 NKikimr::TTimeCastBuckets::Buckets() const {
    return TimecastBucketsPerMediator;
}

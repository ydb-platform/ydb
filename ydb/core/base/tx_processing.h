#pragma once

#include "domain.h"

#include <ydb/core/protos/subdomains.pb.h>

#include <util/generic/vector.h>
#include <util/generic/ptr.h>

namespace NKikimr {
    NKikimrSubDomains::TProcessingParams ExtractProcessingParams(const TDomainsInfo::TDomain &domain);

    struct TCoordinators: TAtomicRefCount<TCoordinators>  {
        using TPtr = TIntrusiveConstPtr<TCoordinators>;

        const TVector<ui64> Coordinators;

        TCoordinators(const NKikimrSubDomains::TProcessingParams &processing);
        TCoordinators(const TDomainsInfo::TDomain &domain);

        TCoordinators(const TVector<ui64> &coordinators);
        TCoordinators(TVector<ui64> &&coordinators);

        ui64 Select(ui64 txId) const;
        const TVector<ui64>& List() const;
    };

    struct TMediators: TAtomicRefCount<TMediators>  {
        using TPtr = TIntrusiveConstPtr<TMediators>;

        const TVector<ui64> Mediators;

        TMediators(const NKikimrSubDomains::TProcessingParams &processing);
        TMediators(const TDomainsInfo::TDomain &domain);

        TMediators(const TVector<ui64> &mediators);
        TMediators(TVector<ui64> &&mediators);

        ui64 Select(ui64 tabletId) const;
        const TVector<ui64>& List() const;
    };

    struct TTimeCastBuckets: TAtomicRefCount<TTimeCastBuckets>  {
        using TPtr = TIntrusiveConstPtr<TTimeCastBuckets>;

        const ui32 TimecastBucketsPerMediator;

        TTimeCastBuckets();
        TTimeCastBuckets(ui32 timecastBuckets);
        TTimeCastBuckets(const NKikimrSubDomains::TProcessingParams &processing);

        ui32 Select(ui64 tabletId) const;
        ui32 Buckets() const;
    };
}

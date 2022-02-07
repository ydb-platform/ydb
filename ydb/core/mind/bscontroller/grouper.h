#pragma once

#include "defs.h"

namespace NKikimr {
namespace NBsController {

struct TCandidate {
    TFailDomain FailDomain;
    TFailDomain PrefixFailDomain;
    TFailDomain InfixFailDomain;
    TFailDomain PostfixFailDomain;
    ui32 Badness;
    ui32 NodeId;
    ui32 PDiskId;
    ui32 VDiskSlotId;

    TCandidate(TFailDomain failDomain, ui8 beginLevel, ui8 lastLevel, ui32 badness, ui32 nodeId, ui32 pDiskId,
        ui32 vDiskSlotId);
};

bool GroupFromCandidates(TVector<TCandidate> &candidates, ui32 domainCount, ui32 candidatesPerDomainCount,
    TVector<TVector<const TCandidate*>> &outBestGroup);

bool VerifyGroup(const TVector<TVector<const TCandidate*>> &group, ui32 domainCount, ui32 candidatesPerDomainCount);

bool CreateGroupWithRings(const TVector<TCandidate>& candidates, ui32 numRings, ui32 numFailDomainsPerRing,
    ui32 numDisksPerFailDomain, ui32 firstRingDxLevel, ui32 lastRingDxLevel,
    TVector<TVector<TVector<const TCandidate*>>>& bestGroup);

} //NBsController

} //NKikimr

#include "blobstorage_groupinfo.h"
#include "blobstorage_groupinfo_blobmap.h"
#include "blobstorage_groupinfo_iter.h"
#include "blobstorage_groupinfo_sets.h"
#include "blobstorage_groupinfo_partlayout.h"
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/pop_count/popcount.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/string/printf.h>
#include <util/string/escape.h>
#include <util/stream/input.h>
#include <util/random/fast.h>
#include <util/system/unaligned_mem.h>
#include <util/string/vector.h>
#include <util/string/type.h>
#include <util/string/cast.h>

namespace NKikimr {

class TQuorumCheckerBase : public TBlobStorageGroupInfo::IQuorumChecker {
protected:
    const TBlobStorageGroupInfo::TTopology *Top;

public:
    TQuorumCheckerBase(const TBlobStorageGroupInfo::TTopology *top)
        : Top(top)
    {}

    bool CheckFailModelForGroup(const TBlobStorageGroupInfo::TGroupVDisks& failedGroupDisks) const override {
        return CheckFailModelForGroupDomains(TBlobStorageGroupInfo::TGroupFailDomains::CreateFromGroupDiskSet(
                failedGroupDisks, TBlobStorageGroupInfo::TGroupFailDomains::EDiskCondition::ANY));
    }

    bool CheckQuorumForGroup(const TBlobStorageGroupInfo::TGroupVDisks& groupDisks) const override {
        return CheckQuorumForGroupDomains(TBlobStorageGroupInfo::TGroupFailDomains::CreateFromGroupDiskSet(
                groupDisks, TBlobStorageGroupInfo::TGroupFailDomains::EDiskCondition::ALL));
    }
};

class TQuorumCheckerOrdinary : public TQuorumCheckerBase {
public:
    using TQuorumCheckerBase::TQuorumCheckerBase;

    bool CheckFailModelForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& failedSubgroupDisks) const override {
        return failedSubgroupDisks.GetNumSetItems() <= Top->GType.Handoff();
    }

    bool CheckFailModelForGroupDomains(const TBlobStorageGroupInfo::TGroupFailDomains& failedDomains) const override {
        return failedDomains.GetNumSetItems() <= Top->GType.Handoff();
    }

    bool CheckQuorumForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& subgroupDisks) const override {
        return CheckFailModelForSubgroup(~subgroupDisks);
    }

    bool CheckQuorumForGroupDomains(const TBlobStorageGroupInfo::TGroupFailDomains& domains) const override {
        return CheckFailModelForGroupDomains(~domains);
    }

    bool IsDegraded(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) const override {
        const auto& domains = TBlobStorageGroupInfo::TGroupFailDomains::CreateFromGroupDiskSet(failedDisks,
            TBlobStorageGroupInfo::TGroupFailDomains::EDiskCondition::ANY);
        return domains.GetNumSetItems() == Top->GType.Handoff();
    }

    bool OneStepFromDegradedOrWorse(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) const override {
        const auto& domains = TBlobStorageGroupInfo::TGroupFailDomains::CreateFromGroupDiskSet(failedDisks,
            TBlobStorageGroupInfo::TGroupFailDomains::EDiskCondition::ANY);
        return domains.GetNumSetItems() + 1 >= Top->GType.Handoff();
    }

    TBlobStorageGroupInfo::EBlobState GetBlobState(const TSubgroupPartLayout& parts,
            const TBlobStorageGroupInfo::TSubgroupVDisks& failedDisks) const override {
        if (!CheckFailModelForSubgroup(failedDisks)) {
            return TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        } else {
            ui32 effectiveReplicas = parts.CountEffectiveReplicas(Top->GType);
            return Top->BlobState(effectiveReplicas, failedDisks.GetNumSetItems());
        }
    }

    ui32 GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const override {
        const TBlobStorageGroupType type = Top->GType;
        const ui32 effectiveReplicas = parts.CountEffectiveReplicas(Top->GType);
        if (effectiveReplicas == type.TotalPartCount()) {
            return 0; // full quorum of records
        } else if (effectiveReplicas < type.MinimalRestorablePartCount()) {
            return 0; // no chance to restore this blob
        }

        // iterate over all possible parts and see if adding extra part increases effective replica count
        auto addsEffectiveReplicas = [&](ui32 part) {
            TSubgroupPartLayout temp(parts);
            temp.AddItem(idxInSubgroup, part, type);
            const ui32 newEffectiveReplicas = temp.CountEffectiveReplicas(type);
            Y_ABORT_UNLESS(newEffectiveReplicas == effectiveReplicas || newEffectiveReplicas == effectiveReplicas + 1);
            return newEffectiveReplicas > effectiveReplicas;
        };
        if (idxInSubgroup < type.TotalPartCount()) {
            return addsEffectiveReplicas(idxInSubgroup) ? 1 << idxInSubgroup : 0;
        } else {
            for (ui32 part = 0; part < type.TotalPartCount(); ++part) {
                if (addsEffectiveReplicas(part)) {
                    return 1 << part;
                }
            }
            return 0;
        }
    }
};

class TQuorumCheckerMirror3of4 : public TQuorumCheckerBase {
public:
    using TQuorumCheckerBase::TQuorumCheckerBase;

    bool CheckFailModelForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& failedSubgroupDisks) const override {
        return failedSubgroupDisks.GetNumSetItems() <= 2;
    }

    bool CheckFailModelForGroupDomains(const TBlobStorageGroupInfo::TGroupFailDomains& failedDomains) const override {
        return failedDomains.GetNumSetItems() <= 2;
    }

    bool CheckQuorumForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& subgroupDisks) const override {
        return CheckFailModelForSubgroup(~subgroupDisks);
    }

    bool CheckQuorumForGroupDomains(const TBlobStorageGroupInfo::TGroupFailDomains& domains) const override {
        return CheckFailModelForGroupDomains(~domains);
    }

    bool IsDegraded(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) const override {
        const auto& domains = TBlobStorageGroupInfo::TGroupFailDomains::CreateFromGroupDiskSet(failedDisks,
            TBlobStorageGroupInfo::TGroupFailDomains::EDiskCondition::ANY);
        return domains.GetNumSetItems() == 2;
    }

    bool OneStepFromDegradedOrWorse(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) const override {
        const auto& domains = TBlobStorageGroupInfo::TGroupFailDomains::CreateFromGroupDiskSet(failedDisks,
            TBlobStorageGroupInfo::TGroupFailDomains::EDiskCondition::ANY);
        return domains.GetNumSetItems() + 1 >= 2;
    }

    TBlobStorageGroupInfo::EBlobState GetBlobState(const TSubgroupPartLayout& parts,
            const TBlobStorageGroupInfo::TSubgroupVDisks& failedDisks) const override {
        if (!CheckFailModelForSubgroup(failedDisks)) {
            return TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        } else if (auto [data, any] = parts.GetMirror3of4State(); data >= 3 && any >= 5) {
            return TBlobStorageGroupInfo::EBS_FULL;
        } else if (data) {
            return TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY;
        } else {
            return TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY;
        }
    }

    ui32 GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const override {
        auto [data, any] = parts.GetMirror3of4State();
        if (!data) {
            return 0; // nowhere to restore from
        } else if (data < 3) {
            // not enough data parts -- we must restore them first
            if ((parts.GetDisksWithPart(0) | parts.GetDisksWithPart(1)) & (1 << idxInSubgroup)) {
                return 0; // this disk already has data part and we can't help the group
            }
            return 1 << (idxInSubgroup & 1); // to fit this possible layout of parts over disks: 0 1 0 1 01 01 01 01
        } else if (any < 5) {
            // enough data parts, but not enough metadata ones
            if ((parts.GetDisksWithPart(0) | parts.GetDisksWithPart(1) | parts.GetDisksWithPart(2)) & (1 << idxInSubgroup)) {
                return 0; // this disk already contains one part
            }
            return 1 << 2;
        } else {
            // full quorum
            return 0;
        }
    }
};

class TQuorumCheckerMirror3dc : public TQuorumCheckerBase {
    static bool CheckFailModel(ui32 num1, ui32 num2) {
        return num1 <= 2 && num2 <= 1;
    }

    static void CountSubgroupRealmStat(const TBlobStorageGroupInfo::TSubgroupVDisks& set, ui32 *num1, ui32 *num2) {
        for (ui32 realm = 0; realm < 3; ++realm) {
            ui32 num = 0;
            for (ui32 domain = 0; domain < 3; ++domain) {
                const ui32 nodeId = realm + domain * 3; // nodeId for blob must be consistent with blob mapper
                num += set[nodeId];
            }
            *num1 += num >= 1;
            *num2 += num >= 2;
        }
    }

    bool CheckFailModelForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& failedSubgroupDisks) const override {
        ui32 num1 = 0; // number of realms with at least one fail domain set
        ui32 num2 = 0; // number of realms with at least two fail domains set
        CountSubgroupRealmStat(failedSubgroupDisks, &num1, &num2);
        return CheckFailModel(num1, num2);
    }

    bool CheckFailModelForGroupDomains(const TBlobStorageGroupInfo::TGroupFailDomains& failedDomains) const override {
        ui32 num1 = 0; // number of realms with at least one fail domain set
        ui32 num2 = 0; // number of realms with at least two fail domains set
        for (auto realmIt = Top->FailRealmsBegin(), realmEnd = Top->FailRealmsEnd(); realmIt != realmEnd; ++realmIt) {
            ui32 num = 0;
            for (const TBlobStorageGroupInfo::TFailDomain& domain : realmIt.GetFailRealmFailDomains()) {
                num += failedDomains[domain.FailDomainOrderNumber];
            }
            num1 += num >= 1;
            num2 += num >= 2;
        }
        return CheckFailModel(num1, num2);
    }

    bool CheckQuorumForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& subgroupDisks) const override {
        ui32 num1 = 0; // number of realms with at least one fail domain set
        ui32 num2 = 0; // number of realms with at least two fail domains set
        CountSubgroupRealmStat(subgroupDisks, &num1, &num2);
        return num1 == 3 || num2 == 2;
    }

    bool CheckQuorumForGroupDomains(const TBlobStorageGroupInfo::TGroupFailDomains& domains) const override {
        // treat all unset domains as failed ones -- and check if the subset fits the failure model
        return CheckFailModelForGroupDomains(~domains);
    }

    bool IsDegraded(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) const override {
        ui32 num1 = 0; // number of realms with at least one fail domain
        for (const auto& realm : Top->FailRealms) {
            for (const auto& domain : realm.FailDomains) {
                for (const auto& disk : domain.VDisks) {
                    if (failedDisks[disk.OrderNumber]) {
                        ++num1;
                        goto quitIter;
                    }
                }
            }
quitIter:   ;
        }
        return num1 == 2; // two realms have faulty disks
    }

    bool OneStepFromDegradedOrWorse(const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) const override {
        return failedDisks.GetNumSetItems();
    }

    TBlobStorageGroupInfo::EBlobState GetBlobState(const TSubgroupPartLayout& parts,
            const TBlobStorageGroupInfo::TSubgroupVDisks& failedDisks) const override {
        if (!CheckFailModelForSubgroup(failedDisks)) {
            return TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        } else {
            const TBlobStorageGroupInfo::TSubgroupVDisks& disksWithReplica = parts.GetInvolvedDisks(Top);

            if (CheckQuorumForSubgroup(disksWithReplica)) {
                // we have all required replicas present in the parts set, so the blob is fully written
                return TBlobStorageGroupInfo::EBS_FULL;
            } else if (disksWithReplica) {
                // we have some disks with replicas, so data could be easily recovered if needed
                return TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY;
            } else {
                // we can't recover data
                return TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY;
            }
        }
    }

    ui32 GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const override {
        const TBlobStorageGroupInfo::TSubgroupVDisks& disksWithReplica = parts.GetInvolvedDisks(Top);
        const ui32 myRing = idxInSubgroup % 3;
        const ui32 disksWithPart = parts.GetDisksWithPart(myRing);
        if (!disksWithReplica) {
            return 0; // nowhere to resurrect from
        } else if (CheckQuorumForSubgroup(disksWithReplica)) {
            return 0; // full quorum of writes
        } else if (parts.GetDisksWithPart(myRing) & (1 << idxInSubgroup)) {
            return 0; // we already have a part
        }
        // filter out only parts on their respective disks to filter out possibly incorrectly written parts
        const ui32 numDisksWithPartInMyRing = PopCount((disksWithPart >> myRing) & 0x49); // binary stencil 001001001
        // resurrect matching part if there are less than 2 parts in our datacenter
        return numDisksWithPartInMyRing < 2 ? 1 << myRing : 0;
    }

public:
    using TQuorumCheckerBase::TQuorumCheckerBase;
};

////////////////////////////////////////////////////////////////////////////
// TBlobStorageGroupInfo::TTopology
////////////////////////////////////////////////////////////////////////////
TBlobStorageGroupInfo::TTopology::TTopology(TBlobStorageGroupType gtype)
    : GType(gtype)
{}

TBlobStorageGroupInfo::TTopology::TTopology(TBlobStorageGroupType gtype, ui32 numFailRealms,
        ui32 numFailDomainsPerFailRealm, ui32 numVDisksPerFailDomain, bool finalize)
    : GType(gtype)
{
    FailRealms = {numFailRealms, {
        {numFailDomainsPerFailRealm, {
            {numVDisksPerFailDomain, TVDiskInfo{}}
        }}
    }};
    if (finalize) {
        FinalizeConstruction();
    }
}

TBlobStorageGroupInfo::TTopology::~TTopology() = default;

bool TBlobStorageGroupInfo::TTopology::EqualityCheck(const TTopology &t) {
    return GType.GetErasure() == t.GType.GetErasure() &&
        TotalFailDomains == t.TotalFailDomains &&
        TotalVDisks == t.TotalVDisks;
}

void TBlobStorageGroupInfo::TTopology::FinalizeConstruction() {
    // finish construction of topology structure
    ui32 failDomainOrderNumber = 0;
    ui32 vdiskOrderNumber = 0;
    VDiskIdForOrderNumber.clear();

    for (auto realm = FailRealms.begin(); realm != FailRealms.end(); ++realm) {
        for (auto domain = realm->FailDomains.begin(); domain != realm->FailDomains.end(); ++domain) {
            domain->FailDomainOrderNumber = failDomainOrderNumber++;
            for (auto vdisk = domain->VDisks.begin(); vdisk != domain->VDisks.end(); ++vdisk) {
                // calculate positions
                ui8 realmPos = realm - FailRealms.begin();            // it.GetFailRealmIdx()
                ui8 domainPos = domain - realm->FailDomains.begin();  // it.GetFailDomainIdx()
                ui8 vdiskPos = vdisk - domain->VDisks.begin();        // it.GetVDiskIdx());
                // fill in VDisk parameters
                vdisk->VDiskIdShort = TVDiskIdShort(realmPos, domainPos, vdiskPos);
                vdisk->OrderNumber = vdiskOrderNumber++;
                vdisk->FailDomainOrderNumber = domain->FailDomainOrderNumber;
                // fill in VDiskIdForOrderNumber
                VDiskIdForOrderNumber.push_back(vdisk->VDiskIdShort);
            }
        }
    }

    TotalFailDomains = failDomainOrderNumber;
    TotalVDisks = vdiskOrderNumber;
    // create blob mapper
    BlobMapper.reset(CreateMapper(GType, this));
    // create quorum checker
    QuorumChecker.reset(CreateQuorumChecker(this));
}

bool TBlobStorageGroupInfo::TTopology::IsValidId(const TVDiskID& vdisk) const {
    if (vdisk.FailRealm >= FailRealms.size()) {
        return false;
    }
    if (vdisk.FailDomain >= GetNumFailDomainsPerFailRealm()) {
        return false;
    }
    if (vdisk.VDisk >= GetNumVDisksPerFailDomain()) {
        return false;
    }
    return true;
}

bool TBlobStorageGroupInfo::TTopology::IsValidId(const TVDiskIdShort& vdisk) const {
    if (vdisk.FailRealm >= FailRealms.size()) {
        return false;
    }
    if (vdisk.FailDomain >= GetNumFailDomainsPerFailRealm()) {
        return false;
    }
    if (vdisk.VDisk >= GetNumVDisksPerFailDomain()) {
        return false;
    }
    return true;
}

ui32 TBlobStorageGroupInfo::TTopology::GetFailDomainOrderNumber(const TVDiskIdShort& vdisk) const {
    return FailRealms[vdisk.FailRealm].FailDomains[vdisk.FailDomain].VDisks[vdisk.VDisk].FailDomainOrderNumber;
}

TVDiskIdShort TBlobStorageGroupInfo::TTopology::GetVDiskId(ui32 orderNumber) const {
    return VDiskIdForOrderNumber[orderNumber];
}

const TBlobStorageGroupInfo::TFailDomain& TBlobStorageGroupInfo::TTopology::GetFailDomain(const TVDiskIdShort& vdisk) const {
    return FailRealms[vdisk.FailRealm].FailDomains[vdisk.FailDomain];
}

const TBlobStorageGroupInfo::TFailDomain& TBlobStorageGroupInfo::TTopology::GetFailDomain(ui32 failDomainOrderNumber) const {
    ui32 realm = 0;
    while (failDomainOrderNumber >= FailRealms[realm].FailDomains.size()) {
        failDomainOrderNumber -= FailRealms[realm].FailDomains.size();
        ++realm;
    }
    return FailRealms[realm].FailDomains[failDomainOrderNumber];
}

ui32 TBlobStorageGroupInfo::TTopology::GetOrderNumber(const TVDiskIdShort &vdisk) const {
    return FailRealms[vdisk.FailRealm].FailDomains[vdisk.FailDomain].VDisks[vdisk.VDisk].OrderNumber;
}

void TBlobStorageGroupInfo::TTopology::PickSubgroup(ui32 hash, TBlobStorageGroupInfo::TOrderNums &orderNums) const {
    return BlobMapper->PickSubgroup(hash, orderNums);
}

bool TBlobStorageGroupInfo::TTopology::BelongsToSubgroup(const TVDiskIdShort& vdisk, ui32 hash) const {
    return BlobMapper->BelongsToSubgroup(vdisk, hash);
}

ui32 TBlobStorageGroupInfo::TTopology::GetIdxInSubgroup(const TVDiskIdShort& vdisk, ui32 hash) const {
    return BlobMapper->GetIdxInSubgroup(vdisk, hash);
}

bool TBlobStorageGroupInfo::TTopology::IsHandoff(const TVDiskIdShort& vdisk, ui32 hash) const {
    return BlobMapper->GetIdxInSubgroup(vdisk, hash) >= GType.TotalPartCount();
}

TVDiskIdShort TBlobStorageGroupInfo::TTopology::GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) const {
    return BlobMapper->GetVDiskInSubgroup(idxInSubgroup, hash);
}

TBlobStorageGroupInfo::EBlobState TBlobStorageGroupInfo::TTopology::BlobState(ui32 effectiveReplicas, ui32 errorDomains) const {
    if (errorDomains > Min(GType.ParityParts(), GType.Handoff())) {
        return EBS_DISINTEGRATED;
    }
    const ui32 minimalRestorable = GType.MinimalRestorablePartCount();
    if (effectiveReplicas + errorDomains < minimalRestorable) {
        return EBS_UNRECOVERABLE_FRAGMENTARY;
    }
    const ui32 totalParts = GType.TotalPartCount();
    if (effectiveReplicas + errorDomains < totalParts) {
        if (effectiveReplicas < minimalRestorable) {
            return EBS_UNRECOVERABLE_FRAGMENTARY;
        }
        return EBS_RECOVERABLE_FRAGMENTARY;
    }
    if (effectiveReplicas == totalParts) {
        return EBS_FULL;
    }
    if (effectiveReplicas < minimalRestorable) {
        return EBS_UNRECOVERABLE_FRAGMENTARY;
    }
    return EBS_RECOVERABLE_DOUBTED;
}

IBlobToDiskMapper *TBlobStorageGroupInfo::TTopology::CreateMapper(TBlobStorageGroupType gtype,
                                                                  const TTopology *topology)
{
    switch (gtype.GetErasure()) {
        case TBlobStorageGroupType::ErasureNone:
        case TBlobStorageGroupType::ErasureMirror3:
        case TBlobStorageGroupType::Erasure3Plus1Block:
        case TBlobStorageGroupType::Erasure3Plus1Stripe:
        case TBlobStorageGroupType::Erasure4Plus2Block:
        case TBlobStorageGroupType::Erasure3Plus2Block:
        case TBlobStorageGroupType::Erasure4Plus2Stripe:
        case TBlobStorageGroupType::Erasure3Plus2Stripe:
        case TBlobStorageGroupType::ErasureMirror3Plus2:
        case TBlobStorageGroupType::Erasure4Plus3Block:
        case TBlobStorageGroupType::Erasure4Plus3Stripe:
        case TBlobStorageGroupType::Erasure3Plus3Block:
        case TBlobStorageGroupType::Erasure3Plus3Stripe:
        case TBlobStorageGroupType::Erasure2Plus3Block:
        case TBlobStorageGroupType::Erasure2Plus3Stripe:
        case TBlobStorageGroupType::Erasure2Plus2Block:
        case TBlobStorageGroupType::Erasure2Plus2Stripe:
        case TBlobStorageGroupType::ErasureMirror3of4:
            return IBlobToDiskMapper::CreateBasicMapper(topology);

        case TBlobStorageGroupType::ErasureMirror3dc:
            return IBlobToDiskMapper::CreateMirror3dcMapper(topology);

        default:
            Y_ABORT("unexpected erasure type 0x%08" PRIx32, static_cast<ui32>(gtype.GetErasure()));
    }

    Y_ABORT();
}

TBlobStorageGroupInfo::IQuorumChecker *TBlobStorageGroupInfo::TTopology::CreateQuorumChecker(const TTopology *topology) {
    switch (topology->GType.GetErasure()) {
        case TBlobStorageGroupType::ErasureNone:
        case TBlobStorageGroupType::ErasureMirror3:
        case TBlobStorageGroupType::Erasure3Plus1Block:
        case TBlobStorageGroupType::Erasure3Plus1Stripe:
        case TBlobStorageGroupType::Erasure4Plus2Block:
        case TBlobStorageGroupType::Erasure3Plus2Block:
        case TBlobStorageGroupType::Erasure4Plus2Stripe:
        case TBlobStorageGroupType::Erasure3Plus2Stripe:
        case TBlobStorageGroupType::ErasureMirror3Plus2:
        case TBlobStorageGroupType::Erasure4Plus3Block:
        case TBlobStorageGroupType::Erasure4Plus3Stripe:
        case TBlobStorageGroupType::Erasure3Plus3Block:
        case TBlobStorageGroupType::Erasure3Plus3Stripe:
        case TBlobStorageGroupType::Erasure2Plus3Block:
        case TBlobStorageGroupType::Erasure2Plus3Stripe:
        case TBlobStorageGroupType::Erasure2Plus2Block:
        case TBlobStorageGroupType::Erasure2Plus2Stripe:
            return new TQuorumCheckerOrdinary(topology);

        case TBlobStorageGroupType::ErasureMirror3dc:
            return new TQuorumCheckerMirror3dc(topology);

        case TBlobStorageGroupType::ErasureMirror3of4:
            return new TQuorumCheckerMirror3of4(topology);

        default:
            Y_ABORT("unexpected erasure type 0x%08" PRIx32,
                   static_cast<ui32>(topology->GType.GetErasure()));
    }

    Y_ABORT();
}

TString TBlobStorageGroupInfo::TTopology::ToString() const {
    TStringStream str;
    str << "{GType# " << GType.ToString();
    str << " FailRealms# {";
    for (ui32 realmIdx = 0; realmIdx < FailRealms.size(); ++realmIdx) {
        const TFailRealm& realm = FailRealms[realmIdx];
        str << (realmIdx ? " " : "") << "[" << realmIdx << "]# TFailRealm{ FailDomains# {";
        for (ui32 domainIdx = 0; domainIdx < realm.FailDomains.size(); ++domainIdx) {
            const TFailDomain& domain = realm.FailDomains[domainIdx];
            str << (domainIdx ? " " : "") << "[" << domainIdx << "]# TFailDomain{";
            str << "FailDomainOrderNumber# " << domain.FailDomainOrderNumber
                << " VDisks# {";
            for (ui32 vdiskIdx = 0; vdiskIdx < domain.VDisks.size(); ++vdiskIdx) {
                const TVDiskInfo& info = domain.VDisks[vdiskIdx];
                str << (vdiskIdx ? " " : "") << "[" << vdiskIdx << "]# TVDiskInfo{"
                << "OrderNumber# " << info.OrderNumber
                << " FailDomainOrderNumber# " << info.FailDomainOrderNumber
                << "}";
            }
            str << "}";
        }
        str << "}}";
    }
    str << "}}";
    return str.Str();
}



////////////////////////////////////////////////////////////////////////////
// TBlobStorageGroupInfo::TDynamicInfo
////////////////////////////////////////////////////////////////////////////
TBlobStorageGroupInfo::TDynamicInfo::TDynamicInfo(TGroupId groupId, ui32 groupGen)
    : GroupId(groupId)
    , GroupGeneration(groupGen)
{}

////////////////////////////////////////////////////////////////////////////
// TBlobStorageGroupInfo
////////////////////////////////////////////////////////////////////////////
TBlobStorageGroupInfo::TBlobStorageGroupInfo(TBlobStorageGroupType gtype, ui32 numVDisksPerFailDomain,
        ui32 numFailDomains, ui32 numFailRealms, const TVector<TActorId> *vdiskIds, EEncryptionMode encryptionMode,
        ELifeCyclePhase lifeCyclePhase, TCypherKey key, TGroupId groupId)
    : GroupID(groupId)
    , GroupGeneration(1)
    , Type(gtype)
    , Dynamic(GroupID, GroupGeneration)
    , EncryptionMode(encryptionMode)
    , LifeCyclePhase(lifeCyclePhase)
    , Key(key)
{
    if (!numFailDomains) {
        numFailDomains = gtype.BlobSubgroupSize();
    }

    TTopology topology(gtype);
    topology.FailRealms.resize(numFailRealms);
    for (TFailRealm& realm : topology.FailRealms) {
        realm.FailDomains.resize(numFailDomains);
        for (TFailDomain& domain : realm.FailDomains) {
            domain.VDisks.resize(numVDisksPerFailDomain);
        }
    }
    Topology = std::make_shared<TTopology>(std::move(topology));
    Topology->FinalizeConstruction();

    for (ui32 i = 0, num = Topology->GetTotalVDisksNum(); i < num; ++i) {
        Dynamic.PushBackActorId(vdiskIds ? vdiskIds->at(i) : MakeBlobStorageVDiskID(1, i + 1, 0));
    }
}

TBlobStorageGroupInfo::TBlobStorageGroupInfo(std::shared_ptr<TTopology> topology, TDynamicInfo&& dyn, TString storagePoolName,
        TMaybe<TKikimrScopeId> acceptedScope, NPDisk::EDeviceType deviceType)
    : GroupID(dyn.GroupId)
    , GroupGeneration(dyn.GroupGeneration)
    , Type(topology->GType)
    , Topology(std::move(topology))
    , Dynamic(std::move(dyn))
    , AcceptedScope(acceptedScope)
    , StoragePoolName(std::move(storagePoolName))
    , DeviceType(deviceType)
{}

TBlobStorageGroupInfo::TBlobStorageGroupInfo(TTopology&& topology, TDynamicInfo&& dyn, TString storagePoolName,
        TMaybe<TKikimrScopeId> acceptedScope, NPDisk::EDeviceType deviceType)
    : TBlobStorageGroupInfo(std::make_shared<TTopology>(std::move(topology)), std::move(dyn), std::move(storagePoolName),
            std::move(acceptedScope), deviceType)
{
    Topology->FinalizeConstruction();
}

TBlobStorageGroupInfo::TBlobStorageGroupInfo(const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TVDiskID& vdiskId,
        const TActorId& actorId)
    : GroupID(vdiskId.GroupID)
    , GroupGeneration(vdiskId.GroupGeneration)
    , Type(info->Type)
    , Topology(info->Topology)
    , Dynamic(GroupID, GroupGeneration)
    , EncryptionMode(info->EncryptionMode)
    , LifeCyclePhase(info->LifeCyclePhase)
    , Key(info->Key)
    , AcceptedScope(info->AcceptedScope)
    , StoragePoolName(info->StoragePoolName)
    , DeviceType(info->DeviceType)
{
    Dynamic.ServiceIdForOrderNumber.resize(Topology->GetTotalVDisksNum(), TActorId());
    Dynamic.ServiceIdForOrderNumber[GetOrderNumber(vdiskId)] = actorId;
}

TBlobStorageGroupInfo::~TBlobStorageGroupInfo()
{}

TIntrusivePtr<TBlobStorageGroupInfo> TBlobStorageGroupInfo::Parse(const NKikimrBlobStorage::TGroupInfo& group,
        const TEncryptionKey *key, IOutputStream *err) {
    auto erasure = (TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies();
    TBlobStorageGroupType type(erasure);
    TBlobStorageGroupInfo::TTopology topology(type);
    TBlobStorageGroupInfo::TDynamicInfo dyn(TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID), group.GetGroupGeneration());
    topology.FailRealms.resize(group.RingsSize());
    for (ui32 ringIdx = 0; ringIdx < group.RingsSize(); ++ringIdx) {
        const auto& realm = group.GetRings(ringIdx);
        TBlobStorageGroupInfo::TFailRealm& bsFailRealm = topology.FailRealms[ringIdx];
        bsFailRealm.FailDomains.resize(realm.FailDomainsSize());
        for (ui32 domainIdx = 0; domainIdx < realm.FailDomainsSize(); ++domainIdx) {
            const auto& domain = realm.GetFailDomains(domainIdx);
            TBlobStorageGroupInfo::TFailDomain& bsDomain = bsFailRealm.FailDomains[domainIdx];
            bsDomain.VDisks.resize(domain.VDiskLocationsSize());
            for (ui32 vdiskIdx = 0; vdiskIdx < domain.VDiskLocationsSize(); ++vdiskIdx) {
                const auto& id = domain.GetVDiskLocations(vdiskIdx);
                TActorId vdiskActorId = MakeBlobStorageVDiskID(id.GetNodeID(), id.GetPDiskID(), id.GetVDiskSlotID());
                dyn.PushBackActorId(vdiskActorId);
            }
        }
    }
    TMaybe<TKikimrScopeId> acceptedScope;
    if (group.HasAcceptedScope()) {
        const auto& scope = group.GetAcceptedScope();
        acceptedScope.ConstructInPlace(scope.GetX1(), scope.GetX2());
    }
    NPDisk::EDeviceType commonDeviceType = NPDisk::DEVICE_TYPE_UNKNOWN;
    if (group.HasDeviceType()) {
        commonDeviceType = PDiskTypeToPDiskType(group.GetDeviceType());
    }
    auto res = MakeIntrusive<TBlobStorageGroupInfo>(std::move(topology), std::move(dyn), group.GetStoragePoolName(),
        acceptedScope, commonDeviceType);
    if (group.HasBlobDepotId()) {
        res->BlobDepotId = group.GetBlobDepotId();
    }
    if (group.HasDecommitStatus()) {
        res->DecommitStatus = group.GetDecommitStatus();
    }

    // process encryption parameters
    res->EncryptionMode = static_cast<EEncryptionMode>(group.GetEncryptionMode());
    if (res->EncryptionMode != EEM_NONE) {
        auto& lcp = res->LifeCyclePhase;
        lcp = static_cast<ELifeCyclePhase>(group.GetLifeCyclePhase());
        switch (lcp) {
            case ELCP_INITIAL:
            case ELCP_IN_TRANSITION:
                break;

            case ELCP_IN_USE: {
                const TString mainKeyId = group.GetMainKeyId();
                const ui64 mainKeyVersion = group.GetMainKeyVersion();
                const TString encryptedGroupKey = group.GetEncryptedGroupKey();
                const ui64 groupKeyNonce = group.GetGroupKeyNonce();

                if (!key || !*key) {
                    lcp = ELCP_KEY_NOT_LOADED;
                } else if (mainKeyId != key->Id) {
                    lcp = ELCP_KEY_ID_ERROR;
                } else if (mainKeyVersion != key->Version) {
                    lcp = ELCP_KEY_VERSION_ERROR;
                } else if (!DecryptGroupKey(res->EncryptionMode, mainKeyId, encryptedGroupKey, groupKeyNonce, key->Key,
                        &res->Key, group.GetGroupID())) {
                    lcp = ELCP_KEY_CRC_ERROR;
                    res->Key.Wipe();
                } else {
                    break;
                }
                if (err) {
                    *err << "LifeCyclePhase# " << lcp << " Key.Id# \"" << EscapeC(key->Id)
                        << "\" Key.Version# " << key->Version << " MainKey.Id# \"" << EscapeC(mainKeyId)
                        << "\" MainKey.Version# " << mainKeyVersion << " GroupKeyNonce# " << groupKeyNonce;
                }
                break;
            }

            default:
                Y_VERIFY_DEBUG_S(false, "unexpected LifeCyclePhase# " << lcp);
                if (err) {
                    *err << "unexpected LifeCyclePhase# " << lcp;
                }
                break;
        }
    }

    // store original group protobuf it was parsed from
    res->Group.emplace(group);
    return res;
}

bool TBlobStorageGroupInfo::DecryptGroupKey(TBlobStorageGroupInfo::EEncryptionMode encryptionMode,
        const TString& /*mainKeyId*/, const TString& encryptedGroupKey, ui64 groupKeyNonce, const TCypherKey& tenantKey,
        TCypherKey *outGroupKey, ui32 groupId) {
    switch (encryptionMode) {
        case TBlobStorageGroupInfo::EEM_NONE:
            return true;
        case TBlobStorageGroupInfo::EEM_ENC_V1:
            {
                // Decrypt the 'Group key' with the 'Tenant key'
                ui8* keyBytes = nullptr;
                ui32 keySize = 0;
                outGroupKey->MutableKeyBytes(&keyBytes, &keySize);

                TStreamCypher cypher;
                bool isKeySet = cypher.SetKey(tenantKey);
                Y_ABORT_UNLESS(isKeySet);
                cypher.StartMessage(groupKeyNonce, 0);

                ui32 h = 0;
                Y_ABORT_UNLESS(encryptedGroupKey.size() == keySize + sizeof(h),
                        "Unexpected encryptedGroupKeySize# %" PRIu32 " keySize# %" PRIu32 " sizeof(h)# %" PRIu32
                        " groupId# %" PRIu32 " encryptedGroupKey# \"%s\"",
                        (ui32)encryptedGroupKey.size(), (ui32)keySize, (ui32)sizeof(h), (ui32)groupId,
                        EscapeC(encryptedGroupKey).c_str());
                cypher.Encrypt(keyBytes, encryptedGroupKey.data(), keySize);
                cypher.Encrypt(&h, encryptedGroupKey.data() + keySize, sizeof(h));

                bool isHashGood = (h == Crc32c(keyBytes, keySize));
                return isHashGood;
            }
    }
    Y_ABORT("Unexpected Encryption Mode# %" PRIu64, (ui64)encryptionMode);
}

const TBlobStorageGroupInfo::IQuorumChecker& TBlobStorageGroupInfo::GetQuorumChecker() const {
    return Topology->GetQuorumChecker();
}

TVDiskID TBlobStorageGroupInfo::CreateVDiskID(const TVDiskIdShort &id) const {
    return TVDiskID(GroupID, GroupGeneration, id.FailRealm, id.FailDomain, id.VDisk);
}

TString TBlobStorageGroupInfo::BlobStateToString(EBlobState state) {
    switch (state) {
        case EBS_DISINTEGRATED:
            return "EBS_DISINTEGRATED";
        case EBS_UNRECOVERABLE_FRAGMENTARY:
            return "EBS_UNRECOVERABLE_FRAGMENTARY";
        case EBS_RECOVERABLE_FRAGMENTARY:
            return "EBS_RECOVERABLE_FRAGMENTARY";
        case EBS_RECOVERABLE_DOUBTED:
            return "EBS_RECOVERABLE_DOUBTED";
        case EBS_FULL:
            return "EBS_FULL";
        default:
            Y_ABORT_UNLESS(false, "Unexpected state# %" PRIu64, (ui64)state);
    }
}

TBlobStorageGroupInfo::EBlobState TBlobStorageGroupInfo::BlobState(ui32 effectiveReplicas, ui32 errorDomains) const {
    return GetTopology().BlobState(effectiveReplicas, errorDomains);
}

void TBlobStorageGroupInfo::PickSubgroup(ui32 hash, TVDiskIds *outVDisk, TServiceIds *outServiceIds) const {
    TOrderNums orderNums;
    Topology->PickSubgroup(hash, orderNums);
    for (const auto x : orderNums) {
        if (outVDisk) {
            outVDisk->push_back(GetVDiskId(x));
        }
        if (outServiceIds) {
            outServiceIds->push_back(GetActorId(x));
        }
    }
}

bool TBlobStorageGroupInfo::BelongsToSubgroup(const TVDiskID &vdisk, ui32 hash) const {
    Y_VERIFY_DEBUG_S(vdisk.GroupID == GroupID, "Expected GroupID# " << GroupID << ", given GroupID# " << vdisk.GroupID);
    Y_VERIFY_DEBUG_S(vdisk.GroupGeneration == GroupGeneration, "Expected GroupGeeration# " << GroupGeneration
            << ", given GroupGeneration# " << vdisk.GroupGeneration);
    return Topology->BelongsToSubgroup(TVDiskIdShort(vdisk), hash);
}

// Returns either vdisk idx in the blob subgroup, or BlobSubgroupSize if the vdisk is not in the blob subgroup
ui32 TBlobStorageGroupInfo::GetIdxInSubgroup(const TVDiskID &vdisk, ui32 hash) const {
    Y_VERIFY_DEBUG_S(vdisk.GroupID == GroupID, "Expected GroupID# " << GroupID << ", given GroupID# " << vdisk.GroupID);
    Y_VERIFY_DEBUG_S(vdisk.GroupGeneration == GroupGeneration, "Expected GroupGeeration# " << GroupGeneration
            << ", given GroupGeneration# " << vdisk.GroupGeneration);
    return Topology->GetIdxInSubgroup(vdisk, hash);
}

TVDiskID TBlobStorageGroupInfo::GetVDiskInSubgroup(ui32 idxInSubgroup, ui32 hash) const {
    auto shortId = Topology->GetVDiskInSubgroup(idxInSubgroup, hash);
    return TVDiskID(GroupID, GroupGeneration, shortId);
}

ui32 TBlobStorageGroupInfo::GetOrderNumber(const TVDiskID &vdisk) const {
    Y_VERIFY_S(vdisk.GroupID == GroupID, "Expected GroupID# " << GroupID << ", given GroupID# " << vdisk.GroupID);
    Y_VERIFY_S(vdisk.GroupGeneration == GroupGeneration, "Expected GroupGeneration# " << GroupGeneration
            << ", given GroupGeneration# " << vdisk.GroupGeneration);
    return Topology->GetOrderNumber(vdisk);
}

ui32 TBlobStorageGroupInfo::GetOrderNumber(const TVDiskIdShort &vdisk) const {
    return Topology->GetOrderNumber(vdisk);
}

bool TBlobStorageGroupInfo::IsValidId(const TVDiskID &vdisk) const {
    return Topology->IsValidId(vdisk);
}

bool TBlobStorageGroupInfo::IsValidId(const TVDiskIdShort  &vdisk) const {
    return Topology->IsValidId(vdisk);
}

ui32 TBlobStorageGroupInfo::GetFailDomainOrderNumber(const TVDiskID& vdisk) const {
    return Topology->GetFailDomainOrderNumber(vdisk);
}

ui32 TBlobStorageGroupInfo::GetFailDomainOrderNumber(const TVDiskIdShort& vdisk) const {
    return Topology->GetFailDomainOrderNumber(vdisk);
}

TVDiskID TBlobStorageGroupInfo::GetVDiskId(ui32 orderNumber) const {
    return TVDiskID(GroupID, GroupGeneration, Topology->GetVDiskId(orderNumber));
}

TVDiskID TBlobStorageGroupInfo::GetVDiskId(const TVDiskIdShort &vd) const {
    return TVDiskID(GroupID, GroupGeneration, vd);
}

TActorId TBlobStorageGroupInfo::GetActorId(ui32 orderNumber) const {
    return Dynamic.ServiceIdForOrderNumber[orderNumber];
}

TActorId TBlobStorageGroupInfo::GetActorId(const TVDiskIdShort &vd) const {
    return GetActorId(Topology->GetOrderNumber(vd));
}

ui32 TBlobStorageGroupInfo::GetTotalVDisksNum() const {
    return Topology->GetTotalVDisksNum();
}

ui32 TBlobStorageGroupInfo::GetTotalFailDomainsNum() const {
    return Topology->GetTotalFailDomainsNum();
}

ui32 TBlobStorageGroupInfo::GetNumVDisksPerFailDomain() const {
    return Topology->GetNumVDisksPerFailDomain();
}

const TBlobStorageGroupInfo::TFailDomain& TBlobStorageGroupInfo::GetFailDomain(const TVDiskID& vdisk) const {
    return Topology->GetFailDomain(vdisk);
}

const TBlobStorageGroupInfo::TFailDomain& TBlobStorageGroupInfo::GetFailDomain(ui32 failDomainOrderNumber) const {
    return Topology->GetFailDomain(failDomainOrderNumber);
}

TString TBlobStorageGroupInfo::ToString() const {
    TStringStream str;
    str << "{GroupID# " << GroupID;
    str << " GroupGeneration# " << GroupGeneration;
    str << " Type# " << Type.ToString();
    str << " FailRealms# {";
    for (ui32 realmIdx = 0; realmIdx < Topology->FailRealms.size(); ++realmIdx) {
        const TFailRealm& realm = Topology->FailRealms[realmIdx];
        str << (realmIdx ? " " : "") << "[" << realmIdx << "]# TFailRealm{ FailDomains# {";
        for (ui32 domainIdx = 0; domainIdx < realm.FailDomains.size(); ++domainIdx) {
            const TFailDomain& domain = realm.FailDomains[domainIdx];
            str << (domainIdx ? " " : "") << "[" << domainIdx << "]# TFailDomain{";
            str << "FailDomainOrderNumber# " << domain.FailDomainOrderNumber
                << " VDisks# {";
            for (ui32 vdiskIdx = 0; vdiskIdx < domain.VDisks.size(); ++vdiskIdx) {
                const TVDiskInfo& info = domain.VDisks[vdiskIdx];
                str << (vdiskIdx ? " " : "") << "[" << vdiskIdx << "]# TVDiskInfo{"
                    << "ActorId# " << GetActorId(info.OrderNumber)
                    << " VDiskId# " << GetVDiskId(info.OrderNumber)
                    << " OrderNumber# " << info.OrderNumber
                    << " FailDomainOrderNumber# " << info.FailDomainOrderNumber
                    << "}";
            }
            str << "}";
        }
        str << "}}";
    }
    str << "}}";
    return str.Str();
}



TVDiskID VDiskIDFromVDiskID(const NKikimrBlobStorage::TVDiskID &x) {
    return TVDiskID(TGroupId::FromProto(&x, &NKikimrBlobStorage::TVDiskID::GetGroupID), x.GetGroupGeneration(), x.GetRing(), x.GetDomain(), x.GetVDisk());
}

void VDiskIDFromVDiskID(const TVDiskID &id, NKikimrBlobStorage::TVDiskID *proto) {
    proto->SetGroupID(id.GroupID.GetRawId());
    proto->SetGroupGeneration(id.GroupGeneration);
    proto->SetRing(id.FailRealm);
    proto->SetDomain(id.FailDomain);
    proto->SetVDisk(id.VDisk);
}

TVDiskID VDiskIDFromString(TString str, bool* isGenerationSet) {
    if (str[0] != '[' || str.back() != ']') {
        return TVDiskID::InvalidId;
    }
    str.pop_back();
    str.erase(str.begin());
    TVector<TString> parts = SplitString(str, ":");
    if (parts.size() != 5) {
        return TVDiskID::InvalidId;
    }

    ui32 groupGeneration = 0;

    if (!IsHexNumber(parts[0]) || !IsNumber(parts[2]) || !IsNumber(parts[3]) || !IsNumber(parts[4])
        || !(IsNumber(parts[1]) || parts[1] == "_")) {
        return TVDiskID::InvalidId;
    }

    if (parts[1] == "_") {
        if (isGenerationSet) {
            *isGenerationSet = false;
        }
    } else {
        if (isGenerationSet) {
            *isGenerationSet = true;
        }
        groupGeneration = IntFromString<ui32, 10>(parts[1]);
    }
    return TVDiskID(TGroupId::FromValue(IntFromString<ui32, 16>(parts[0])),
        groupGeneration,
        IntFromString<ui8, 10>(parts[2]),
        IntFromString<ui8, 10>(parts[3]),
        IntFromString<ui8, 10>(parts[4]));
}


TFailDomain::TLevelIds::TLevelIds() {
}

bool TFailDomain::TLevelIds::IsEmpty() const {
    return Ids.empty();
}

bool TFailDomain::TLevelIds::operator==(const TLevelIds& other) const {
    if (Ids.size() != other.Ids.size()) {
        return false;
    }
    return (memcmp(Ids.data(), other.Ids.data(), Ids.size()) == 0);
}

bool TFailDomain::TLevelIds::operator<(const TLevelIds& other) const {
    TVector<ui8>::const_iterator a = Ids.begin();
    TVector<ui8>::const_iterator b = other.Ids.begin();
    while (true) {
        if (a == Ids.end()) {
            if (b == other.Ids.end()) {
                return false;
            }
            return true;
        }
        if (b == other.Ids.end()) {
            return false;
        }
        if (*a < *b) {
            return true;
        }
        if (*a > *b) {
            return false;
        }
        ++a;
        ++b;
    }
}

TFailDomain::TFailDomain() {
}

TFailDomain::TFailDomain(const TString &data) {
    ui8 *end = (ui8*)const_cast<char*>(data.data()) + (data.size() / RecordSize) * RecordSize;
    for (ui8 *cursor = (ui8*)const_cast<char*>(data.data()); cursor < end; cursor += RecordSize) {
        Levels[*cursor] = ReadUnaligned<ui32>((ui32*)(cursor + 1));
    }
}

TString TFailDomain::SerializeFailDomain() const {
    TString data = TString::Uninitialized(RecordSize * Levels.size());
    TLevels::const_iterator a = Levels.begin();
    size_t offset = 0;
    while (a != Levels.end()) {
        *(ui8*)(data.data() + offset) = a->first;
        WriteUnaligned<ui32>((ui32*)(data.data() + offset + sizeof(ui8)), a->second);
        offset += RecordSize;
        ++a;
    }
    return data;
}

TFailDomain::TLevelIds TFailDomain::MakeIds() const {
    TLevelIds id;
    TLevels::const_iterator it = Levels.begin();
    while (it != Levels.end()) {
        id.Ids.push_back(it->first);
        ++it;
    }
    return id;
}

TFailDomain::TLevelIds TFailDomain::Intersect(const TLevelIds &id) const {
    TLevelIds result;
    TVector<ui8>::const_iterator a = id.Ids.begin();
    TLevels::const_iterator b = Levels.begin();
    while (a != id.Ids.end() && b != Levels.end()) {
        if (*a == b->first) {
            result.Ids.push_back(*a);
            ++a;
            ++b;
        } else if (*a < b->first) {
            ++a;
        } else {
            ++b;
        }
    }
    return result;
}

bool TFailDomain::IsColliding(const TFailDomain &other) const {
    TLevels::const_iterator a = Levels.begin();
    TLevels::const_iterator b = other.Levels.begin();
    while (a != Levels.end() && b != other.Levels.end()) {
        if (a->first < b->first) {
            ++a;
        } else if (b->first < a->first) {
            ++b;
        } else {
            if (a->second != b->second) {
                return false;
            }
            ++a;
            ++b;
        }
    }
    return true;
}

bool TFailDomain::IsSubdomainOf(const TFailDomain &other) const {
    TLevels::const_iterator a = Levels.begin();
    TLevels::const_iterator b = other.Levels.begin();
    while (a != Levels.end() && b != other.Levels.end()) {
        if (a->first < b->first) {
            a++;
        } else if (b->first < a->first) {
            return false;
        } else {
            if (a->second != b->second) {
                return false;
            }
            ++a;
            ++b;
        }
    }
    if (a != Levels.end() && b == other.Levels.end()) {
        return false;
    }
    return true;
}

bool TFailDomain::IsEqual(const TFailDomain &other) const {
    TLevels::const_iterator a = Levels.begin();
    TLevels::const_iterator b = other.Levels.begin();
    while (a != Levels.end() && b != other.Levels.end()) {
        if (a->first != b->first || a->second != b->second) {
            return false;
        }
        ++a;
        ++b;
    }
    return true;
}

bool TFailDomain::IsDifferentAt(const TLevelIds &id, const TFailDomain &other) const {
    TVector<ui8>::const_iterator key = id.Ids.begin();
    TLevels::const_iterator a = Levels.begin();
    TLevels::const_iterator b = other.Levels.begin();

    while (key != id.Ids.end()) {
        while (true) {
            if (a == Levels.end()) {
                Y_ABORT("Not enough a levels for FailDomain comparison");
            }
            if (a->first < *key) {
                ++a;
            } else if (a->first == *key) {

                while (true) {
                    if (b == other.Levels.end()) {
                        Y_ABORT("Not enough b levels for FailDomain comparison");
                    }
                    if (b->first < *key) {
                        ++b;
                    } else if (b->first == *key) {
                        if (a->second != b->second) {
                            return true;
                        }
                        ++key;
                        ++a;
                        ++b;
                        break;
                    } else {
                        Y_ABORT("Missing b level for FailDomain comparison");
                    }
                }
                break;
            } else {
                Y_ABORT("Missing a level for FailDomain comparison");
            }
        }
    }
    return false;
}

bool TFailDomain::operator<(const TFailDomain &other) const {
    TLevels::const_iterator a = Levels.begin();
    TLevels::const_iterator b = other.Levels.begin();
    while (a != Levels.end() && b != other.Levels.end()) {
        if (a->first < b->first) {
            return true;
        } else if (a->first > b->first) {
            return false;
        } else {
            if (a->second < b->second) {
                return true;
            } else if (a->second > b->second) {
                return false;
            }
            ++a;
            ++b;
        }
    }
    if (b != other.Levels.end()) {
        return true;
    }
    return false;
}

TString TFailDomain::ToString() const {
    TStringBuilder builder;
    builder << "[";
    for (TLevels::const_iterator a = Levels.begin(); a != Levels.end(); ++a) {
        builder << "{" << (ui32)a->first << ":" << (ui32)a->second << "}";
    }
    builder << "]";
    return builder;
}

TFailDomain TFailDomain::Slice(ui32 level) const {
    TFailDomain res;
    for (const auto& [key, value] : Levels) {
        if (key < level) {
            res.Levels.emplace_hint(res.Levels.end(), key, value);
        }
    }
    return res;
}

}

#include "blobstorage_ingress.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

#include <library/cpp/pop_count/popcount.h>

#include <util/generic/bitops.h>

namespace NKikimr {

    using namespace NMatrix;


    ////////////////////////////////////////////////////////////////////////////
    // TIngressCache -- precalculate some common parts to operate faster
    ////////////////////////////////////////////////////////////////////////////
    TIngressCachePtr TIngressCache::Create(std::shared_ptr<TBlobStorageGroupInfo::TTopology> top, const TVDiskIdShort &vdisk) {
        // vdiskOrderNum
        ui32 vdiskOrderNum = top->GetOrderNumber(vdisk);
        Y_ABORT_UNLESS(vdiskOrderNum < MaxVDisksInGroup);

        // totalVDisks
        ui32 totalVDisks = top->GetTotalVDisksNum();
        Y_ABORT_UNLESS(totalVDisks <= MaxVDisksInGroup);

        // domainsNum and disksInDomain
        ui32 domainsNum = top->GetTotalFailDomainsNum();
        ui32 disksInDomain = top->GetNumVDisksPerFailDomain();

        Y_ABORT_UNLESS(domainsNum * disksInDomain == totalVDisks, "domainsNum# %" PRIu32 " disksInDomain# %" PRIu32
                " totalVDisks# %" PRIu32 " erasure# %s", domainsNum, disksInDomain, totalVDisks,
                TBlobStorageGroupType::ErasureName[top->GType.GetErasure()].data());

        // handoff
        ui32 handoff = top->GType.Handoff();
        Y_ABORT_UNLESS(handoff < domainsNum);

        // barrierIngressValueMask
        ui32 barrierIngressValueMask = (1ull << totalVDisks) - 1;

        // barrierIngressDomainMask
        ui32 barrierIngressDomainMask = (1ull << disksInDomain) - 1;

        return new TIngressCache(vdiskOrderNum, totalVDisks, domainsNum, disksInDomain, handoff,
                                 barrierIngressValueMask, barrierIngressDomainMask, std::move(top));
    }

    TIngressCache::TIngressCache(ui32 vdiskOrderNum, ui32 totalVDisks, ui32 domainsNum,
                                 ui32 disksInDomain, ui32 handoff, ui32 barrierIngressValueMask,
                                 ui32 barrierIngressDomainMask, std::shared_ptr<TBlobStorageGroupInfo::TTopology> topology)
        : VDiskOrderNum(vdiskOrderNum)
        , TotalVDisks(totalVDisks)
        , DomainsNum(domainsNum)
        , DisksInDomain(disksInDomain)
        , Handoff(handoff)
        , BarrierIngressValueMask(barrierIngressValueMask)
        , BarrierIngressDomainMask(barrierIngressDomainMask)
        , Topology(std::move(topology))
    {}


    ////////////////////////////////////////////////////////////////////////////
    // TIngress -- sync info for LogoBlobs
    ////////////////////////////////////////////////////////////////////////////

#define SETUP_VECTORS(data, gtype) \
    ui32 totalParts = (gtype).TotalPartCount(); \
    ui32 start = 2; \
    ui8* const dataPtr = (ui8*)(&(data));\
    TShiftedMainBitVec main(dataPtr, start, (start + totalParts)); \
    start += totalParts;    \
    TShiftedMainBitVec local(dataPtr, start, (start + totalParts)); \
    start += totalParts;    \
    ui32 handoffNum = (gtype).Handoff(); \
    Y_DEBUG_ABORT_UNLESS(handoffNum <= MaxHandoffNodes); \
    const ui32 handoffVectorBits = totalParts * 2; \
    Y_DEBUG_ABORT_UNLESS(start + handoffNum * handoffVectorBits <= sizeof(data) * NMatrix::BitsInByte); \
    TShiftedHandoffBitVec handoff[MaxHandoffNodes]; \
    { \
        for (unsigned i = 0; i < handoffNum; i++) { \
            handoff[i] = TShiftedHandoffBitVec(dataPtr, start, start + handoffVectorBits); \
            start += handoffVectorBits; \
        } \
    }

    TIngress::EMode TIngress::IngressMode(TBlobStorageGroupType gtype) {
        switch (gtype.GetErasure()) {
            case TBlobStorageGroupType::ErasureMirror3of4:
                return EMode::MIRROR3OF4;
            default:
                return EMode::GENERIC;
        }
    }

    TIngress::TIngress(ui64 rawData) {
        Data = rawData;
    }

    bool TIngress::KeepUnconditionally(EMode ingressMode) const {
        return GetCollectMode(ingressMode) == CollectModeKeep;
    }

    void TIngress::SetKeep(EMode ingressMode, ECollectMode mode) {
        ui8& b = reinterpret_cast<ui8&>(Data);
        switch (ingressMode) {
            case EMode::GENERIC:
                Y_DEBUG_ABORT_UNLESS((b >> 6) == 0);
                b |= ui8(mode) << 6;
                break;
            case EMode::MIRROR3OF4:
                Data |= static_cast<ui64>(mode) << 62;
                break;
        }
    }

    int TIngress::GetCollectMode(EMode ingressMode) const {
        switch (ingressMode) {
            case EMode::GENERIC:
                return reinterpret_cast<const ui8&>(Data) >> 6;
            case EMode::MIRROR3OF4:
                return Data >> 62;
        }
    }

    bool TIngress::MustKnowAboutLogoBlob(const TBlobStorageGroupInfo::TTopology *top,
                                         const TVDiskIdShort &vdisk,
                                         const TLogoBlobID &id) {
        return top->GetIdxInSubgroup(vdisk, id.Hash()) != top->GType.BlobSubgroupSize();
    }

    TMaybe<TIngress> TIngress::CreateIngressWithLocal(const TBlobStorageGroupInfo::TTopology *top,
                                                      const TVDiskIdShort &vdisk,
                                                      const TLogoBlobID &id) {
        const ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());
        return CreateIngressInternal(top->GType, nodeId, id, true); // create local bits
    }

    TMaybe<TIngress> TIngress::CreateIngressWOLocal(const TBlobStorageGroupInfo::TTopology *top,
                                                    const TVDiskIdShort &vdisk,
                                                    const TLogoBlobID &id) {
        const ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());
        return CreateIngressInternal(top->GType, nodeId, id, false); // w/o local bits
    }

    TMaybe<TIngress> TIngress::CreateIngressInternal(TBlobStorageGroupType gtype,
                                                     const ui8 nodeId,
                                                     const TLogoBlobID &id,
                                                     const bool setUpLocalBits) {
        if (nodeId == gtype.BlobSubgroupSize()) {
            return Nothing();
        }

        switch (IngressMode(gtype)) {
            case EMode::GENERIC: {
                TIngress ingress;
                const ui8 subgroupSz = gtype.BlobSubgroupSize();
                Y_DEBUG_ABORT_UNLESS(subgroupSz <= MaxNodesPerBlob);
                SETUP_VECTORS(ingress.Data, gtype);
                if (0 < id.PartId() && id.PartId() < totalParts + 1u) {
                    // good
                    ui8 part = id.PartId() - 1u;

                    // setup local bits (i.e. 'we have data for this part')
                    if (setUpLocalBits) {
                        local.Set(part);
                    }
                    // setup ingress bits (i.e. 'we know about this part')
                    if (nodeId < totalParts) {
                        if (nodeId != part) {
                            return TMaybe<TIngress>();
                        }
                        main.Set(part);
                    } else {
                        handoff[nodeId - totalParts].Set(part);
                    }
                    return ingress;
                } else {
                    // bad
                    return Nothing();
                }
            }
            case EMode::MIRROR3OF4: {
                if (id.PartId() == 0) {
                    return Nothing(); // incorrect blob id
                }
                const ui8 partIdx = id.PartId() - 1;
                if (partIdx >= gtype.TotalPartCount()) {
                    return Nothing(); // incorrect part id
                }
                const auto& v = NMatrix::TVectorType::MakeOneHot(partIdx, gtype.TotalPartCount());
                ui64 raw = 0;
                if (setUpLocalBits) {
                    raw |= static_cast<ui64>(v.Raw()) << (62 - 8);
                }
                raw |= static_cast<ui64>(v.Raw()) << (62 - 8 - gtype.TotalPartCount() * (1 + nodeId));
                return TIngress(raw);
            }
        }
    }

    TVectorType TIngress::PartsWeKnowAbout(TBlobStorageGroupType gtype) const {
        NMatrix::TVectorType res(0, gtype.TotalPartCount());
        for (ui8 i = 0; i < gtype.BlobSubgroupSize(); ++i) {
            res |= KnownParts(gtype, i);
        }
        return res;
    }

    TVectorType TIngress::PartsWeMustHaveLocally(const TBlobStorageGroupInfo::TTopology *top,
                                                 const TVDiskIdShort &vdisk,
                                                 const TLogoBlobID &id) const {
        return KnownParts(top->GType, top->GetIdxInSubgroup(vdisk, id.Hash()));
    }

    TIngress::TPairOfVectors TIngress::HandoffParts(const TBlobStorageGroupInfo::TTopology *top,
                                                    const TVDiskIdShort &vdisk,
                                                    const TLogoBlobID &id) const {
        Y_ABORT_UNLESS(IngressMode(top->GType) == EMode::GENERIC);

        // FIXME: think how we merge ingress (especially for handoff replicas) (when we delete parts)
        Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
        SETUP_VECTORS(Data, top->GType);

        ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());
        if (nodeId < totalParts) {
            TVectorType emptyVec(0, totalParts);
            return TPairOfVectors(emptyVec, emptyVec);
        } else {
            ui8 handoffNodeId = nodeId - totalParts;
            Y_DEBUG_ABORT_UNLESS(handoffNodeId < handoffNum);

            TVectorType m = handoff[handoffNodeId].ToVector(); // map of handoff replicas on this node
            TVectorType mainVec = main.ToVector();
            TVectorType toMove = m - mainVec;   // what we can send to main replicas
            TVectorType deleted = GetVDiskHandoffDeletedVec(top, vdisk, id);
            TVectorType toDel = m & mainVec & ~deleted;  // not deleted, what we can to delete
            return TPairOfVectors(toMove, toDel);
        }
    }

    NMatrix::TVectorType TIngress::GetVDiskHandoffVec(const TBlobStorageGroupInfo::TTopology *top,
                                                 const TVDiskIdShort &vdisk,
                                                 const TLogoBlobID &id) const {
        Y_ABORT_UNLESS(IngressMode(top->GType) == EMode::GENERIC);
        Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
        SETUP_VECTORS(Data, top->GType);

        ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());

        if (nodeId < totalParts) {
            return TVectorType(0, totalParts);
        }

        ui8 handoffNodeId = nodeId - totalParts;
        Y_DEBUG_ABORT_UNLESS(handoffNodeId < handoffNum);
        return handoff[handoffNodeId].ToVector();
    }

    NMatrix::TVectorType TIngress::GetVDiskHandoffDeletedVec(const TBlobStorageGroupInfo::TTopology *top,
                                                 const TVDiskIdShort &vdisk,
                                                 const TLogoBlobID &id) const {
        Y_ABORT_UNLESS(IngressMode(top->GType) == EMode::GENERIC);
        Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
        SETUP_VECTORS(Data, top->GType);

        ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());

        if (nodeId < totalParts) {
            return TVectorType(0, totalParts);
        }

        ui8 handoffNodeId = nodeId - totalParts;
        Y_DEBUG_ABORT_UNLESS(handoffNodeId < handoffNum);
        return handoff[handoffNodeId].DeletedPartsVector();
    }

    NMatrix::TVectorType TIngress::LocalParts(TBlobStorageGroupType gtype) const {
        switch (IngressMode(gtype)) {
            case EMode::GENERIC: {
                SETUP_VECTORS(Data, gtype);
                return local.ToVector();
            }
            case EMode::MIRROR3OF4:
                return NMatrix::TVectorType(Data >> (62 - 8), gtype.TotalPartCount());
        }
    }

    NMatrix::TVectorType TIngress::KnownParts(TBlobStorageGroupType gtype, ui8 nodeId) const {
        const ui8 numParts = gtype.TotalPartCount();
        Y_DEBUG_ABORT_UNLESS(nodeId < gtype.BlobSubgroupSize());
        switch (IngressMode(gtype)) {
            case EMode::GENERIC: {
                SETUP_VECTORS(Data, gtype);
                if (nodeId < numParts) { // main disk
                    return main.Get(nodeId)
                        ? NMatrix::TVectorType::MakeOneHot(nodeId, numParts)
                        : NMatrix::TVectorType(0, numParts);
                } else { // handoff disk
                    return handoff[nodeId - numParts].ToVector();
                }
            }
            case EMode::MIRROR3OF4:
                // CollectMode[2] Local[numParts] Disk0[numParts] Disk1[numParts] ... Disk7[numParts]
                return NMatrix::TVectorType(Data >> (62 - 8 - numParts * (1 + nodeId)), numParts);
        }
    }

    TVDiskIdShort TIngress::GetMainReplica(const TBlobStorageGroupInfo::TTopology *top, const TLogoBlobID &id) {
        Y_ABORT_UNLESS(IngressMode(top->GType) == EMode::GENERIC);

        Y_DEBUG_ABORT_UNLESS(id.PartId() != 0);
        ui8 partId = id.PartId();
        return top->GetVDiskInSubgroup(partId - 1, id.Hash());
    }

    void TIngress::DeleteHandoff(const TBlobStorageGroupInfo::TTopology *top,
                                 const TVDiskIdShort &vdisk,
                                 const TLogoBlobID &id,
                                 bool deleteLocal) {
        Y_ABORT_UNLESS(IngressMode(top->GType) == EMode::GENERIC);

        Y_DEBUG_ABORT_UNLESS(id.PartId() != 0);
        SETUP_VECTORS(Data, top->GType);

        ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());
        Y_ABORT_UNLESS(nodeId >= totalParts, "DeleteHandoff: can't delete main replica; nodeId# %u totalParts# %u vdisk# %s "
               "id# %s", unsigned(nodeId), unsigned(totalParts), vdisk.ToString().data(), id.ToString().data());

        ui8 handoffNodeId = nodeId - totalParts;
        Y_DEBUG_ABORT_UNLESS(handoffNodeId < handoffNum);

        ui8 i = id.PartId() - 1u;
        handoff[handoffNodeId].Delete(i);   // delete handoff
        if (deleteLocal) {
            local.Clear(i);
        }
    }

    // Make a copy of ingress w/o local bits
    TIngress TIngress::CopyWithoutLocal(TBlobStorageGroupType gtype) const {
        switch (IngressMode(gtype)) {
            case EMode::GENERIC: {
                TIngress res;
                res.Data = Data;

                SETUP_VECTORS(res.Data, gtype);
                for (ui32 i = 0; i < totalParts; i++)
                    local.Clear(i);

                return res;
            }
            case EMode::MIRROR3OF4: {
                const ui64 mask = ((static_cast<ui64>(1) << gtype.TotalPartCount()) - 1) << (62 - gtype.TotalPartCount());
                return TIngress(Data & ~mask);
            }
        }
    }

    TString TIngress::ToString(const TBlobStorageGroupInfo::TTopology *top,
                              const TVDiskIdShort &vdisk,
                              const TLogoBlobID &id) const {
        switch (IngressMode(top->GType)) {
            case EMode::GENERIC: {
                TStringStream str;
                SETUP_VECTORS(Data, top->GType);
                str << "{";
                // find nodeId, get list of vdisks/services
                ui8 nodeId = top->GetIdxInSubgroup(vdisk, id.Hash());

                str << "nodeId: " << ui32(nodeId);

                {
                    str << " main: ";
                    TVectorType vec = main.ToVector();
                    for (ui8 i = 0; i < totalParts; i++) {
                        if (i)
                            str << " ";
                        str << vec.Get(i);
                    }
                }
                {
                    for (ui8 hf = 0; hf < handoffNum; hf++) {
                        str << " handoff" << ui32(hf) << ": ";
                        for (ui8 i = 0; i < totalParts; i++) {
                            if (i)
                                str << " ";
                            ui8 elem = handoff[hf].GetRaw(i);
                            str << ((elem & 0x2) >> 1) << (elem & 0x1);
                        }
                    }
                }
                {
                    str << " local: ";
                    TVectorType vec = local.ToVector();
                    for (ui8 i = 0; i < totalParts; i++) {
                        if (i)
                            str << " ";
                        str << vec.Get(i);
                    }
                }
                str << " " << CollectMode2String(GetCollectMode(TIngress::IngressMode(top->GType)));
                str << "}";
                return str.Str();
            }
            case EMode::MIRROR3OF4: {
                TStringBuilder s;
                s << "{nodeId: " << top->GetIdxInSubgroup(vdisk, id.Hash()) << " ";
                ui64 r = Data << 2;
                auto printGroup = [&](TString name) {
                    s << name << ": ";
                    for (unsigned i = 0; i < top->GType.TotalPartCount(); ++i, r <<= 1) {
                        s << (r >> 63);
                    }
                    s << " ";
                };
                printGroup("local");
                for (unsigned i = 0; i < top->GType.BlobSubgroupSize(); ++i) {
                    printGroup(Sprintf("node%u", i));
                }
                return s << CollectMode2String(GetCollectMode(EMode::MIRROR3OF4)) << "}";
            }
        }
    }

    TString TIngress::PrintVDisksForLogoBlob(const TBlobStorageGroupInfo *info, const TLogoBlobID &id) {
        Y_DEBUG_ABORT_UNLESS(id.PartId() == 0);
        TBlobStorageGroupInfo::TVDiskIds outVDisks;
        info->PickSubgroup(id.Hash(), &outVDisks, nullptr);
        TStringStream str;
        for (ui8 i = 0; i < outVDisks.size(); i++) {
            if (i)
                str << " ";
            str << outVDisks[i].ToString();
        }
        return str.Str();
    }

    void TIngress::Merge(TIngress &myIngress, const TIngress &newIngress) {
        myIngress.Data |= newIngress.Data;
    }

    TIngress TIngress::CreateFromRepl(const TBlobStorageGroupInfo::TTopology *top,
                                      const TVDiskIdShort& vdisk,
                                      const TLogoBlobID& id,
                                      NMatrix::TVectorType recoveredParts) {
        TIngress res;
        Y_ABORT_UNLESS(id.PartId() == 0);
        for (ui8 i = recoveredParts.FirstPosition(); i != recoveredParts.GetSize(); i = recoveredParts.NextPosition(i)) {
            res.Merge(*CreateIngressWithLocal(top, vdisk, TLogoBlobID(id, i + 1)));
        }
        return res;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TBarrierIngress -- sync info for Barriers (garbage collection)
    ////////////////////////////////////////////////////////////////////////////
    TBarrierIngress::TBarrierIngress()
        : Data(0)
    {}

    TBarrierIngress::TBarrierIngress(ui8 id) {
        Data = 0;
        // set up
        TBarrierIngress::SetUp(this, id);
    }

    TBarrierIngress::TBarrierIngress(const TIngressCache *cache) {
        Data = 0;
        // set up
        TBarrierIngress::SetUp(this, cache);
    }

    TBarrierIngress TBarrierIngress::CreateFromRaw(ui32 raw) {
        TBarrierIngress ingress;
        ingress.Data = raw;
        return ingress;
    }

    bool TBarrierIngress::IsQuorum(const TIngressCache *cache) const {
        /*
           BlobStorage Group
           fd -- fail domain, i.e. rack
           0, 1, 2, etc -- vdisk order number

            fd0     fd1     fd2     fd3
           +---+   +---+   +---+   +---+
           | 0 |   | 2 |   | 4 |   | 6 |
           +---+   +---+   +---+   +---+
           +---+   +---+   +---+   +---+
           | 1 |   | 3 |   | 5 |   | 7 |
           +---+   +---+   +---+   +---+

           When we syncronize barriers, we synchronize keep/dontkeep flags.
           Our purpose is _not to miss these flags_. How to achieve this?
           We need that barrier record were present at 'majority' of disks.
           What is 'majority'? Simple TotalVDisks / 2 + 1 is not enough
           because of our data layout (we put each part in its own faildomain).
           So, we need majority in _terms of faildomains_, i.e. number of fully
           synced racks minus handoff parts.
         */

        auto& topology = *cache->Topology;
        auto synced = TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(&topology, Data & cache->BarrierIngressValueMask);
        return topology.GetQuorumChecker().CheckQuorumForGroup(synced);
    }

    TString TBarrierIngress::ToString(const TIngressCache *cache) const {
        /*
           BlobStorage Group
           fd -- fail domain, i.e. rack
           0, 1, 2, etc -- vdisk order number

            fd0     fd1     fd2     fd3
           +---+   +---+   +---+   +---+
           | 0 |   | 2 |   | 4 |   | 6 |
           +---+   +---+   +---+   +---+
           +---+   +---+   +---+   +---+
           | 1 |   | 3 |   | 5 |   | 7 |
           +---+   +---+   +---+   +---+

           Output format. Lets say all disks have barrier set except of 5 and 6,
           and our disk order number is 2. Than output will be

           [11][*1*1][10][01]

        */
        TStringStream str;
        for (ui32 i = 0; i < cache->DomainsNum; i++) {
            str << "[";
            for (ui32 j = 0; j < cache->DisksInDomain; j++) {
                ui32 vdiskOrderNum = (i * cache->DisksInDomain + j);
                ui32 exist = !!(Data & (1 << vdiskOrderNum));
                // recognize self in the output
                const char *framing = (vdiskOrderNum == cache->VDiskOrderNum) ? "*" : "";
                str << framing << exist << framing;
            }
            str << "]";
        }
        return str.Str();
    }

    void TBarrierIngress::Merge(TBarrierIngress &myIngress, const TBarrierIngress &newIngress) {
        myIngress.Data |= newIngress.Data;
    }

    void TBarrierIngress::SetUp(TBarrierIngress *ingress, ui8 id) {
        Y_DEBUG_ABORT_UNLESS(id < MaxVDisksInGroup);
        ingress->Data = (1u << ui32(id));
    }

    void TBarrierIngress::SetUp(TBarrierIngress *ingress, const TIngressCache *cache) {
        ui8 id = cache->VDiskOrderNum;
        SetUp(ingress, id);
    }

    void TBarrierIngress::CheckBlobStorageGroup(const TBlobStorageGroupInfo *info) {
        const ui32 num = info->GetTotalVDisksNum();
        Y_ABORT_UNLESS(num <= MaxVDisksInGroup, "Number of vdisks in group is too large; MaxVDisksInGroup# %" PRIu32
               " actualNum# %" PRIu32, MaxVDisksInGroup, num);
    }

    ////////////////////////////////////////////////////////////////////////////
    // We check vdisk group for correspondence with ingress facilities
    ////////////////////////////////////////////////////////////////////////////
    void IngressGlobalCheck(const TBlobStorageGroupInfo *info) {
        TBarrierIngress::CheckBlobStorageGroup(info);
    }


} // NKikimr

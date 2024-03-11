#include "statestorage.h"
#include "tabletid.h"
#include <util/generic/xrange.h>
#include <util/generic/mem_copy.h>
#include <util/generic/algorithm.h>

namespace NKikimr {

static const ui32 Primes[128] = {
    104743, 105023, 105359, 105613,
    104759, 105031, 105361, 105619,
    104761, 105037, 105367, 105649,
    104773, 105071, 105373, 105653,
    104779, 105097, 105379, 105667,
    104789, 105107, 105389, 105673,
    104801, 105137, 105397, 105683,
    104803, 105143, 105401, 105691,
    104827, 105167, 105407, 105701,
    104831, 105173, 105437, 105727,
    104849, 105199, 105449, 105733,
    104851, 105211, 105467, 105751,
    104869, 105227, 105491, 105761,
    104879, 105229, 105499, 105767,
    104891, 105239, 105503, 105769,
    104911, 105251, 105509, 105817,
    104917, 105253, 105517, 105829,
    104933, 105263, 105527, 105863,
    104947, 105269, 105529, 105871,
    104953, 105277, 105533, 105883,
    104959, 105319, 105541, 105899,
    104971, 105323, 105557, 105907,
    104987, 105331, 105563, 105913,
    104999, 105337, 105601, 105929,
    105019, 105341, 105607, 105943,
    105953, 106261, 106487, 106753,
    105967, 106273, 106501, 106759,
    105971, 106277, 106531, 106781,
    105977, 106279, 106537, 106783,
    105983, 106291, 106541, 106787,
    105997, 106297, 106543, 106801,
    106013, 106303, 106591, 106823,
};

constexpr ui64 MaxRingCount = 1024;
constexpr ui64 MaxNodeCount = 1024;

class TStateStorageRingWalker {
    const ui32 Sz;
    const ui32 Delta;
    ui32 A;
public:
    TStateStorageRingWalker(ui32 hash, ui32 sz)
        : Sz(sz)
        , Delta(Primes[hash % 128])
        , A(hash + Delta)
    {
        Y_DEBUG_ABORT_UNLESS(Delta > Sz);
    }

    ui32 Next() {
        A += Delta;
        return (A % Sz);
    }
};

void TStateStorageInfo::SelectReplicas(ui64 tabletId, TSelection *selection) const {
    const ui32 hash = StateStorageHashFromTabletID(tabletId);
    const ui32 total = Rings.size();

    Y_ABORT_UNLESS(NToSelect <= total);

    if (selection->Sz < NToSelect) {
        selection->Status.Reset(new TStateStorageInfo::TSelection::EStatus[NToSelect]);
        selection->SelectedReplicas.Reset(new TActorId[NToSelect]);
    }

    selection->Sz = NToSelect;

    Fill(selection->Status.Get(), selection->Status.Get() + NToSelect, TStateStorageInfo::TSelection::StatusUnknown);

    if (NToSelect == total) {
        for (ui32 idx : xrange(total)) {
            selection->SelectedReplicas[idx] = Rings[idx].SelectReplica(hash);
        }
    } else { // NToSelect < total, first - select rings with walker, then select concrete node
        TStateStorageRingWalker walker(hash, total);
        for (ui32 idx : xrange(NToSelect))
            selection->SelectedReplicas[idx] = Rings[walker.Next()].SelectReplica(hash);
    }
}

TActorId TStateStorageInfo::TRing::SelectReplica(ui32 hash) const {
    if (Replicas.size() == 1)
        return Replicas[0];

    Y_ABORT_UNLESS(!Replicas.empty());
    if (UseRingSpecificNodeSelection) {
        return Replicas[CombineHashes(hash, ContentHash()) % Replicas.size()];
    } else {
        return Replicas[hash % Replicas.size()];
    }
}

TList<TActorId> TStateStorageInfo::SelectAllReplicas() const {
// TODO: we really need this method in such way?
    TList<TActorId> replicas;

    for (auto &ring : Rings) {
        for (TActorId replica : ring.Replicas)
            replicas.push_back(replica);
    }

    return replicas;
}

ui32 TStateStorageInfo::TRing::ContentHash() const {
    ui64 hash = 17;
    for (TActorId replica : Replicas) {
        hash = Hash64to32((hash << 32) | replica.Hash32());
    }
    return static_cast<ui32>(hash);
}

ui32 TStateStorageInfo::ContentHash() const {
    ui64 hash = RelaxedLoad<ui64>(&Hash);
    if (Y_UNLIKELY(hash == Max<ui64>())) {
        hash = 37;
        for (const TRing &ring : Rings) {
            hash = Hash64to32((hash << 32) | ring.ContentHash());
        }
        RelaxedStore<ui64>(&Hash, static_cast<ui32>(hash));
    }
    return static_cast<ui32>(hash);
}

void TStateStorageInfo::TSelection::MergeReply(EStatus status, EStatus *owner, ui64 targetCookie, bool resetOld) {
    ui32 unknown = 0;
    ui32 ok = 0;
    ui32 outdated = 0;

    const ui32 majority = Sz / 2 + 1;

    ui32 cookie = 0;
    for (ui32 i = 0; i < Sz; ++i) {
        EStatus &st = Status[i];
        if (resetOld && st != StatusUnknown)
            st = StatusOutdated;

        if (cookie == targetCookie)
            st = status;

        ++cookie;

        switch (st) {
        case StatusUnknown:
            ++unknown;
            break;
        case StatusOk:
            ++ok;
            break;
        case StatusNoInfo:
            break;
        case StatusOutdated:
            ++outdated;
            break;
        }
    }

    if (owner) {
        if (ok >= majority) {
            *owner = StatusOk;
        } else if (outdated >= majority) {
            *owner = StatusOutdated;
        } else if (ok + unknown < majority) {
            if (outdated)
                *owner = StatusOutdated;
            else
                *owner = StatusNoInfo;
        }
    }
}

static void CopyStateStorageRingInfo(
    const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &source,
    TStateStorageInfo *info,
    char *serviceId,
    ui32 depth
) {
    info->NToSelect = source.GetNToSelect();

    const bool hasRings = source.RingSize() > 0;
    const bool hasNodes = source.NodeSize() > 0;

    if (hasRings) { // has explicitely defined rings, use them as info rings
        Y_ABORT_UNLESS(!hasNodes);
        Y_ABORT_UNLESS(source.RingSize() < MaxRingCount);
        info->Rings.resize(source.RingSize());

        for (ui32 iring = 0, ering = source.RingSize(); iring != ering; ++iring) {
            serviceId[depth] = (iring + 1);

            const NKikimrConfig::TDomainsConfig::TStateStorage::TRing &ring = source.GetRing(iring);
            info->Rings[iring].UseRingSpecificNodeSelection = ring.GetUseRingSpecificNodeSelection();
            info->Rings[iring].IsDisabled = ring.GetIsDisabled();

            if (ring.GetUseSingleNodeActorId()) {
                Y_ABORT_UNLESS(ring.NodeSize() == 1);

                const TActorId replicaActorID = TActorId(ring.GetNode(0), TStringBuf(serviceId, serviceId + 12));
                info->Rings[iring].Replicas.push_back(replicaActorID);
            }
            else {
               Y_ABORT_UNLESS(ring.NodeSize() > 0);

               for (ui32 inode = 0, enode = ring.NodeSize(); inode != enode; ++inode) {
                    serviceId[depth + 1] = (inode + 1);
                    const TActorId replicaActorID = TActorId(ring.GetNode(inode), TStringBuf(serviceId, serviceId + 12));
                    info->Rings[iring].Replicas.push_back(replicaActorID);
               }
            }
            // reset for next ring
            serviceId[depth + 1] = char();
        }

        return;
    }

    if (hasNodes) { // has explicitely defined replicas, use nodes as 1-node rings
        Y_ABORT_UNLESS(!hasRings);
        Y_ABORT_UNLESS(source.NodeSize() < MaxNodeCount);

        info->Rings.resize(source.NodeSize());
        for (ui32 inode = 0, enode = source.NodeSize(); inode != enode; ++inode) {
            serviceId[depth] = (inode + 1);

            const TActorId replicaActorID = TActorId(source.GetNode(inode), TStringBuf(serviceId, serviceId + 12));
            info->Rings[inode].Replicas.push_back(replicaActorID);
        }

        return;
    }

    Y_ABORT("must have rings or legacy node config");
}

TIntrusivePtr<TStateStorageInfo> BuildStateStorageInfo(char (&namePrefix)[TActorId::MaxServiceIDLength], const NKikimrConfig::TDomainsConfig::TStateStorage& config) {
    TIntrusivePtr<TStateStorageInfo> info = new TStateStorageInfo();
    Y_ABORT_UNLESS(config.GetSSId() == 1);
    info->StateStorageVersion = config.GetStateStorageVersion();
    
    info->CompatibleVersions.reserve(config.CompatibleVersionsSize());
    for (ui32 version : config.GetCompatibleVersions()) {
            info->CompatibleVersions.push_back(version);
    }
    
    const size_t offset = FindIndex(namePrefix, char());
    Y_ABORT_UNLESS(offset != NPOS && (offset + sizeof(ui32)) < TActorId::MaxServiceIDLength);

    const ui32 stateStorageGroup = 1;
    memcpy(namePrefix + offset, reinterpret_cast<const char *>(&stateStorageGroup), sizeof(ui32));
    CopyStateStorageRingInfo(config.GetRing(), info.Get(), namePrefix, offset + sizeof(ui32));

    return info;
}

void BuildStateStorageInfos(const NKikimrConfig::TDomainsConfig::TStateStorage& config,
    TIntrusivePtr<TStateStorageInfo> &stateStorageInfo,
    TIntrusivePtr<TStateStorageInfo> &boardInfo,
    TIntrusivePtr<TStateStorageInfo> &schemeBoardInfo)
{
    char ssr[TActorId::MaxServiceIDLength] = { 's', 's', 'r' }; // state storage replica
    char ssb[TActorId::MaxServiceIDLength] = { 's', 's', 'b' }; // state storage board
    char sbr[TActorId::MaxServiceIDLength] = { 's', 'b', 'r' }; // scheme board replica

    stateStorageInfo = BuildStateStorageInfo(ssr, config);
    boardInfo = BuildStateStorageInfo(ssb, config);
    schemeBoardInfo = BuildStateStorageInfo(sbr, config);
}

}

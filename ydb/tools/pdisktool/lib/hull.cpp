#include "hull.h"

#include <cstring>
#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/protos/base.pb.h>

#include <algorithm>
#include <tuple>
#include <util/generic/hash_set.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NPDiskTool {

namespace {

constexpr ui32 HullEntryMagic = 0x93F7ADD5;

bool ParseHullEntryPoint(const TString& data, NKikimrVDiskData::THullDbEntryPoint& pb, TIssueLog& issues) {
    if (data.size() < sizeof(ui32)) {
        issues.Warning("hull", "Hull entry point shorter than magic", true);
        return false;
    }
    const ui32 magic = ReadUnaligned<ui32>(data.data());
    if (magic != HullEntryMagic) {
        issues.Warning("hull", TStringBuilder() << "Unknown hull entry-point magic# " << magic, true);
        return false;
    }
    if (!pb.ParseFromArray(data.data() + sizeof(ui32), data.size() - sizeof(ui32))) {
        issues.Warning("hull", "Failed to parse THullDbEntryPoint protobuf", true);
        return false;
    }
    return true;
}

TString ReadDiskPart(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const TDiskPart& part,
    TIssueLog& issues)
{
    return ReadLogicalRange(device, format, state, part.ChunkIdx, part.Offset, part.Size, issues, "sst");
}

template <class TKey, class TMemRec>
bool LoadSstLinear(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const TDiskPart& entry,
    TVector<TIndexRecord<TKey, TMemRec>>& index,
    TVector<TDiskPart>& outbound,
    TVector<TDiskPart>& visitedParts,
    TIssueLog& issues)
{
    TDiskPart cur = entry;
    bool first = true;
    ui64 restIndex = 0;
    ui64 restOutbound = 0;
    ui64 items = 0;
    ui64 outboundItems = 0;

    TVector<TString> partsNewestFirst;
    ui64 collected = 0;
    THashSet<std::tuple<ui32, ui32, ui32>> visitedLinks;
    while (!cur.Empty()) {
        // The PrevPart links come off the disk; a damaged one can point back into the chain.
        if (!visitedLinks.insert(std::make_tuple(cur.ChunkIdx, cur.Offset, cur.Size)).second) {
            issues.Warning("hull", TStringBuilder() << "SST fragment chain loops at " << cur.ToString(), true);
            return false;
        }
        visitedParts.push_back(cur);
        TString data = ReadDiskPart(device, format, state, cur, issues);
        if (data.size() < (first ? sizeof(TIdxDiskPlaceHolder) : sizeof(TIdxDiskLinker))) {
            issues.Warning("hull", TStringBuilder() << "SST fragment too small at " << cur.ToString(), true);
            return false;
        }
        if (first) {
            TIdxDiskPlaceHolder ph(0);
            memcpy(&ph, data.data() + data.size() - sizeof(TIdxDiskPlaceHolder), sizeof(TIdxDiskPlaceHolder));
            if (ph.MagicNumber != TIdxDiskPlaceHolder::Signature) {
                issues.Warning("hull", TStringBuilder() << "Bad SST placeholder magic at " << cur.ToString(), true);
                return false;
            }
            // ui64 throughout: the products below overflow ui32 for the values a damaged
            // placeholder can hold, which would leave the buffer smaller than the copies into it.
            items = ph.Info.Items;
            outboundItems = ph.Info.OutboundItems;
            restIndex = ph.Info.IdxTotalSize;
            restOutbound = outboundItems * sizeof(TDiskPart);
            partsNewestFirst.push_back(data.substr(0, data.size() - sizeof(TIdxDiskPlaceHolder)));
            cur = ph.PrevPart;
            first = false;
        } else {
            TIdxDiskLinker linker;
            memcpy(&linker, data.data() + data.size() - sizeof(TIdxDiskLinker), sizeof(TIdxDiskLinker));
            partsNewestFirst.push_back(data.substr(0, data.size() - sizeof(TIdxDiskLinker)));
            cur = linker.PrevPart;
        }
        collected += partsNewestFirst.back().size();
    }

    // The placeholder must agree with the bytes the fragments actually carry, otherwise the index
    // and outbound tables would be read from beyond what was collected.
    const ui64 total = restIndex + restOutbound;
    const ui64 indexBytes = items * sizeof(TIndexRecord<TKey, TMemRec>);
    if (total > collected || indexBytes > restIndex) {
        issues.Warning("hull", TStringBuilder() << "SST placeholder disagrees with the fragments at "
            << entry.ToString() << ": items# " << items << " idxTotalSize# " << restIndex
            << " outboundItems# " << outboundItems << " collected# " << collected, true);
        return false;
    }

    TString buf = TString::Uninitialized(total);
    memset(buf.Detach(), 0, total);
    ui64 write = total;
    for (const auto& part : partsNewestFirst) {
        if (part.size() > write) {
            issues.Warning("hull", "SST index overflow while concatenating parts", true);
            return false;
        }
        write -= part.size();
        memcpy(buf.Detach() + write, part.data(), part.size());
    }

    outbound.resize(outboundItems);
    if (outboundItems) {
        memcpy(outbound.data(), buf.data() + restIndex, restOutbound);
    }
    index.resize(items);
    if (items) {
        memcpy(index.data(), buf.data(), indexBytes);
    }
    return true;
}

static const TVector<TErasureType::EErasureSpecies> ErasureCandidates = {
    TErasureType::ErasureNone,
    TErasureType::Erasure4Plus2Block,
    TErasureType::ErasureMirror3dc,
    TErasureType::Erasure4Plus3Block,
    TErasureType::Erasure3Plus3Block,
    TErasureType::ErasureMirror3of4,
};

bool ParseLogoBlobOpt(
    const TString& raw,
    TMaybe<TErasureType::EErasureSpecies> hint,
    TLogoBlobID& id,
    TString& data,
    bool& keep,
    TMaybe<TErasureType::EErasureSpecies>& used)
{
    auto trySpecies = [&](TErasureType::EErasureSpecies s) -> bool {
        TBlobStorageGroupType gtype(s);
        if (raw.size() < sizeof(TLogoBlobID)) {
            return false;
        }
        id = ReadUnaligned<TLogoBlobID>(raw.data());
        const ui64 partSize = gtype.PartSize(id);
        if (raw.size() == sizeof(TLogoBlobID) + partSize) {
            data = raw.substr(sizeof(TLogoBlobID), partSize);
            keep = false;
            used = s;
            return true;
        }
        if (raw.size() == sizeof(TLogoBlobID) + partSize + 1) {
            data = raw.substr(sizeof(TLogoBlobID), partSize);
            keep = true;
            used = s;
            return true;
        }
        return false;
    };
    if (hint) {
        if (trySpecies(*hint)) {
            return true;
        }
    }
    for (auto s : ErasureCandidates) {
        if (hint && s == *hint) {
            continue;
        }
        if (trySpecies(s)) {
            return true;
        }
    }
    return false;
}

void UpsertBlob(TVector<TBlobIndexEntry>& blobs, const TLogoBlobID& id, const TMemRecLogoBlob& rec) {
    TBlobIndexEntry e;
    e.Id = TLogoBlobID(id, 0);
    e.MemRec = rec;
    auto it = std::lower_bound(blobs.begin(), blobs.end(), e,
        [](const TBlobIndexEntry& a, const TBlobIndexEntry& b) { return a.Id < b.Id; });
    if (it != blobs.end() && TKeyLogoBlob(it->Id).IsSameAs(TKeyLogoBlob(e.Id))) {
        if (!rec.HasData()) {
            // Metadata-only records can be folded into the entry that is already there.
            TMemRecLogoBlob merged = it->MemRec;
            merged.Merge(rec, TKeyLogoBlob(e.Id), false, TBlobStorageGroupType(TErasureType::ErasureNone));
            it->MemRec = merged;
            return;
        }
        if (!it->MemRec.HasData()) {
            it->MemRec = rec;
            return;
        }
        // Both records point at data. Two huge parts of one blob are logged separately, so keeping
        // only the newer one would drop a part; BuildBlobViews groups the entries back together.
    }
    blobs.insert(it, std::move(e));
}

} // namespace

TVector<TDiskPart> CollectReferencedParts(const THullSnapshot& snap) {
    TVector<TDiskPart> parts = snap.SstParts;
    for (const auto& e : snap.Blobs) {
        if (e.MemRec.GetType() == TBlobType::MemBlob) {
            continue;
        }
        TDiskDataExtractor extr;
        const TDiskPart* outbound = e.Outbound.empty() ? nullptr : e.Outbound.data();
        e.MemRec.GetDiskData(&extr, outbound);
        for (const TDiskPart* p = extr.Begin; p != extr.End; ++p) {
            if (!p->Empty()) {
                parts.push_back(*p);
            }
        }
    }
    std::sort(parts.begin(), parts.end(), [](const TDiskPart& a, const TDiskPart& b) {
        return std::make_tuple(a.ChunkIdx, a.Offset, a.Size) < std::make_tuple(b.ChunkIdx, b.Offset, b.Size);
    });
    parts.erase(std::unique(parts.begin(), parts.end(), [](const TDiskPart& a, const TDiskPart& b) {
        return a.ChunkIdx == b.ChunkIdx && a.Offset == b.Offset && a.Size == b.Size;
    }), parts.end());
    return parts;
}

THullSnapshot ReconstructHull(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const TLogScanResult& log,
    TOwner owner,
    TMaybe<TErasureType::EErasureSpecies> erasure,
    TIssueLog& issues)
{
    THullSnapshot snap;
    snap.Erasure = erasure;

    if (owner >= state.Owners.size()) {
        // The owner table comes from the SysLog; without it there are no starting points to walk.
        issues.Error("hull", TStringBuilder() << "No owner table available (SysLog was not parsed);"
            " cannot reconstruct the Hull of owner# " << (ui32)owner);
        return snap;
    }
    const auto& ownerState = state.Owners[owner];
    auto loadFromEntry = [&](ui8 signature, auto loader) {
        auto it = ownerState.StartingPoints.find(signature);
        if (it == ownerState.StartingPoints.end()) {
            return;
        }
        loader(it->second.second, it->second.first);
    };

    auto loadLogoSsts = [&](const TString& payload, ui64 /*lsn*/) {
        NKikimrVDiskData::THullDbEntryPoint pb;
        if (!ParseHullEntryPoint(payload, pb, issues)) {
            return;
        }
        snap.LogoBlobsCompactedLsn = pb.GetLevelIndex().GetCompactedLsn();
        auto loadLevel = [&](const auto& ssts) {
            for (const auto& p : ssts) {
                TDiskPart part(p);
                TVector<TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>> index;
                TVector<TDiskPart> outbound;
                if (!LoadSstLinear<TKeyLogoBlob, TMemRecLogoBlob>(device, format, state, part, index, outbound,
                    snap.SstParts, issues))
                {
                    continue;
                }
                for (const auto& rec : index) {
                    TBlobIndexEntry e;
                    e.Id = rec.GetKey().LogoBlobID();
                    e.MemRec = rec.GetMemRec();
                    e.Outbound = outbound;
                    snap.Blobs.push_back(std::move(e));
                }
            }
        };
        if (pb.GetLevelIndex().HasLevel0()) {
            loadLevel(pb.GetLevelIndex().GetLevel0().GetSsts());
        }
        for (const auto& level : pb.GetLevelIndex().GetOtherLevels()) {
            loadLevel(level.GetSsts());
        }
    };

    auto loadBlocksSsts = [&](const TString& payload, ui64 /*lsn*/) {
        NKikimrVDiskData::THullDbEntryPoint pb;
        if (!ParseHullEntryPoint(payload, pb, issues)) {
            return;
        }
        snap.BlocksCompactedLsn = pb.GetLevelIndex().GetCompactedLsn();
        auto loadLevel = [&](const auto& ssts) {
            for (const auto& p : ssts) {
                TVector<TIndexRecord<TKeyBlock, TMemRecBlock>> index;
                TVector<TDiskPart> outbound;
                if (!LoadSstLinear<TKeyBlock, TMemRecBlock>(device, format, state, TDiskPart(p), index, outbound,
                    snap.SstParts, issues))
                {
                    continue;
                }
                for (const auto& rec : index) {
                    snap.Blocks.push_back({rec.GetKey(), rec.GetMemRec()});
                }
            }
        };
        if (pb.GetLevelIndex().HasLevel0()) {
            loadLevel(pb.GetLevelIndex().GetLevel0().GetSsts());
        }
        for (const auto& level : pb.GetLevelIndex().GetOtherLevels()) {
            loadLevel(level.GetSsts());
        }
    };

    auto loadBarriersSsts = [&](const TString& payload, ui64 /*lsn*/) {
        NKikimrVDiskData::THullDbEntryPoint pb;
        if (!ParseHullEntryPoint(payload, pb, issues)) {
            return;
        }
        snap.BarriersCompactedLsn = pb.GetLevelIndex().GetCompactedLsn();
        auto loadLevel = [&](const auto& ssts) {
            for (const auto& p : ssts) {
                TVector<TIndexRecord<TKeyBarrier, TMemRecBarrier>> index;
                TVector<TDiskPart> outbound;
                if (!LoadSstLinear<TKeyBarrier, TMemRecBarrier>(device, format, state, TDiskPart(p), index, outbound,
                    snap.SstParts, issues))
                {
                    continue;
                }
                for (const auto& rec : index) {
                    snap.Barriers.push_back({rec.GetKey(), rec.GetMemRec()});
                }
            }
        };
        if (pb.GetLevelIndex().HasLevel0()) {
            loadLevel(pb.GetLevelIndex().GetLevel0().GetSsts());
        }
        for (const auto& level : pb.GetLevelIndex().GetOtherLevels()) {
            loadLevel(level.GetSsts());
        }
    };

    loadFromEntry(TLogSignature::SignatureHullLogoBlobsDB, loadLogoSsts);
    loadFromEntry(TLogSignature::SignatureHullBlocksDB, loadBlocksSsts);
    loadFromEntry(TLogSignature::SignatureHullBarriersDB, loadBarriersSsts);

    std::sort(snap.Blobs.begin(), snap.Blobs.end(),
        [](const TBlobIndexEntry& a, const TBlobIndexEntry& b) { return a.Id < b.Id; });

    TRepeatedIssues skipped;
    TRepeatedIssues unparsed;
    for (const auto& rec : log.Records) {
        if (rec.OwnerId != owner) {
            continue;
        }
        const ui8 sig = rec.Signature.GetUnmasked();
        switch (sig) {
            case TLogSignature::SignatureLogoBlobOpt: {
                if (rec.Lsn <= snap.LogoBlobsCompactedLsn) {
                    break;
                }
                TLogoBlobID id;
                TString data;
                bool keep = false;
                TMaybe<TErasureType::EErasureSpecies> used;
                if (!ParseLogoBlobOpt(rec.Payload, snap.Erasure, id, data, keep, used)) {
                    unparsed.Add("Cannot parse LogoBlobOpt", rec.Lsn);
                    break;
                }
                if (!snap.Erasure && used) {
                    snap.Erasure = used;
                    issues.Info("hull", TStringBuilder() << "Inferred erasure# " << (ui32)*used);
                }
                TMemRecLogoBlob mem;
                mem.SetMemBlob(0, data.size());
                TBlobIndexEntry e;
                e.Id = TLogoBlobID(id, 0);
                e.MemRec = mem;
                e.InlineData = data;
                e.InlinePartId = id.PartId();
                auto it = std::lower_bound(snap.Blobs.begin(), snap.Blobs.end(), e,
                    [](const TBlobIndexEntry& a, const TBlobIndexEntry& b) { return a.Id < b.Id; });
                if (it != snap.Blobs.end() && TKeyLogoBlob(it->Id).IsSameAs(TKeyLogoBlob(e.Id))) {
                    *it = e;
                } else {
                    snap.Blobs.insert(it, std::move(e));
                }
                break;
            }
            case TLogSignature::SignatureHugeLogoBlob: {
                if (rec.Lsn <= snap.LogoBlobsCompactedLsn) {
                    break;
                }
                const char* cur = rec.Payload.data();
                const char* end = cur + rec.Payload.size();
                if (size_t(end - cur) < sizeof(ui16)) {
                    unparsed.Add("Cannot parse HugeLogoBlob", rec.Lsn);
                    break;
                }
                const ui16 lbSize = ReadUnaligned<ui16>(cur);
                cur += sizeof(ui16);
                if (size_t(end - cur) < lbSize) {
                    unparsed.Add("Cannot parse HugeLogoBlob", rec.Lsn);
                    break;
                }
                NKikimrProto::TLogoBlobID protoId;
                if (!protoId.ParseFromArray(cur, lbSize)) {
                    unparsed.Add("Cannot parse HugeLogoBlob id", rec.Lsn);
                    break;
                }
                const TLogoBlobID hugeId = LogoBlobIDFromLogoBlobID(protoId);
                cur += lbSize;
                if (size_t(end - cur) < sizeof(ui64)) {
                    unparsed.Add("Truncated HugeLogoBlob ingress", rec.Lsn);
                    break;
                }
                // The ingress tells which parts are local, which is what maps a range to a part id.
                const TIngress ingress(ReadUnaligned<ui64>(cur));
                cur += sizeof(ui64);
                TDiskPart addr;
                // TDiskPart::Parse only checks the span it is handed, so the payload must be
                // measured here before it is asked to read twelve bytes.
                if (size_t(end - cur) < TDiskPart::SerializedSize) {
                    unparsed.Add("Truncated HugeLogoBlob addr", rec.Lsn);
                    break;
                }
                if (!addr.Parse(cur, cur + TDiskPart::SerializedSize)) {
                    unparsed.Add("Cannot parse HugeLogoBlob addr", rec.Lsn);
                    break;
                }
                TMemRecLogoBlob mem(ingress);
                mem.SetHugeBlob(addr);
                UpsertBlob(snap.Blobs, hugeId, mem);
                break;
            }
            case TLogSignature::SignatureBlock: {
                if (rec.Lsn <= snap.BlocksCompactedLsn) {
                    break;
                }
                NKikimrBlobStorage::TEvVBlock pb;
                if (!pb.ParseFromArray(rec.Payload.data(), rec.Payload.size())) {
                    unparsed.Add("Cannot parse Block", rec.Lsn);
                    break;
                }
                TBlockIndexEntry e{TKeyBlock(pb.GetTabletId()), TMemRecBlock(pb.GetGeneration())};
                auto it = std::lower_bound(snap.Blocks.begin(), snap.Blocks.end(), e,
                    [](const TBlockIndexEntry& a, const TBlockIndexEntry& b) { return a.Key < b.Key; });
                if (it != snap.Blocks.end() && it->Key == e.Key) {
                    it->MemRec.Merge(e.MemRec, e.Key, false, TBlobStorageGroupType());
                } else {
                    snap.Blocks.insert(it, e);
                }
                break;
            }
            case TLogSignature::SignatureGC: {
                if (rec.Lsn <= snap.BarriersCompactedLsn) {
                    break;
                }
                NKikimrBlobStorage::TEvVCollectGarbage pb;
                if (!pb.ParseFromArray(rec.Payload.data(), rec.Payload.size())) {
                    unparsed.Add("Cannot parse GC", rec.Lsn);
                    break;
                }
                TBarrierIndexEntry e;
                e.Key = TKeyBarrier(pb.GetTabletId(), pb.GetChannel(), pb.GetRecordGeneration(),
                    pb.GetPerGenerationCounter(), pb.GetHard());
                e.MemRec = TMemRecBarrier(pb.GetCollectGeneration(), pb.GetCollectStep(), TBarrierIngress());
                snap.Barriers.push_back(e);
                break;
            }
            case TLogSignature::SignatureAddBulkSst:
            case TLogSignature::SignatureLocalSyncData:
            case TLogSignature::SignaturePhantomBlobs: {
                skipped.Add(TStringBuilder() << SignatureName(sig) << " not replayed by this tool", rec.Lsn);
                break;
            }
            default:
                if (sig != TLogSignature::SignatureHullLogoBlobsDB
                    && sig != TLogSignature::SignatureHullBlocksDB
                    && sig != TLogSignature::SignatureHullBarriersDB
                    && sig != TLogSignature::SignatureHullCutLog
                    && sig != TLogSignature::First)
                {
                    skipped.Add(TStringBuilder() << "Unhandled log signature " << SignatureName(sig), rec.Lsn);
                }
                break;
        }
    }
    skipped.Flush(issues, "info");
    unparsed.Flush(issues, "warning");

    std::sort(snap.Blobs.begin(), snap.Blobs.end(),
        [](const TBlobIndexEntry& a, const TBlobIndexEntry& b) { return a.Id < b.Id; });
    std::sort(snap.Blocks.begin(), snap.Blocks.end(),
        [](const TBlockIndexEntry& a, const TBlockIndexEntry& b) { return a.Key < b.Key; });
    std::sort(snap.Barriers.begin(), snap.Barriers.end(),
        [](const TBarrierIndexEntry& a, const TBarrierIndexEntry& b) { return a.Key < b.Key; });
    return snap;
}

} // namespace NKikimr::NPDiskTool

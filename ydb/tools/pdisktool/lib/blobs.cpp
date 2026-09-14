#include "blobs.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>

#include <algorithm>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

namespace NKikimr::NPDiskTool {

namespace {

bool PassRange(const TLogoBlobID& id, const TListFilter& filter) {
    if (filter.From && id < *filter.From) {
        return false;
    }
    if (filter.To && *filter.To < id) {
        return false;
    }
    if (filter.TabletId && id.TabletID() != *filter.TabletId) {
        return false;
    }
    if (filter.Channel && id.Channel() != *filter.Channel) {
        return false;
    }
    return true;
}

TLogoBlobID ParseToken(const TString& token) {
    if (!token) {
        return TLogoBlobID();
    }
    TLogoBlobID id;
    TString error;
    if (!TLogoBlobID::Parse(id, token, error)) {
        return TLogoBlobID();
    }
    return id;
}

// Splits the range of an on-disk blob into its individual parts, mirroring TDiskBlob's layout.
// TDiskBlob itself aborts on any inconsistency, which a recovery tool reading damaged data must not
// do, so the layout is recomputed here and simply reported as unsplittable when it does not add up.
bool LocateParts(
    const TLogoBlobID& id,
    TBlobStorageGroupType gtype,
    NMatrix::TVectorType parts,
    const TDiskPart& where,
    TVector<std::pair<ui32, TDiskPart>>& out)
{
    if (parts.Empty()) {
        return false;
    }
    ui64 blobSize = 0;
    for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
        blobSize += gtype.PartSize(TLogoBlobID(id, i + 1));
    }
    if (blobSize == 0 || blobSize > where.Size) {
        return false;
    }
    const ui32 header = where.Size - blobSize;
    if (header != TDiskBlob::GetBlobHeaderSize(EBlobHeaderMode::NO_HEADER)
        && header != TDiskBlob::GetBlobHeaderSize(EBlobHeaderMode::OLD_HEADER)
        && header != TDiskBlob::GetBlobHeaderSize(EBlobHeaderMode::XXH3_64BIT_HEADER))
    {
        return false;
    }
    ui32 offset = where.Offset + header;
    for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
        const ui32 size = gtype.PartSize(TLogoBlobID(id, i + 1));
        out.emplace_back(i + 1, TDiskPart(where.ChunkIdx, offset, size));
        offset += size;
    }
    return true;
}

// Adds a copy, keeping the list free of exact duplicates: the same SST content reached through
// several levels describes the same bytes and should not look like independent copies.
void AddCopy(TVector<TBlobPartView>& parts, ui32 partId, TPartCopy copy) {
    auto it = std::lower_bound(parts.begin(), parts.end(), partId,
        [](const TBlobPartView& p, ui32 id) { return p.PartId < id; });
    if (it == parts.end() || it->PartId != partId) {
        it = parts.insert(it, TBlobPartView{partId, {}});
    }
    for (const auto& existing : it->Copies) {
        if (existing.Where == copy.Where && existing.InlineData == copy.InlineData) {
            return;
        }
    }
    it->Copies.push_back(std::move(copy));
}

} // namespace

TVector<TBlobView> BuildBlobViews(const THullSnapshot& snap, TIssueLog& issues) {
    const TBlobStorageGroupType gtype(snap.Erasure.GetOrElse(TErasureType::ErasureNone));
    ui32 unsplittable = 0;

    TVector<TBlobView> views;
    for (const auto& e : snap.Blobs) {
        if (views.empty() || !TKeyLogoBlob(views.back().Id).IsSameAs(TKeyLogoBlob(e.Id))) {
            // IsSameAs ignores the part id, so keep the full id out of the view: whichever record
            // happened to group first must not lend its part id to the listing or a file name.
            views.push_back(TBlobView{e.Id.FullID(), 0, {}});
        }
        auto& view = views.back();
        view.Ingress |= e.MemRec.GetIngress().Raw();

        if (e.InlineData) {
            TPartCopy copy;
            copy.Type = TBlobType::MemBlob;
            copy.InlineData = e.InlineData;
            AddCopy(view.Parts, e.InlinePartId ? e.InlinePartId : 1, std::move(copy));
        }
        if (e.MemRec.GetType() == TBlobType::MemBlob || !e.MemRec.HasData()) {
            continue;
        }

        TDiskDataExtractor extr;
        const TDiskPart* outbound = e.Outbound.empty() ? nullptr : e.Outbound.data();
        if (e.MemRec.GetType() == TBlobType::ManyHugeBlobs && e.Outbound.empty()) {
            ++unsplittable;
            continue; // GetDiskData would dereference a missing outbound array
        }
        e.MemRec.GetDiskData(&extr, outbound);
        const NMatrix::TVectorType local = e.MemRec.GetLocalParts(gtype);

        // A DiskBlob packs all local parts into one range; the huge flavours use one range per part.
        if (e.MemRec.GetType() == TBlobType::DiskBlob) {
            for (const TDiskPart* p = extr.Begin; p != extr.End; ++p) {
                if (p->Empty()) {
                    continue;
                }
                TVector<std::pair<ui32, TDiskPart>> located;
                if (LocateParts(e.Id, gtype, local, *p, located)) {
                    for (const auto& [partId, range] : located) {
                        AddCopy(view.Parts, partId, TPartCopy{TBlobType::DiskBlob, range, {}, false});
                    }
                } else {
                    ++unsplittable;
                    const ui32 partId = local.Empty() ? 0 : ui32(local.FirstPosition()) + 1;
                    AddCopy(view.Parts, partId, TPartCopy{TBlobType::DiskBlob, *p, {}, true});
                }
            }
            continue;
        }

        ui8 bit = local.Empty() ? 0 : local.FirstPosition();
        for (const TDiskPart* p = extr.Begin; p != extr.End; ++p) {
            if (p->Empty()) {
                continue;
            }
            const ui32 partId = local.Empty() ? 1 : ui32(bit) + 1;
            NMatrix::TVectorType single(0, Max<ui8>(local.GetSize(), 1));
            if (!local.Empty()) {
                single.Set(bit);
                bit = local.NextPosition(bit);
            }
            TVector<std::pair<ui32, TDiskPart>> located;
            if (!local.Empty() && LocateParts(e.Id, gtype, single, *p, located)) {
                AddCopy(view.Parts, partId, TPartCopy{e.MemRec.GetType(), located.front().second, {}, false});
            } else {
                // Without a known erasure the header size cannot be derived; keep the whole range.
                AddCopy(view.Parts, partId, TPartCopy{e.MemRec.GetType(), *p, {}, true});
            }
        }
    }

    if (unsplittable) {
        issues.Warning("hull", TStringBuilder() << unsplittable
            << " blob record(s) could not be resolved to individual parts"
            << (snap.Erasure ? "" : "; pass --erasure so part sizes can be derived"), true);
    }
    return views;
}

void ListBlobs(
    const THullSnapshot& snap,
    const TListFilter& filter,
    TIssueLog& issues,
    NKikimr::NPdiskTool::TBlobsResult& out)
{
    TLogoBlobID from = filter.From.GetOrElse(TLogoBlobID());
    if (filter.ContinueToken) {
        from = ParseToken(filter.ContinueToken);
    }
    ui32 n = 0;
    ui32 skippedWithoutData = 0;
    TLogoBlobID last;
    for (const auto& view : BuildBlobViews(snap, issues)) {
        if (view.Id < from) {
            continue;
        }
        if (filter.ContinueToken && view.Id == from) {
            continue;
        }
        if (filter.To && *filter.To < view.Id) {
            break; // the views are sorted, so nothing past the upper bound can match
        }
        if (!PassRange(view.Id, filter)) {
            continue;
        }
        if (!view.HasData()) {
            ++skippedWithoutData;
            if (filter.DataOnly) {
                continue;
            }
        }
        auto* b = out.AddBlobs();
        b->SetLogoBlobId(view.Id.ToString());
        b->SetIngress(view.Ingress);
        for (const auto& part : view.Parts) {
            for (const auto& copy : part.Copies) {
                auto* p = b->AddParts();
                p->SetPartId(part.PartId);
                p->SetSize(copy.Size());
                p->SetChunkIdx(copy.Where.ChunkIdx);
                p->SetOffset(copy.Where.Offset);
                p->SetBlobType(TBlobType::TypeToStr(copy.Type));
                p->SetCopies(part.Copies.size());
                p->SetPacked(copy.Packed);
            }
        }
        last = view.Id;
        ++n;
        if (n >= filter.Limit) {
            out.SetContinueToken(last.ToString());
            break;
        }
    }
    out.SetTotalListed(n);
    out.SetSkippedWithoutData(skippedWithoutData);
}

void ListBarriers(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBarriersResult& out)
{
    // The token counts matching rows, not raw index positions, so paging does not depend on the
    // filter staying the same between calls.
    ui32 skip = 0;
    if (filter.ContinueToken) {
        skip = FromString<ui32>(filter.ContinueToken);
    }
    ui32 matched = 0;
    ui32 n = 0;
    for (ui32 i = 0; i < snap.Barriers.size(); ++i) {
        const auto& e = snap.Barriers[i];
        if (filter.TabletId && e.Key.TabletId != *filter.TabletId) {
            continue;
        }
        if (filter.Channel && e.Key.Channel != *filter.Channel) {
            continue;
        }
        if (matched++ < skip) {
            continue;
        }
        auto* b = out.AddBarriers();
        b->SetTabletId(e.Key.TabletId);
        b->SetChannel(e.Key.Channel);
        b->SetHard(e.Key.Hard);
        b->SetGen(e.Key.Gen);
        b->SetGenCounter(e.Key.GenCounter);
        b->SetCollectGen(e.MemRec.CollectGen);
        b->SetCollectStep(e.MemRec.CollectStep);
        ++n;
        if (n >= filter.Limit) {
            out.SetContinueToken(ToString(matched));
            break;
        }
    }
}

void ListBlocks(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBlocksResult& out)
{
    ui32 skip = 0;
    if (filter.ContinueToken) {
        skip = FromString<ui32>(filter.ContinueToken);
    }
    ui32 matched = 0;
    ui32 n = 0;
    for (ui32 i = 0; i < snap.Blocks.size(); ++i) {
        const auto& e = snap.Blocks[i];
        if (filter.TabletId && e.Key.TabletId != *filter.TabletId) {
            continue;
        }
        if (matched++ < skip) {
            continue;
        }
        auto* b = out.AddBlocks();
        b->SetTabletId(e.Key.TabletId);
        b->SetBlockedGeneration(e.MemRec.BlockedGeneration);
        ++n;
        if (n >= filter.Limit) {
            out.SetContinueToken(ToString(matched));
            break;
        }
    }
}

bool ExportBlobParts(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const THullSnapshot& snap,
    const TLogoBlobID& from,
    const TMaybe<TLogoBlobID>& to,
    const TListFilter& filter,
    const TString& outputDir,
    TIssueLog& issues,
    TExportStats& stats)
{
    stats = {};
    TFsPath dir(outputDir);
    dir.MkDirs();

    auto write = [&](const TString& name, const TString& data) {
        TFileOutput out(dir / name);
        out.Write(data.data(), data.size());
        out.Flush();
    };

    for (const auto& view : BuildBlobViews(snap, issues)) {
        if (view.Id < from) {
            continue;
        }
        if (to && *to < view.Id) {
            break;
        }
        if (!PassRange(view.Id, filter)) {
            continue;
        }
        if (!view.HasData()) {
            continue;
        }
        bool wroteAnything = false;
        for (const auto& part : view.Parts) {
            TVector<TString> copies;
            for (const auto& copy : part.Copies) {
                copies.push_back(copy.InlineData
                    ? copy.InlineData
                    : ReadLogicalRange(device, format, state, copy.Where.ChunkIdx, copy.Where.Offset,
                        copy.Where.Size, issues, "blob"));
            }
            if (copies.empty()) {
                continue;
            }
            const TString base = TStringBuilder() << view.Id.ToString() << ".part" << part.PartId;
            const bool allEqual = std::equal(copies.begin() + 1, copies.end(), copies.begin());
            if (copies.size() > 1) {
                ++stats.PartsWithSeveralCopies;
            }
            if (allEqual) {
                write(base, copies.front());
            } else {
                // Diverging copies are a real finding, so keep every one of them for inspection
                // rather than silently picking a winner.
                ++stats.PartsWithDifferingCopies;
                TStringBuilder where;
                for (size_t i = 0; i < part.Copies.size(); ++i) {
                    write(TStringBuilder() << base << ".copy" << (i + 1), copies[i]);
                    where << (i ? ", " : "") << part.Copies[i].Where.ToString()
                        << " size# " << copies[i].size();
                }
                issues.Error("export-blob", TStringBuilder() << view.Id.ToString()
                    << " part " << part.PartId << " has " << copies.size()
                    << " copies that differ: " << where
                    << "; written as " << base << ".copyN");
            }
            ++stats.Parts;
            wroteAnything = true;
        }
        if (wroteAnything) {
            ++stats.Blobs;
        }
    }
    return true;
}

} // namespace NKikimr::NPDiskTool

#include "blobs.h"

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

void FillParts(const TBlobIndexEntry& e, NKikimr::NPdiskTool::TBlobInfo& proto) {
    proto.SetIngress(e.MemRec.GetIngress().Raw());
    if (e.MemRec.GetType() != TBlobType::MemBlob) {
        TDiskDataExtractor extr;
        const TDiskPart* outbound = e.Outbound.empty() ? nullptr : e.Outbound.data();
        e.MemRec.GetDiskData(&extr, outbound);
        ui32 i = 0;
        for (const TDiskPart* p = extr.Begin; p != extr.End; ++p, ++i) {
            auto* part = proto.AddParts();
            part->SetPartId(i + 1);
            part->SetSize(p->Size);
            part->SetChunkIdx(p->ChunkIdx);
            part->SetOffset(p->Offset);
            part->SetBlobType(TBlobType::TypeToStr(e.MemRec.GetType()));
        }
    }
    if (e.InlineData) {
        auto* part = proto.AddParts();
        part->SetPartId(e.Id.PartId() ? e.Id.PartId() : 1);
        part->SetSize(e.InlineData.size());
        part->SetBlobType("MemBlob");
    }
}

} // namespace

void ListBlobs(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBlobsResult& out)
{
    TLogoBlobID from = filter.From.GetOrElse(TLogoBlobID());
    if (filter.ContinueToken) {
        from = ParseToken(filter.ContinueToken);
    }
    ui32 n = 0;
    TLogoBlobID last;
    for (const auto& e : snap.Blobs) {
        if (e.Id < from) {
            continue;
        }
        if (filter.ContinueToken && e.Id == from) {
            continue;
        }
        if (!PassRange(e.Id, filter)) {
            continue;
        }
        auto* b = out.AddBlobs();
        b->SetLogoBlobId(e.Id.ToString());
        b->SetTabletId(e.Id.TabletID());
        b->SetChannel(e.Id.Channel());
        b->SetGeneration(e.Id.Generation());
        b->SetStep(e.Id.Step());
        b->SetCookie(e.Id.Cookie());
        b->SetBlobSize(e.Id.BlobSize());
        FillParts(e, *b);
        last = e.Id;
        ++n;
        if (n >= filter.Limit) {
            out.SetContinueToken(last.ToString());
            break;
        }
    }
    out.SetTotalListed(n);
}

void ListBarriers(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBarriersResult& out)
{
    ui32 skip = 0;
    if (filter.ContinueToken) {
        skip = FromString<ui32>(filter.ContinueToken);
    }
    ui32 n = 0;
    for (ui32 i = skip; i < snap.Barriers.size(); ++i) {
        const auto& e = snap.Barriers[i];
        if (filter.TabletId && e.Key.TabletId != *filter.TabletId) {
            continue;
        }
        if (filter.Channel && e.Key.Channel != *filter.Channel) {
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
            out.SetContinueToken(ToString(i + 1));
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
    ui32 n = 0;
    for (ui32 i = skip; i < snap.Blocks.size(); ++i) {
        const auto& e = snap.Blocks[i];
        if (filter.TabletId && e.Key.TabletId != *filter.TabletId) {
            continue;
        }
        auto* b = out.AddBlocks();
        b->SetTabletId(e.Key.TabletId);
        b->SetBlockedGeneration(e.MemRec.BlockedGeneration);
        ++n;
        if (n >= filter.Limit) {
            out.SetContinueToken(ToString(i + 1));
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
    const TString& outputDir,
    TIssueLog& issues,
    ui32& exported)
{
    exported = 0;
    TFsPath dir(outputDir);
    dir.MkDirs();
    for (const auto& e : snap.Blobs) {
        if (e.Id < from) {
            continue;
        }
        if (to && *to < e.Id) {
            break;
        }
        if (e.InlineData) {
            TString name = TStringBuilder() << e.Id.ToString() << ".part";
            TFileOutput out(dir / name);
            out.Write(e.InlineData.data(), e.InlineData.size());
            ++exported;
            continue;
        }
        if (e.MemRec.GetType() == TBlobType::MemBlob) {
            continue;
        }
        TDiskDataExtractor extr;
        const TDiskPart* outbound = e.Outbound.empty() ? nullptr : e.Outbound.data();
        e.MemRec.GetDiskData(&extr, outbound);
        ui32 partNo = 0;
        for (const TDiskPart* p = extr.Begin; p != extr.End; ++p, ++partNo) {
            if (p->Empty()) {
                continue;
            }
            TString data = ReadLogicalRange(device, format, state, p->ChunkIdx, p->Offset, p->Size, issues);
            TString name = TStringBuilder() << e.Id.ToString() << ".part" << (partNo + 1);
            TFileOutput out(dir / name);
            out.Write(data.data(), data.size());
        }
        ++exported;
    }
    return true;
}

} // namespace NKikimr::NPDiskTool

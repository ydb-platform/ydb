#include "session.h"
#include "sector.h"

#include <cstring>
#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NKikimr::NPDiskTool {

bool TPDiskSession::Load(const TSessionOptions& opts) {
    Opts = opts;
    Issues.Strict = opts.Strict;
    FormatResult = ReadDiskFormat(*Device, opts.MainKey, Issues, opts.ShowKeys);
    if (!FormatResult.Ok) {
        return false;
    }
    Format = FormatResult.Format;
    Loaded = true;
    SysLogRaw = ReadSysLog(*Device, Format, Issues);
    if (!SysLogRaw.Ok) {
        return true; // format is enough for some commands
    }
    State = ParseSysLogPayload(SysLogRaw.Payload, Format, Issues);
    Log = ScanMainLog(*Device, Format, State, Issues);
    return true;
}

bool TPDiskSession::OpenFile(const TString& path, const TSessionOptions& opts, bool requireFormat) {
    try {
        Device = OpenFileDevice(path, opts.TryLock, Issues);
    } catch (const yexception& e) {
        Issues.Error(path, TStringBuilder() << "Cannot open device: " << e.what());
        return false;
    }
    if (!Load(opts) && requireFormat) {
        return false;
    }
    return true;
}

bool TPDiskSession::OpenSectorMap(TIntrusivePtr<NPDisk::TSectorMap> map, const TSessionOptions& opts) {
    Device = OpenSectorMapDevice(std::move(map));
    return Load(opts);
}

TOwner TPDiskSession::ResolveOwner(const TString& vdisk, TMaybe<ui32> ownerId, TIssueLog& issues) const {
    if (ownerId) {
        if (*ownerId >= 256) {
            issues.Error("owner", TStringBuilder() << "Owner id out of range: " << *ownerId);
            return 0;
        }
        return static_cast<TOwner>(*ownerId);
    }
    if (!vdisk) {
        issues.Error("owner", "Specify --vdisk or --owner");
        return 0;
    }
    TString want = vdisk;
    if (!want.StartsWith("[")) {
        want = TStringBuilder() << "[" << want << "]";
    }
    for (ui32 i = 0; i < State.Owners.size(); ++i) {
        const auto& id = State.Owners[i].VDiskId;
        if (id == TVDiskID::InvalidId) {
            continue;
        }
        if (id.ToString() == want || id.ToStringWOGeneration() == want) {
            return static_cast<TOwner>(i);
        }
    }
    issues.Error("owner", TStringBuilder() << "No owner matches VDisk " << vdisk);
    return 0;
}

static bool TryMetadataFormatSector(
    IDeviceReader& device,
    const TMainKey& mainKey,
    bool encryption,
    TIssueLog& issues,
    NPDisk::TMetadataFormatSector& out)
{
    const ui32 total = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
    TVector<ui8> raw(total);
    device.Pread(raw.data(), total, 0, issues);

    constexpr ui32 usefulDataSize = NPDisk::FormatSectorSize - sizeof(TDataSectorFooter);
    TVector<ui8> decrypted(usefulDataSize);
    auto& decryptedSector = *reinterpret_cast<NPDisk::TMetadataFormatSector*>(decrypted.data());
    bool found = false;

    TPDiskStreamCypher cypher(encryption);
    const ui8* data = raw.data();
    for (ui32 i = 0; i < NPDisk::ReplicationFactor; ++i, data += NPDisk::FormatSectorSize) {
        const auto& footer = *reinterpret_cast<const TDataSectorFooter*>(data + usefulDataSize);
        for (const auto& key : mainKey.Keys) {
            cypher.SetKey(key);
            cypher.StartMessage(footer.Nonce);
            cypher.Encrypt(decrypted.data(), data, decrypted.size());
            TPDiskHashCalculator hasher;
            hasher.Hash(decrypted.data(), decrypted.size());
            if (hasher.GetHashResult() == footer.Hash && decryptedSector.Magic == NPDisk::MagicMetadataFormatSector) {
                if (!found || out.SequenceNumber < decryptedSector.SequenceNumber) {
                    out = decryptedSector;
                    found = true;
                }
            }
        }
    }
    return found;
}

// A metadata record holds a node's config blob; anything larger than this is a damaged length field
// rather than a record, and must not become an allocation or a read size.
static constexpr ui64 MaxMetadataLength = 32ull << 20;

static bool TryReadMetadataPayload(
    IDeviceReader& device,
    ui64 offset,
    ui64 length,
    const TKey& dataKey,
    bool encryption,
    TIssueLog& issues,
    NKikimr::NPdiskTool::TMetadataResult& proto)
{
    if (length < sizeof(NPDisk::TMetadataHeader) || length > MaxMetadataLength) {
        issues.Warning("metadata", TStringBuilder() << "Implausible metadata length# " << length, true);
        return false;
    }
    TVector<ui8> buf(length);
    device.Pread(buf.data(), static_cast<ui32>(length), offset, issues);
    auto* header = reinterpret_cast<NPDisk::TMetadataHeader*>(buf.data());
    TPDiskStreamCypher cypher(encryption);
    cypher.SetKey(dataKey);
    header->Encrypt(cypher);
    ui64 magic = dataKey;
    if (!header->CheckHash(&magic)) {
        return false;
    }
    if (sizeof(NPDisk::TMetadataHeader) + header->Length > length) {
        issues.Warning("metadata", "Metadata header length exceeds stored record", true);
        return false;
    }
    header->EncryptData(cypher);
    if (!header->CheckDataHash()) {
        issues.Warning("metadata", "Metadata payload hash mismatch", true);
        return false;
    }
    proto.SetPresent(true);
    proto.SetSequenceNumber(header->SequenceNumber);
    proto.SetLength(header->Length);
    proto.SetData(TString(reinterpret_cast<const char*>(header + 1), header->Length));
    return true;
}

static bool TryReadFormattedSlot(
    IDeviceReader& device,
    const TDiskFormat& format,
    TChunkIdx chunkIdx,
    ui32 offsetInSectors,
    TIssueLog& issues,
    NKikimr::NPdiskTool::TMetadataResult& proto)
{
    const ui64 offset = format.Offset(chunkIdx, offsetInSectors);
    const ui64 headerBytes = format.RoundUpToSectorSize(sizeof(NPDisk::TMetadataHeader));
    ui64 magic = format.ChunkKey;

    auto tryEnc = [&](bool encryption) -> bool {
        TVector<ui8> buf(headerBytes);
        device.Pread(buf.data(), static_cast<ui32>(headerBytes), offset, issues);
        auto* header = reinterpret_cast<NPDisk::TMetadataHeader*>(buf.data());
        TPDiskStreamCypher cypher(encryption);
        cypher.SetKey(format.ChunkKey);
        header->Encrypt(cypher);
        if (!header->CheckHash(&magic)) {
            return false;
        }
        if (header->Length > MaxMetadataLength) {
            issues.Warning("metadata", TStringBuilder() << "Implausible metadata slot length# "
                << header->Length << " chunk# " << chunkIdx, true);
            return false;
        }
        const ui64 total = sizeof(NPDisk::TMetadataHeader) + header->Length;
        const ui64 need = format.RoundUpToSectorSize(total);
        if (need > headerBytes) {
            buf.resize(need);
            device.Pread(buf.data(), static_cast<ui32>(need), offset, issues);
            header = reinterpret_cast<NPDisk::TMetadataHeader*>(buf.data());
            header->Encrypt(cypher);
            header->EncryptData(cypher);
        } else {
            header->EncryptData(cypher);
        }
        if (!header->CheckDataHash()) {
            issues.Warning("metadata", TStringBuilder() << "Metadata slot data hash mismatch chunk# " << chunkIdx, true);
            return false;
        }
        proto.SetPresent(true);
        proto.SetSequenceNumber(header->SequenceNumber);
        proto.SetLength(header->Length);
        proto.SetData(TString(reinterpret_cast<const char*>(header + 1), header->Length));
        return true;
    };

    return tryEnc(true) || tryEnc(false);
}

void ReadMetadata(
    IDeviceReader& device,
    const TMainKey& mainKey,
    const TFormatReadResult& format,
    const TParsedSysLog* state,
    TIssueLog& issues,
    NKikimr::NPdiskTool::TMetadataResult& proto)
{
    proto.SetPresent(false);
    NPDisk::TMetadataFormatSector sector = {};
    if (TryMetadataFormatSector(device, mainKey, true, issues, sector)
        || TryMetadataFormatSector(device, mainKey, false, issues, sector))
    {
        if (TryReadMetadataPayload(device, sector.Offset, sector.Length, sector.DataKey, true, issues, proto)
            || TryReadMetadataPayload(device, sector.Offset, sector.Length, sector.DataKey, false, issues, proto))
        {
            return;
        }
    }
    if (!format.Ok || !state) {
        issues.Info("metadata", "No metadata vault record found");
        return;
    }
    const ui32 half = format.Format.ChunkSize / (2 * format.Format.SectorSize);
    ui64 bestSeq = 0;
    NKikimr::NPdiskTool::TMetadataResult best;
    for (ui32 i = 0; i < state->Chunks.size(); ++i) {
        if (state->Chunks[i].OwnerId != EOwner::OwnerMetadata) {
            continue;
        }
        for (ui32 slot : {0u, half}) {
            NKikimr::NPdiskTool::TMetadataResult cand;
            if (TryReadFormattedSlot(device, format.Format, i, slot, issues, cand)) {
                if (cand.GetSequenceNumber() >= bestSeq) {
                    bestSeq = cand.GetSequenceNumber();
                    best = cand;
                }
            }
        }
    }
    if (best.GetPresent()) {
        proto.Swap(&best);
        return;
    }
    issues.Info("metadata", "No metadata vault record found");
}

} // namespace NKikimr::NPDiskTool

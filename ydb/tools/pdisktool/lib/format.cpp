#include "format.h"

#include <cstring>
#include <util/datetime/base.h>
#include <util/string/hex.h>

namespace NKikimr::NPDiskTool {

namespace {

bool IsAllZero(const ui8* data, ui32 size) {
    for (ui32 i = 0; i < size; ++i) {
        if (data[i] != 0) {
            return false;
        }
    }
    return true;
}

TString DescribeRawPrefix(const ui8* raw, ui32 total) {
    TStringStream s;
    s << "deviceSizePrefixHex# " << HexEncode(raw, Min<ui32>(16, total));
    if (total >= NPDisk::FormatSectorSize * NPDisk::ReplicationFactor && IsAllZero(raw, total)) {
        s << " (format area is all zeros; unformatted device or the read returned no data)";
    }
    if (total > 512 + 8 && memcmp(raw + 512, "EFI PART", 8) == 0) {
        s << " (looks like a GPT header at offset 512; this is probably the whole disk, not the PDisk partition)";
    }
    return s.Str();
}

} // namespace

TFormatReadResult ReadDiskFormat(
    IDeviceReader& device,
    const TMainKey& mainKey,
    TIssueLog& issues,
    bool /*showKeys*/)
{
    TFormatReadResult result;
    const ui32 total = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
    TVector<ui8> buffer(total);
    ui8* raw = buffer.data();
    device.Pread(raw, total, 0, issues);

    // A hash-valid record can still be one this build cannot represent; TDiskFormat::UpgradeFrom
    // answers that with Y_VERIFY, which would abort the tool instead of reporting the version.
    auto isRepresentable = [&](const TDiskFormat& candidate, TString& error) {
        if (candidate.Version > PDISK_FORMAT_VERSION) {
            error = TStringBuilder() << "format version# " << candidate.Version
                << " is newer than this build supports (" << PDISK_FORMAT_VERSION << ")";
            return false;
        }
        if (candidate.GetUsedSize() > sizeof(TDiskFormat)) {
            error = TStringBuilder() << "diskFormatSize# " << candidate.DiskFormatSize
                << " exceeds this build's TDiskFormat (" << sizeof(TDiskFormat) << ")";
            return false;
        }
        return true;
    };

    TString unrepresentable;
    auto tryKey = [&](TKey key, bool encrypt) -> bool {
        TVector<TFormatReplicaInfo> replicas(NPDisk::ReplicationFactor);
        TDiskFormat winner;
        bool haveWinner = false;

        TPDiskStreamCypher cypher(encrypt);
        cypher.SetKey(key);

        for (ui32 i = 0; i < NPDisk::ReplicationFactor; ++i) {
            ui8* sector = raw + i * NPDisk::FormatSectorSize;
            auto* footer = reinterpret_cast<TDataSectorFooter*>(
                sector + NPDisk::FormatSectorSize - sizeof(TDataSectorFooter));
            replicas[i].Index = i;
            replicas[i].Nonce = footer->Nonce;

            alignas(16) NPDisk::TDiskFormatSector formatCandidate;
            cypher.StartMessage(footer->Nonce);
            cypher.Encrypt(formatCandidate.Raw, sector, NPDisk::FormatSectorSize);

            replicas[i].Decrypted = true;
            if (formatCandidate.Format.IsHashOk(NPDisk::FormatSectorSize)) {
                replicas[i].HashOk = true;
                replicas[i].Format = formatCandidate.Format;
                TString error;
                if (!isRepresentable(formatCandidate.Format, error)) {
                    replicas[i].Error = error;
                    unrepresentable = error;
                    continue;
                }
                winner = formatCandidate.Format;
                haveWinner = true;
            } else {
                replicas[i].Error = TStringBuilder() << "hash mismatch nonce# " << footer->Nonce
                    << " version# " << formatCandidate.Format.Version
                    << " diskFormatSize# " << formatCandidate.Format.DiskFormatSize
                    << " headHex# " << HexEncode(sector, 16);
            }
        }

        if (haveWinner) {
            result.Ok = true;
            result.Format.UpgradeFrom(winner);
            result.Replicas = std::move(replicas);
            result.UsedEncryption = encrypt;
            return true;
        }
        result.Replicas = std::move(replicas);
        return false;
    };

    for (ui32 k = 0; k < mainKey.Keys.size(); ++k) {
        if (tryKey(mainKey.Keys[k], true)) {
            result.WinningKeyIndex = k;
            if (k + 1 < mainKey.Keys.size()) {
                issues.Warning("format", "An older main key decrypted the format record");
            }
            ui32 okCount = 0;
            for (const auto& r : result.Replicas) {
                if (r.HashOk) {
                    ++okCount;
                } else {
                    issues.Warning("format", TStringBuilder() << "Format replica " << r.Index << " is invalid");
                }
            }
            if (okCount > 1) {
                for (const auto& r : result.Replicas) {
                    if (r.HashOk && r.Format.Guid != result.Format.Guid) {
                        issues.Warning("format", TStringBuilder() << "Format replica " << r.Index
                            << " Guid disagrees", true);
                    }
                }
            }
            return result;
        }
        if (tryKey(mainKey.Keys[k], false)) {
            result.WinningKeyIndex = k;
            issues.Info("format", "Format record is stored unencrypted");
            return result;
        }
    }

    if (unrepresentable) {
        issues.Error("format", TStringBuilder()
            << "The format record decrypted and its hash is valid, but " << unrepresentable
            << "; use a pdisktool built from the same revision as the ydbd that formatted this disk");
        result.Ok = false;
        return result;
    }

    issues.Error("format", TStringBuilder()
        << "Could not decrypt/validate any format replica with " << mainKey.Keys.size()
        << " main key(s). " << DescribeRawPrefix(raw, total)
        << ". --key-file must be ydbd's TKeyConfig proto (--pdisk-key-file), or the binary container "
        << "named in ContainerPath. --main-key accepts decimal, 0x-hex, or YdbDefaultPDiskSequence.");
    result.Ok = false;
    return result;
}

void FillFormatProto(const TFormatReadResult& result, NKikimr::NPdiskTool::TFormatResult& proto, bool showKeys) {
    if (result.Ok) {
        auto* f = proto.MutableFormat();
        const auto& fmt = result.Format;
        f->SetVersion(fmt.Version);
        f->SetDiskSize(fmt.DiskSize);
        f->SetGuid(fmt.Guid);
        f->SetChunkSize(fmt.ChunkSize);
        f->SetSectorSize(fmt.SectorSize);
        f->SetSysLogSectorCount(fmt.SysLogSectorCount);
        f->SetSystemChunkCount(fmt.SystemChunkCount);
        TString text(fmt.FormatText, strnlen(fmt.FormatText, sizeof(fmt.FormatText)));
        f->SetFormatText(text);
        f->SetDiskFormatSize(fmt.DiskFormatSize);
        f->SetTimestampUs(fmt.TimestampUs);
        f->SetTimestamp(TInstant::MicroSeconds(fmt.TimestampUs).ToString());
        f->SetFormatFlagsRaw(fmt.FormatFlags);
        f->SetFormatFlags(fmt.FormatFlagsToString(fmt.FormatFlags));
        f->SetMagicNextLogChunkReference(fmt.MagicNextLogChunkReference);
        f->SetMagicLogChunk(fmt.MagicLogChunk);
        f->SetMagicDataChunk(fmt.MagicDataChunk);
        f->SetMagicSysLogChunk(fmt.MagicSysLogChunk);
        f->SetMagicFormatChunk(fmt.MagicFormatChunk);
        // TDiskFormat divides ChunkSize / SectorSize; both are 0 when decrypt failed.
        if (fmt.SectorSize != 0 && fmt.ChunkSize >= fmt.SectorSize) {
            f->SetUserAccessibleChunkSize(fmt.GetUserAccessibleChunkSize());
        }
        if (showKeys) {
            f->SetSysLogKey(fmt.SysLogKey);
            f->SetLogKey(fmt.LogKey);
            f->SetChunkKey(fmt.ChunkKey);
        }
    }
    for (const auto& r : result.Replicas) {
        auto* p = proto.AddReplicas();
        p->SetIndex(r.Index);
        p->SetNonce(r.Nonce);
        p->SetHashOk(r.HashOk);
        p->SetDecrypted(r.Decrypted);
        if (r.Error) {
            p->SetError(r.Error);
        }
    }
}

void PrintFormatText(const NKikimr::NPdiskTool::TFormatResult& proto, IOutputStream& out) {
    if (!proto.HasFormat()) {
        // Every field would read as zero, which looks like a format full of zeros rather than a
        // record that never decrypted.
        out << "PDisk format: not decrypted, see the replicas below" << Endl;
    } else {
        const auto& f = proto.GetFormat();
        out << "PDisk format" << Endl;
        out << "  Version: " << f.GetVersion() << Endl;
        out << "  DiskSize: " << f.GetDiskSize() << " bytes" << Endl;
        out << "  Guid: " << f.GetGuid() << Endl;
        out << "  ChunkSize: " << f.GetChunkSize() << Endl;
        out << "  SectorSize: " << f.GetSectorSize() << Endl;
        out << "  SysLogSectorCount: " << f.GetSysLogSectorCount() << Endl;
        out << "  SystemChunkCount: " << f.GetSystemChunkCount() << Endl;
        out << "  UserAccessibleChunkSize: " << f.GetUserAccessibleChunkSize() << Endl;
        out << "  FormatText: \"" << f.GetFormatText() << "\"" << Endl;
        out << "  DiskFormatSize: " << f.GetDiskFormatSize() << Endl;
        out << "  Timestamp: " << f.GetTimestamp() << Endl;
        out << "  FormatFlags: " << f.GetFormatFlags() << Endl;
        out << "  Magics: next=" << f.GetMagicNextLogChunkReference()
            << " log=" << f.GetMagicLogChunk()
            << " data=" << f.GetMagicDataChunk()
            << " syslog=" << f.GetMagicSysLogChunk()
            << " format=" << f.GetMagicFormatChunk() << Endl;
        if (f.HasSysLogKey()) {
            out << "  SysLogKey: " << f.GetSysLogKey() << Endl;
            out << "  LogKey: " << f.GetLogKey() << Endl;
            out << "  ChunkKey: " << f.GetChunkKey() << Endl;
        }
    }
    out << "Replicas:" << Endl;
    for (const auto& r : proto.GetReplicas()) {
        out << "  [" << r.GetIndex() << "] nonce=" << r.GetNonce()
            << " hashOk=" << r.GetHashOk()
            << " decrypted=" << r.GetDecrypted();
        if (r.HasError()) {
            out << " error=" << r.GetError();
        }
        out << Endl;
    }
}

} // namespace NKikimr::NPDiskTool

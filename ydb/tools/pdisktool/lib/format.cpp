#include "format.h"

#include <cstring>
#include <util/datetime/base.h>

namespace NKikimr::NPDiskTool {

TFormatReadResult ReadDiskFormat(
    IDeviceReader& device,
    const TMainKey& mainKey,
    TIssueLog& issues,
    bool /*showKeys*/)
{
    TFormatReadResult result;
    const ui32 total = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
    TVector<ui8> raw(total);
    device.Pread(raw.data(), total, 0, issues);

    auto tryKey = [&](TKey key, bool encrypt) -> bool {
        TVector<TFormatReplicaInfo> replicas(NPDisk::ReplicationFactor);
        TDiskFormat winner;
        bool haveWinner = false;
        ui32 lastGood = Max<ui32>();

        TPDiskStreamCypher cypher(encrypt);
        cypher.SetKey(key);

        for (ui32 i = 0; i < NPDisk::ReplicationFactor; ++i) {
            ui8* sector = raw.data() + i * NPDisk::FormatSectorSize;
            auto* footer = reinterpret_cast<TDataSectorFooter*>(
                sector + NPDisk::FormatSectorSize - sizeof(TDataSectorFooter));
            replicas[i].Index = i;
            replicas[i].Nonce = footer->Nonce;

            alignas(16) TDiskFormat candidate = {};
            cypher.StartMessage(footer->Nonce);
            cypher.Encrypt(&candidate, sector, sizeof(TDiskFormat));

            replicas[i].Decrypted = true;
            if (candidate.IsHashOk(NPDisk::FormatSectorSize)) {
                replicas[i].HashOk = true;
                replicas[i].Format = candidate;
                winner = candidate;
                haveWinner = true;
                lastGood = i;
            } else {
                replicas[i].Error = "hash mismatch";
            }
        }

        if (haveWinner) {
            result.Ok = true;
            result.Format.UpgradeFrom(winner);
            result.Replicas = std::move(replicas);
            result.UsedEncryption = encrypt;
            Y_UNUSED(lastGood);
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
                // Check disagreement among valid replicas
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

    issues.Error("format", "Could not decrypt/validate any format replica; check --main-key / --key-file");
    result.Ok = false;
    return result;
}

void FillFormatProto(const TFormatReadResult& result, NKikimr::NPdiskTool::TFormatResult& proto, bool showKeys) {
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
    f->SetUserAccessibleChunkSize(fmt.GetUserAccessibleChunkSize());
    if (showKeys) {
        f->SetSysLogKey(fmt.SysLogKey);
        f->SetLogKey(fmt.LogKey);
        f->SetChunkKey(fmt.ChunkKey);
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

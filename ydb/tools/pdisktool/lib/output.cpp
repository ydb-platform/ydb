#include "output.h"

#include <google/protobuf/util/json_util.h>
#include <util/string/printf.h>

namespace NKikimr::NPDiskTool {

bool PrintMessage(const google::protobuf::Message& msg, bool json, IOutputStream& out) {
    if (!json) {
        return false;
    }
    using namespace google::protobuf::util;
    TString jsonStr;
    JsonPrintOptions opts;
    opts.preserve_proto_field_names = true;
    opts.add_whitespace = true;
    const auto st = MessageToJsonString(msg, &jsonStr, opts);
    if (!st.ok()) {
        Cerr << "JSON serialization failed: " << st.ToString() << Endl;
        return true;
    }
    out << jsonStr;
    return true;
}

void PrintIssues(const TIssueLog& issues, IOutputStream& err) {
    for (const auto& i : issues.Items) {
        err << i.Severity << ": [" << i.Location << "] " << i.Message;
        if (i.Guessed) {
            err << " (guessed)";
        }
        err << Endl;
    }
}

static void FillOwner(const TOwnerState& o, ui32 ownerId, ui32 chunksOwned, NKikimr::NPdiskTool::TOwnerInfo* p) {
    p->SetOwnerId(ownerId);
    p->SetVDiskId(o.VDiskId.ToString());
    p->SetGroupId(o.VDiskId.GroupID.GetRawId());
    p->SetGroupGeneration(o.VDiskId.GroupGeneration);
    p->SetFailRealm(o.VDiskId.FailRealm);
    p->SetFailDomain(o.VDiskId.FailDomain);
    p->SetVDisk(o.VDiskId.VDisk);
    p->SetGroupSizeInUnits(o.GroupSizeInUnits);
    p->SetChunksOwned(chunksOwned);
    p->SetCurrentFirstLsnToKeep(o.CurrentFirstLsnToKeep);
    p->SetFirstNonceToKeep(o.FirstNonceToKeep);
    p->SetLastWrittenCommitLsn(o.LastWrittenCommitLsn);
    for (const auto& [sig, rec] : o.StartingPoints) {
        auto* sp = p->AddStartingPoints();
        sp->SetSignature(sig);
        sp->SetSignatureName(SignatureName(sig));
        sp->SetLsn(rec.first);
        sp->SetPayloadSize(rec.second.size());
    }
}

static TVector<ui32> CountChunks(const TParsedSysLog& state) {
    TVector<ui32> counts(256);
    for (ui32 i = 0; i < state.Chunks.size(); ++i) {
        counts[state.Chunks[i].OwnerId]++;
    }
    return counts;
}

void FillOwnersProto(const TParsedSysLog& state, NKikimr::NPdiskTool::TOwnersResult& proto, bool withChunks) {
    const auto counts = CountChunks(state);
    for (ui32 i = 0; i < state.Owners.size(); ++i) {
        if (state.Owners[i].VDiskId == TVDiskID::InvalidId) {
            continue;
        }
        auto* p = proto.AddOwners();
        FillOwner(state.Owners[i], i, counts[i], p);
        if (withChunks) {
            for (ui32 c = 0; c < state.Chunks.size(); ++c) {
                if (state.Chunks[c].OwnerId == i) {
                    auto* ch = p->AddChunks();
                    ch->SetChunkIdx(c);
                    ch->SetOwnerId(i);
                    ch->SetCommitState(CommitStateName(state.Chunks[c].CommitState));
                    ch->SetNonce(state.Chunks[c].Nonce);
                }
            }
        }
    }
}

void PrintOwnersText(const NKikimr::NPdiskTool::TOwnersResult& proto, IOutputStream& out) {
    out << "OwnerId\tVDiskId\tGroupId\tGroupSizeInUnits\tChunksOwned\tCurLsnToKeep\tFirstNonceToKeep\tLastWrittenCommitLsn\tStartingPoints" << Endl;
    for (const auto& o : proto.GetOwners()) {
        out << o.GetOwnerId() << "\t" << o.GetVDiskId() << "\t" << o.GetGroupId()
            << "\t" << o.GetGroupSizeInUnits() << "\t" << o.GetChunksOwned()
            << "\t" << o.GetCurrentFirstLsnToKeep() << "\t" << o.GetFirstNonceToKeep()
            << "\t" << o.GetLastWrittenCommitLsn() << "\t";
        bool first = true;
        for (const auto& sp : o.GetStartingPoints()) {
            if (!first) {
                out << ",";
            }
            first = false;
            out << sp.GetSignatureName() << "@" << sp.GetLsn();
        }
        out << Endl;
        for (const auto& c : o.GetChunks()) {
            out << "  chunk " << c.GetChunkIdx() << " state=" << c.GetCommitState()
                << " nonce=" << c.GetNonce() << Endl;
        }
    }
}

void FillChunksProto(const TParsedSysLog& state, NKikimr::NPdiskTool::TChunksResult& proto) {
    for (ui32 i = 0; i < state.Chunks.size(); ++i) {
        auto* c = proto.AddChunks();
        c->SetChunkIdx(i);
        c->SetOwnerId(state.Chunks[i].OwnerId);
        c->SetCommitState(CommitStateName(state.Chunks[i].CommitState));
        c->SetNonce(state.Chunks[i].Nonce);
    }
}

void PrintChunksText(const NKikimr::NPdiskTool::TChunksResult& proto, IOutputStream& out) {
    out << "ChunkIdx\tOwnerId\tCommitState\tNonce" << Endl;
    for (const auto& c : proto.GetChunks()) {
        out << c.GetChunkIdx() << "\t" << c.GetOwnerId() << "\t" << c.GetCommitState()
            << "\t" << c.GetNonce() << Endl;
    }
}

void FillLogChunksProto(const TLogScanResult& log, const TParsedSysLog& /*state*/, NKikimr::NPdiskTool::TLogChunksResult& proto) {
    for (const auto& lc : log.LogChunks) {
        auto* p = proto.AddChunks();
        p->SetChunkIdx(lc.ChunkIdx);
        p->SetIsCommitted(lc.IsCommitted);
        p->SetFirstNonce(lc.FirstNonce);
        p->SetLastNonce(lc.LastNonce);
        p->SetCurrentUserCount(lc.CurrentUserCount);
        for (ui32 o = 0; o < 256; ++o) {
            if (lc.OwnerLsnRange[o].Present) {
                auto* r = p->AddOwnerRanges();
                r->SetOwnerId(o);
                r->SetFirstLsn(lc.OwnerLsnRange[o].FirstLsn);
                r->SetLastLsn(lc.OwnerLsnRange[o].LastLsn);
            }
        }
    }
}

void PrintLogChunksText(const NKikimr::NPdiskTool::TLogChunksResult& proto, IOutputStream& out) {
    out << "#\tChunkId\tIsCommitted\tNonces\tUsers" << Endl;
    ui32 i = 0;
    for (const auto& c : proto.GetChunks()) {
        out << i++ << "\t" << c.GetChunkIdx() << "\t" << c.GetIsCommitted()
            << "\t[" << c.GetFirstNonce() << "," << c.GetLastNonce() << "]\t"
            << c.GetCurrentUserCount();
        for (const auto& r : c.GetOwnerRanges()) {
            out << " owner" << r.GetOwnerId() << "=[" << r.GetFirstLsn() << "," << r.GetLastLsn() << "]";
        }
        out << Endl;
    }
}

void FillSysLogProto(const TSysLogReadResult& raw, const TParsedSysLog& state, NKikimr::NPdiskTool::TSysLogResult& proto) {
    auto* rec = proto.MutableRecord();
    rec->SetVersion(state.Record.Version);
    rec->SetNonceSysLog(state.Record.Nonces.Value[NPDisk::NonceSysLog]);
    rec->SetNonceLog(state.Record.Nonces.Value[NPDisk::NonceLog]);
    rec->SetNonceData(state.Record.Nonces.Value[NPDisk::NonceData]);
    rec->SetLogHeadChunkIdx(state.Record.LogHeadChunkIdx);
    rec->SetLogHeadChunkPreviousNonce(state.Record.LogHeadChunkPreviousNonce);
    rec->SetLsn(raw.Lsn);
    rec->SetFirstLogChunkToParseCommits(state.FirstLogChunkToParseCommits);
    const auto counts = CountChunks(state);
    for (ui32 i = 0; i < state.Owners.size(); ++i) {
        if (state.Owners[i].VDiskId == TVDiskID::InvalidId) {
            continue;
        }
        FillOwner(state.Owners[i], i, counts[i], rec->AddOwners());
    }
    proto.SetLoopOffset(raw.LoopOffset);
    proto.SetBestNonce(raw.BestNonce);
    for (const auto& s : raw.SectorSets) {
        auto* p = proto.AddSectorSets();
        p->SetSetIdx(s.SetIdx);
        p->SetNonce(s.Nonce);
        p->SetGoodSectorFlags(s.GoodSectorFlags);
        p->SetHasStart(s.HasStart);
        p->SetHasEnd(s.HasEnd);
        p->SetIsConsistent(s.IsConsistent);
        p->SetIsNonceReversal(s.IsNonceReversal);
    }
}

void PrintSysLogText(const NKikimr::NPdiskTool::TSysLogResult& proto, IOutputStream& out) {
    const auto& r = proto.GetRecord();
    out << "SysLog version=" << r.GetVersion()
        << " logHead=" << r.GetLogHeadChunkIdx()
        << " prevNonce=" << r.GetLogHeadChunkPreviousNonce()
        << " lsn=" << r.GetLsn()
        << " firstChunkToParseCommits=" << r.GetFirstLogChunkToParseCommits() << Endl;
    out << "Nonces: syslog=" << r.GetNonceSysLog() << " log=" << r.GetNonceLog() << " data=" << r.GetNonceData() << Endl;
    out << "LoopOffset=" << proto.GetLoopOffset() << " BestNonce=" << proto.GetBestNonce() << Endl;
    out << "SectorSets:" << Endl;
    for (const auto& s : proto.GetSectorSets()) {
        out << "  [" << s.GetSetIdx() << "] nonce=" << s.GetNonce()
            << " flags=" << s.GetGoodSectorFlags()
            << " start=" << s.GetHasStart()
            << " end=" << s.GetHasEnd()
            << " consistent=" << s.GetIsConsistent()
            << " reversal=" << s.GetIsNonceReversal() << Endl;
    }
    out << "Owners:" << Endl;
    NKikimr::NPdiskTool::TOwnersResult tmp;
    for (const auto& o : r.GetOwners()) {
        *tmp.AddOwners() = o;
    }
    PrintOwnersText(tmp, out);
}

void FillParseLogProto(const TLogScanResult& log, ui32 ownerFilter, NKikimr::NPdiskTool::TParseLogResult& proto) {
    ui64 n = 0;
    for (const auto& rec : log.Records) {
        if (ownerFilter != Max<ui32>() && rec.OwnerId != ownerFilter) {
            continue;
        }
        auto* p = proto.AddRecords();
        p->SetOwnerId(rec.OwnerId);
        p->SetSignature(rec.Signature);
        p->SetSignatureName(SignatureName(rec.Signature.GetUnmasked()));
        p->SetLsn(rec.Lsn);
        p->SetNonce(rec.Nonce);
        p->SetChunkIdx(rec.ChunkIdx);
        p->SetPayloadSize(rec.RawPayload.size());
        p->SetHasCommitRecord(rec.HasCommit);
        p->SetFirstLsnToKeep(rec.FirstLsnToKeep);
        p->SetIsStartingPoint(rec.IsStartingPoint);
        for (auto c : rec.CommitChunks) {
            p->AddCommitChunks(c);
        }
        for (auto c : rec.DeleteChunks) {
            p->AddDeleteChunks(c);
        }
        ++n;
    }
    proto.SetRecordCount(n);
}

void PrintParseLogText(const NKikimr::NPdiskTool::TParseLogResult& proto, IOutputStream& out) {
    out << "Owner\tLsn\tSignature\tNonce\tChunk\tPayload\tCommit" << Endl;
    for (const auto& r : proto.GetRecords()) {
        out << r.GetOwnerId() << "\t" << r.GetLsn() << "\t" << r.GetSignatureName()
            << "\t" << r.GetNonce() << "\t" << r.GetChunkIdx() << "\t" << r.GetPayloadSize();
        if (r.GetHasCommitRecord()) {
            out << "\tcommit firstLsnToKeep=" << r.GetFirstLsnToKeep()
                << " starting=" << r.GetIsStartingPoint();
        }
        out << Endl;
    }
    out << "Total records: " << proto.GetRecordCount() << Endl;
}

void PrintBlobsText(const NKikimr::NPdiskTool::TBlobsResult& proto, IOutputStream& out) {
    out << "LogoBlobId\tParts" << Endl;
    for (const auto& b : proto.GetBlobs()) {
        out << b.GetLogoBlobId();
        for (const auto& p : b.GetParts()) {
            out << "\tpart" << p.GetPartId() << "=" << p.GetBlobType();
            if (p.GetChunkIdx()) { // inline parts from the log have no on-disk location
                out << ":" << p.GetChunkIdx() << "+" << p.GetOffset();
            }
            out << "/" << p.GetSize();
            if (p.GetCopies() > 1) {
                out << " (copy of " << p.GetCopies() << ")";
            }
            if (p.GetPacked()) {
                out << " (packed)";
            }
        }
        out << Endl;
    }
    out << "listed: " << proto.GetTotalListed() << Endl;
    if (proto.GetSkippedWithoutData()) {
        out << "skipped without data: " << proto.GetSkippedWithoutData()
            << " (use --all to list them)" << Endl;
    }
    if (proto.HasContinueToken()) {
        out << "continue-token: " << proto.GetContinueToken() << Endl;
    }
}

void PrintBarriersText(const NKikimr::NPdiskTool::TBarriersResult& proto, IOutputStream& out) {
    out << "Tablet\tChannel\tHard\tGen\tGenCounter\tCollectGen\tCollectStep" << Endl;
    for (const auto& b : proto.GetBarriers()) {
        out << b.GetTabletId() << "\t" << b.GetChannel() << "\t" << b.GetHard()
            << "\t" << b.GetGen() << "\t" << b.GetGenCounter()
            << "\t" << b.GetCollectGen() << "\t" << b.GetCollectStep() << Endl;
    }
}

void PrintBlocksText(const NKikimr::NPdiskTool::TBlocksResult& proto, IOutputStream& out) {
    out << "Tablet\tBlockedGeneration" << Endl;
    for (const auto& b : proto.GetBlocks()) {
        out << b.GetTabletId() << "\t" << b.GetBlockedGeneration() << Endl;
    }
}

void PrintVerifyText(const NKikimr::NPdiskTool::TVerifyResult& proto, IOutputStream& out) {
    out << "FormatReplicasOk: " << proto.GetFormatReplicasOk() << Endl;
    out << "SysLogSetsOk: " << proto.GetSysLogSetsOk() << Endl;
    out << "LogRecords: " << proto.GetLogRecords() << Endl;
    out << "DataSectorsScanned: " << proto.GetDataSectorsScanned() << Endl;
    out << "DataSectorsUnwritten: " << proto.GetDataSectorsUnwritten() << Endl;
    out << "ReferencedSectorsChecked: " << proto.GetReferencedSectorsChecked() << Endl;
    out << "ReferencedSectorsBad: " << proto.GetReferencedSectorsBad() << Endl;
}

TString HexDump(const void* data, ui32 size) {
    TStringStream out;
    const ui8* p = static_cast<const ui8*>(data);
    const ui32 width = 16;
    for (ui32 row = 0; row * width < size; ++row) {
        out << Sprintf("%06x: ", row * width);
        for (ui32 col = 0; col < width; ++col) {
            const ui32 idx = row * width + col;
            if (col) {
                out << ' ';
            }
            if (idx < size) {
                out << Sprintf("%02x", p[idx]);
            } else {
                out << "  ";
            }
        }
        out << '\n';
    }
    return out.Str();
}

} // namespace NKikimr::NPDiskTool

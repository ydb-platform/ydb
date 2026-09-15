#include "commands.h"

#include "blobs.h"
#include "blobsource.h"
#include "chunk.h"
#include "keys.h"
#include "output.h"
#include "sector.h"
#include "session.h"
#include "tabletdb.h"
#include "tabletdump.h"
#include "tabletlog.h"

#include <ydb/core/erasure/erasure.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

namespace NKikimr::NPDiskTool {

using namespace NLastGetopt;

void PrintUsage(const TString& argv0) {
    Cerr << "Usage: " << argv0 << " <command> [options]\n\n"
        << "PDisk/VDisk disaster-recovery tool. Read-only; run against a stopped ydbd.\n\n"
        << "Commands:\n"
        << "  format            Decrypt and dump the three format replicas\n"
        << "  syslog            Reconstruct and dump SysLog\n"
        << "  owners            List VDisk owners (PDisk web monitoring fields)\n"
        << "  chunks            Full chunk map\n"
        << "  log-chunks        Log-chunk table (nonce ranges, per-owner LSNs)\n"
        << "  starting-points   Last record per signature per owner\n"
        << "  export-chunk      Decrypt a chunk to a file (--chunk N --output FILE)\n"
        << "  export-log        Decrypt the log chain to a binary container\n"
        << "  parse-log         Parse a log export or the live device\n"
        << "  blobs             List logo blobs this VDisk holds data for (--all for the rest;\n"
        << "                    --tablet, --channel, --from, --to to narrow it down)\n"
        << "  barriers          List barriers for a VDisk\n"
        << "  blocks            List blocks for a VDisk\n"
        << "  export-blob       Export this VDisk's blob parts to files, comparing duplicate copies\n"
        << "  restore-tablet    Rebuild a flat tablet's tables from export-blob directories\n"
        << "                    (--tablet ID --blobs DIR [--blobs DIR ...] --erasure NAME --output DIR)\n"
        << "  verify            Scan format/syslog/log/data and list issues\n"
        << "  dump-sector       Hex-dump a physical sector\n"
        << "  metadata          Dump the metadata vault blob if present\n\n"
        << "Global options:\n"
        << "  --device PATH     File or block device (required for most commands)\n"
        << "  --main-key NUM    Encryption main key (repeatable; decimal, 0x-hex, or YdbDefaultPDiskSequence)\n"
        << "  --key-file PATH   ydbd TKeyConfig proto (--pdisk-key-file) or a raw key container\n"
        << "  --pin STR         Pin for a raw --key-file container (default EmptyPin)\n"
        << "  --format text|json  Output format (default text)\n"
        << "  --show-keys       Include encryption keys in format output\n"
        << "  --strict          Fail on the first inconsistency\n"
        << "  --no-lock         Do not try to take a shared flock\n";
}

struct TGlobals {
    TString Device;
    TVector<ui64> MainKeys;
    TString KeyFile;
    TString Pin;
    TString Format = "text";
    bool ShowKeys = false;
    bool Strict = false;
    bool NoLock = false;

    bool Json() const {
        return Format == "json";
    }

    TSessionOptions SessionOpts(TIssueLog& issues) const {
        TSessionOptions opts;
        opts.MainKey = MakeMainKey(MainKeys, KeyFile, Pin, !MainKeys.empty() || !KeyFile.empty(), issues);
        opts.Strict = Strict;
        opts.ShowKeys = ShowKeys;
        opts.TryLock = !NoLock;
        return opts;
    }
};

static void AddGlobals(TOpts& opts, TGlobals& g) {
    opts.AddLongOption("device", "path to PDisk file or block device")
        .RequiredArgument("PATH").StoreResult(&g.Device);
    opts.AddLongOption("main-key", "encryption main key: decimal, 0x-hex, or YdbDefaultPDiskSequence (repeatable)")
        .RequiredArgument("NUM").Handler1T<TString>([&g](const TString& value) {
            g.MainKeys.push_back(ParseMainKeyArg(value));
        });
    opts.AddLongOption("key-file", "ydbd TKeyConfig proto or raw key container")
        .RequiredArgument("PATH").StoreResult(&g.KeyFile);
    opts.AddLongOption("pin", "pin for --key-file").RequiredArgument("STR").StoreResult(&g.Pin);
    opts.AddLongOption("format", "text or json").RequiredArgument("FMT").StoreResult(&g.Format);
    opts.AddLongOption("show-keys", "include keys in format output").NoArgument().SetFlag(&g.ShowKeys);
    opts.AddLongOption("strict", "fail on the first error").NoArgument().SetFlag(&g.Strict);
    opts.AddLongOption("no-lock", "do not flock the device").NoArgument().SetFlag(&g.NoLock);
    opts.AddHelpOption();
}

static int Finish(const google::protobuf::Message& proto, const TIssueLog& issues, bool json, bool fatal) {
    PrintIssues(issues, Cerr);
    if (json) {
        PrintMessage(proto, true, Cout);
    }
    if (fatal || (issues.Strict && issues.HasErrors()) || issues.StrictTriggered) {
        return 1;
    }
    return 0;
}

template <class TProto, class TText>
static int FinishText(TProto& proto, const TIssueLog& issues, bool json, TText&& text, bool fatal = false) {
    issues.FillProto(proto.MutableIssues());
    if (!json) {
        text(proto, Cout);
    }
    return Finish(proto, issues, json, fatal);
}

static bool OpenSession(const TGlobals& g, TPDiskSession& session, bool requireFormat = true) {
    if (!g.Device) {
        Cerr << "--device is required" << Endl;
        return false;
    }
    TSessionOptions opts = g.SessionOpts(session.Issues);
    if (!session.OpenFile(g.Device, opts, requireFormat)) {
        PrintIssues(session.Issues, Cerr);
        return false;
    }
    return true;
}

static TMaybe<TErasureType::EErasureSpecies> ParseErasure(const TString& name, TIssueLog& issues) {
    if (!name) {
        return {};
    }
    TErasureType::EErasureSpecies s;
    if (!TErasureType::ParseErasureName(s, name)) {
        issues.Error("erasure", TStringBuilder() << "Unknown erasure name: " << name
            << " (try none, block-4-2, mirror-3-dc, block-4-3, mirror-3of4)");
        return {};
    }
    return s;
}

static TMaybe<TLogoBlobID> ParseBlobId(const TString& s, TIssueLog& issues, const TString& loc) {
    if (!s) {
        return {};
    }
    TLogoBlobID id;
    TString err;
    if (!TLogoBlobID::Parse(id, s, err)) {
        issues.Error(loc, TStringBuilder() << "Cannot parse LogoBlobID " << s << ": " << err);
        return {};
    }
    return id;
}

int CmdFormat(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session, /*requireFormat=*/ false)) {
        return 1;
    }
    NKikimr::NPdiskTool::TFormatResult proto;
    FillFormatProto(session.FormatResult, proto, g.ShowKeys);
    session.Issues.FillProto(proto.MutableIssues());
    if (!g.Json()) {
        PrintFormatText(proto, Cout);
    }
    return Finish(proto, session.Issues, g.Json(), !session.FormatResult.Ok);
}

int CmdSysLog(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TSysLogResult proto;
    FillSysLogProto(session.SysLogRaw, session.State, proto);
    return FinishText(proto, session.Issues, g.Json(), PrintSysLogText, !session.SysLogRaw.Ok);
}

int CmdOwners(const TGlobals& g, bool withChunks) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TOwnersResult proto;
    FillOwnersProto(session.State, proto, withChunks);
    return FinishText(proto, session.Issues, g.Json(), PrintOwnersText);
}

int CmdChunks(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TChunksResult proto;
    FillChunksProto(session.State, proto);
    return FinishText(proto, session.Issues, g.Json(), PrintChunksText);
}

int CmdLogChunks(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TLogChunksResult proto;
    FillLogChunksProto(session.Log, session.State, proto);
    return FinishText(proto, session.Issues, g.Json(), PrintLogChunksText);
}

int CmdStartingPoints(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TStartingPointsResult proto;
    NKikimr::NPdiskTool::TOwnersResult tmp;
    FillOwnersProto(session.State, tmp, false);
    for (auto& o : *tmp.MutableOwners()) {
        proto.AddOwners()->Swap(&o);
    }
    auto print = [](const NKikimr::NPdiskTool::TStartingPointsResult& p, IOutputStream& out) {
        out << "OwnerId\tVDiskId\tSignature\tLsn\tPayloadSize" << Endl;
        for (const auto& o : p.GetOwners()) {
            for (const auto& sp : o.GetStartingPoints()) {
                out << o.GetOwnerId() << "\t" << o.GetVDiskId() << "\t" << sp.GetSignatureName()
                    << "\t" << sp.GetLsn() << "\t" << sp.GetPayloadSize() << Endl;
            }
        }
    };
    return FinishText(proto, session.Issues, g.Json(), print);
}

int CmdExportChunk(const TGlobals& g, ui32 chunk, const TString& output, bool raw) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    const ui32 chunkCount = session.Format.DiskSizeChunks();
    if (chunkCount && chunk >= chunkCount) {
        session.Issues.Error("export-chunk", TStringBuilder() << "Chunk " << chunk
            << " is outside the disk, which holds " << chunkCount << " chunk(s)");
        PrintIssues(session.Issues, Cerr);
        return 1;
    }
    NKikimr::NPdiskTool::TExportResult proto;
    proto.SetPath(output);
    proto.SetChunkIdx(chunk);
    ui64 bytes = 0;
    ui32 gaps = 0;
    WriteChunkToFile(*session.Device, session.Format, session.State, chunk, output, raw,
        session.Issues, bytes, gaps);
    proto.SetBytesWritten(bytes);
    proto.SetGaps(gaps);
    auto print = [](const NKikimr::NPdiskTool::TExportResult& p, IOutputStream& out) {
        out << "Wrote " << p.GetBytesWritten() << " bytes to " << p.GetPath()
            << " (gaps=" << p.GetGaps() << ")" << Endl;
    };
    return FinishText(proto, session.Issues, g.Json(), print);
}

int CmdExportLog(const TGlobals& g, const TString& output, TMaybe<ui32> owner) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TExportResult proto;
    proto.SetPath(output);
    ui64 bytes = 0;
    WriteLogExport(session.Format, session.Log, output, owner.GetOrElse(Max<ui32>()), session.Issues, bytes);
    proto.SetBytesWritten(bytes);
    auto print = [](const NKikimr::NPdiskTool::TExportResult& p, IOutputStream& out) {
        out << "Wrote " << p.GetBytesWritten() << " bytes to " << p.GetPath() << Endl;
    };
    return FinishText(proto, session.Issues, g.Json(), print);
}

int CmdParseLog(const TGlobals& g, const TString& input, TMaybe<ui32> owner) {
    TIssueLog issues;
    issues.Strict = g.Strict;
    TLogScanResult scan;
    if (input) {
        scan = ReadLogExport(input, issues);
    } else {
        TPDiskSession session;
        if (!OpenSession(g, session)) {
            return 1;
        }
        issues = session.Issues;
        scan = session.Log;
    }
    NKikimr::NPdiskTool::TParseLogResult proto;
    FillParseLogProto(scan, owner.GetOrElse(Max<ui32>()), proto);
    return FinishText(proto, issues, g.Json(), PrintParseLogText);
}

static bool LoadHull(
    TPDiskSession& session,
    const TString& vdisk,
    TMaybe<ui32> ownerId,
    const TString& erasure,
    THullSnapshot& snap)
{
    if (!session.SysLogRaw.Ok) {
        session.Issues.Error("hull", "SysLog could not be reconstructed, so there is no owner table"
            " and no log to replay; run the syslog command to see why");
        return false;
    }
    const TOwner owner = session.ResolveOwner(vdisk, ownerId, session.Issues);
    if (session.Issues.HasErrors() && owner == 0 && !ownerId) {
        return false;
    }
    auto er = ParseErasure(erasure, session.Issues);
    snap = ReconstructHull(*session.Device, session.Format, session.State, session.Log, owner, er, session.Issues);
    return true;
}

int CmdBlobs(const TGlobals& g, const TString& vdisk, TMaybe<ui32> owner, const TString& erasure, const TListFilter& filter) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    THullSnapshot snap;
    if (!LoadHull(session, vdisk, owner, erasure, snap)) {
        PrintIssues(session.Issues, Cerr);
        return 1;
    }
    NKikimr::NPdiskTool::TBlobsResult proto;
    ListBlobs(snap, filter, session.Issues, proto);
    return FinishText(proto, session.Issues, g.Json(), PrintBlobsText);
}

int CmdBarriers(const TGlobals& g, const TString& vdisk, TMaybe<ui32> owner, const TString& erasure, const TListFilter& filter) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    THullSnapshot snap;
    if (!LoadHull(session, vdisk, owner, erasure, snap)) {
        PrintIssues(session.Issues, Cerr);
        return 1;
    }
    NKikimr::NPdiskTool::TBarriersResult proto;
    ListBarriers(snap, filter, proto);
    return FinishText(proto, session.Issues, g.Json(), PrintBarriersText);
}

int CmdBlocks(const TGlobals& g, const TString& vdisk, TMaybe<ui32> owner, const TString& erasure, const TListFilter& filter) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    THullSnapshot snap;
    if (!LoadHull(session, vdisk, owner, erasure, snap)) {
        PrintIssues(session.Issues, Cerr);
        return 1;
    }
    NKikimr::NPdiskTool::TBlocksResult proto;
    ListBlocks(snap, filter, proto);
    return FinishText(proto, session.Issues, g.Json(), PrintBlocksText);
}

int CmdExportBlob(
    const TGlobals& g,
    const TString& vdisk,
    TMaybe<ui32> owner,
    const TString& erasure,
    const TString& id,
    const TString& from,
    const TString& to,
    const TListFilter& filter,
    const TString& output)
{
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    THullSnapshot snap;
    if (!LoadHull(session, vdisk, owner, erasure, snap)) {
        PrintIssues(session.Issues, Cerr);
        return 1;
    }
    auto fromId = ParseBlobId(id ? id : from, session.Issues, "export-blob");
    auto toId = id ? fromId : ParseBlobId(to, session.Issues, "export-blob");
    if (!fromId && !filter.TabletId && !filter.Channel) {
        session.Issues.Error("export-blob", "Specify --id, --from, --tablet or --channel");
        PrintIssues(session.Issues, Cerr);
        return 1;
    }
    TExportStats stats;
    ExportBlobParts(*session.Device, session.Format, session.State, snap,
        fromId.GetOrElse(TLogoBlobID()), toId, filter, output, session.Issues, stats);
    NKikimr::NPdiskTool::TExportBlobResult proto;
    proto.SetPath(output);
    proto.SetBlobs(stats.Blobs);
    proto.SetParts(stats.Parts);
    proto.SetPartsWithSeveralCopies(stats.PartsWithSeveralCopies);
    proto.SetPartsWithDifferingCopies(stats.PartsWithDifferingCopies);
    auto print = [](const NKikimr::NPdiskTool::TExportBlobResult& p, IOutputStream& out) {
        out << "Exported " << p.GetParts() << " part(s) of " << p.GetBlobs()
            << " blob(s) to " << p.GetPath() << Endl;
        out << "parts with several copies: " << p.GetPartsWithSeveralCopies()
            << ", of them differing: " << p.GetPartsWithDifferingCopies() << Endl;
    };
    return FinishText(proto, session.Issues, g.Json(), print, stats.PartsWithDifferingCopies > 0);
}

struct TRestoreTabletArgs {
    ui64 TabletId = 0;
    TVector<TString> Dirs;
    TString Erasure;
    ui32 MaxGeneration = Max<ui32>();
    TDumpOptions Dump;
};

int CmdRestoreTablet(const TGlobals& g, const TRestoreTabletArgs& args) {
    TIssueLog issues;
    issues.Strict = g.Strict;

    NKikimr::NPdiskTool::TRestoreTabletResult proto;
    proto.SetTabletId(args.TabletId);
    proto.SetOutput(args.Dump.Output);

    auto species = ParseErasure(args.Erasure, issues);
    if (issues.HasErrors()) {
        PrintIssues(issues, Cerr);
        return 1;
    }
    if (!species) {
        issues.Warning("restore-tablet", "No --erasure was given, so only blobs whose parts already"
            " span the whole body can be used; give the group erasure to recover the rest");
    }

    TMaybe<TErasureType> erasure;
    if (species) {
        erasure = TErasureType(*species);
    }

    TBlobStore store(erasure, issues);
    for (const TString& dir : args.Dirs) {
        store.AddDirectory(dir);
    }
    store.FlushIssues();

    const auto& blobStats = store.Stats();
    proto.SetBlobsFound(blobStats.Blobs);
    proto.SetBlobsRestored(blobStats.Restored);
    proto.SetBlobsUnrecoverable(blobStats.Unrecoverable);
    proto.SetPartsWithDifferingCopies(blobStats.DisagreeingParts);

    if (!blobStats.Blobs) {
        issues.Error("restore-tablet", TStringBuilder() << "No exported blob parts were found in "
            << args.Dirs.size() << (args.Dirs.size() == 1 ? " input directory" : " input directories")
            << "; files are expected to be named the way export-blob names them");
        PrintIssues(issues, Cerr);
        return 1;
    }

    auto history = RebuildTabletHistory(store, args.TabletId, args.MaxGeneration, issues);
    proto.SetKeyEntryCandidatesTried(history.Stats.CandidatesTried);
    if (history.Ok) {
        proto.SetKeyEntry(history.KeyEntry.ToString());
        proto.SetSnapshotGeneration(history.Snapshot.first);
        proto.SetSnapshotStep(history.Snapshot.second);
        proto.SetConfirmedGeneration(history.Confirmed.first);
        proto.SetConfirmedStep(history.Confirmed.second);
        proto.SetLatestGeneration(history.Latest.first);
        proto.SetLatestStep(history.Latest.second);
        proto.SetGapsTolerated(history.Stats.GapsTolerated);
    }

    bool booted = false;
    TDumpStats dumpStats;
    if (history.Ok) {
        TTabletBoot boot(store, args.TabletId, issues);
        booted = boot.Run(history);

        const auto& bootStats = boot.Stats();
        proto.SetHasSnapshot(bootStats.HasSnapshot);
        proto.SetLogEntriesApplied(bootStats.RedoEntries);
        proto.SetLogEntriesSkipped(bootStats.RedoSkipped);
        proto.SetSchemeEntriesApplied(bootStats.AlterEntries);
        proto.SetSchemeEntriesSkipped(bootStats.AlterSkipped);
        proto.SetPartsLoaded(bootStats.BundlesLoaded);
        proto.SetPartsDropped(bootStats.BundlesDropped);
        proto.SetTxStatusLoaded(bootStats.TxStatusLoaded);
        proto.SetTxStatusDropped(bootStats.TxStatusDropped);
        proto.SetPagesRead(bootStats.PagesRead);
        proto.SetPagesMissing(bootStats.PagesMissing);
        proto.SetPagesCorrupt(bootStats.PagesCorrupt);

        if (booted) {
            // The page env has to outlive the iteration, so the dump happens while boot is alive.
            booted = DumpTablet(boot.Database(), boot.Pages(), args.Dump, issues, dumpStats);
        }
    }

    for (const auto& table : dumpStats.Tables) {
        auto* out = proto.AddTables();
        out->SetTableId(table.Table);
        out->SetName(table.Name);
        out->SetFile(table.File);
        out->SetRows(table.Rows);
        out->SetErasedRows(table.Erased);
        out->SetBytes(table.Bytes);
        out->SetComplete(table.Complete);
    }
    proto.SetTotalRows(dumpStats.Rows);
    proto.SetIncompleteTables(dumpStats.Incomplete);
    proto.SetDescription(dumpStats.Description);

    // The blobs that could not be reassembled explain most of what is missing above, so keep a sample
    // of them in the result instead of the whole list.
    constexpr size_t maxListed = 64;
    for (const auto& id : store.Unrecoverable()) {
        if (proto.UnrecoverableBlobsSize() >= maxListed) {
            break;
        }
        proto.AddUnrecoverableBlobs(id.ToString());
    }

    auto print = [](const NKikimr::NPdiskTool::TRestoreTabletResult& p, IOutputStream& out) {
        out << "Tablet " << p.GetTabletId() << Endl;
        out << "blobs: " << p.GetBlobsFound() << " found, " << p.GetBlobsRestored() << " restored, "
            << p.GetBlobsUnrecoverable() << " unrecoverable";
        if (p.GetPartsWithDifferingCopies()) {
            out << ", " << p.GetPartsWithDifferingCopies() << " part(s) had differing copies";
        }
        out << Endl;
        if (p.HasKeyEntry()) {
            out << "key entry: " << p.GetKeyEntry() << " (candidates tried "
                << p.GetKeyEntryCandidatesTried() << ")" << Endl;
            out << "snapshot: " << p.GetSnapshotGeneration() << ":" << p.GetSnapshotStep()
                << ", confirmed " << p.GetConfirmedGeneration() << ":" << p.GetConfirmedStep()
                << ", latest " << p.GetLatestGeneration() << ":" << p.GetLatestStep() << Endl;
            if (p.GetGapsTolerated()) {
                out << "the log had gaps, so some of the newest changes are missing" << Endl;
            }
            out << "log: " << p.GetLogEntriesApplied() << " applied, " << p.GetLogEntriesSkipped()
                << " skipped; scheme: " << p.GetSchemeEntriesApplied() << " applied, "
                << p.GetSchemeEntriesSkipped() << " skipped" << Endl;
            out << "parts: " << p.GetPartsLoaded() << " loaded, " << p.GetPartsDropped()
                << " dropped; pages: " << p.GetPagesRead() << " read, " << p.GetPagesMissing()
                << " missing, " << p.GetPagesCorrupt() << " corrupt" << Endl;
        }
        if (p.TablesSize()) {
            out << Endl << "TableId\tRows\tErased\tComplete\tFile" << Endl;
            for (const auto& t : p.GetTables()) {
                out << t.GetTableId() << "\t" << t.GetRows() << "\t" << t.GetErasedRows() << "\t"
                    << (t.GetComplete() ? "yes" : "no") << "\t" << t.GetFile() << Endl;
            }
            out << "total " << p.GetTotalRows() << " row(s)";
            if (p.GetIncompleteTables()) {
                out << ", " << p.GetIncompleteTables() << " table(s) truncated";
            }
            out << Endl;
        }
        if (p.GetDescription()) {
            out << "tables described in " << p.GetDescription() << Endl;
        }
        if (p.UnrecoverableBlobsSize()) {
            out << Endl << "Unrecoverable blobs (up to 64 shown):" << Endl;
            for (const auto& id : p.GetUnrecoverableBlobs()) {
                out << "  " << id << Endl;
            }
        }
    };

    return FinishText(proto, issues, g.Json(), print, !booted);
}

int CmdVerify(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TVerifyResult proto;
    ui64 formatOk = 0;
    for (const auto& r : session.FormatResult.Replicas) {
        if (r.HashOk) {
            ++formatOk;
        }
    }
    proto.SetFormatReplicasOk(formatOk);
    ui64 syslogOk = 0;
    for (const auto& s : session.SysLogRaw.SectorSets) {
        if (s.IsConsistent && s.GoodSectorFlags) {
            ++syslogOk;
        }
    }
    proto.SetSysLogSetsOk(syslogOk);
    proto.SetLogRecords(session.Log.Records.size());

    // A committed chunk is only partly written: huge slots are reserved before the data lands and an
    // SST leaves its tail free. Scan the chunks for a written/unwritten census, but keep quiet about
    // sectors with no valid hash -- they are the normal steady state, not damage.
    ui64 scanned = 0;
    ui64 unwritten = 0;
    const ui32 sectors = session.Format.ChunkSize / session.Format.SectorSize;
    for (ui32 i = 0; i < session.State.Chunks.size(); ++i) {
        if (!IsOwnerUser(session.State.Chunks[i].OwnerId)) {
            continue;
        }
        if (session.State.Chunks[i].CommitState != TChunkState::DATA_COMMITTED) {
            continue;
        }
        for (ui32 s = 0; s < sectors; ++s) {
            const ui64 offset = session.Format.Offset(i, s);
            ++scanned;
            auto restored = RestoreOneSector(*session.Device, session.Format, offset,
                session.Format.MagicDataChunk, session.Format.ChunkKey, false, session.Issues,
                TStringBuilder() << "verify[" << i << ":" << s << "]",
                {}, ESectorRef::Unreferenced);
            if (!restored.Ok) {
                ++unwritten;
            }
        }
    }
    proto.SetDataSectorsScanned(scanned);
    proto.SetDataSectorsUnwritten(unwritten);

    // Now the part that matters: every sector some index or blob part points at must hash correctly.
    ui64 refChecked = 0;
    ui64 refBad = 0;
    for (ui32 owner = 0; owner < session.State.Owners.size(); ++owner) {
        if (!IsOwnerUser(static_cast<TOwner>(owner))) {
            continue;
        }
        if (session.State.Owners[owner].VDiskId == TVDiskID::InvalidId) {
            continue;
        }
        auto snap = ReconstructHull(*session.Device, session.Format, session.State, session.Log,
            static_cast<TOwner>(owner), Nothing(), session.Issues);
        for (const auto& part : CollectReferencedParts(snap)) {
            auto res = CheckLogicalRange(*session.Device, session.Format, session.State, part.ChunkIdx,
                part.Offset, part.Size, session.Issues,
                TStringBuilder() << "verify-ref[owner " << owner << "]");
            refChecked += res.Checked;
            refBad += res.Bad;
        }
    }
    proto.SetReferencedSectorsChecked(refChecked);
    proto.SetReferencedSectorsBad(refBad);
    return FinishText(proto, session.Issues, g.Json(), PrintVerifyText);
}

int CmdDumpSector(const TGlobals& g, ui64 offset, ui32 size, bool decrypt) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    if (size == 0) {
        size = session.Format.SectorSize ? session.Format.SectorSize : 4096;
    }
    // The dump is hex-encoded into a proto field, so a stray --size must not turn into a huge read.
    const ui32 maxDump = 16u << 20;
    if (size > maxDump) {
        session.Issues.Warning("dump-sector", TStringBuilder() << "Size " << size
            << " is too large; dumping " << maxDump << " bytes");
        size = maxDump;
    }
    TVector<ui8> buf(size);
    session.Device->Pread(buf.data(), size, offset, session.Issues);
    NKikimr::NPdiskTool::TDumpSectorResult proto;
    proto.SetOffset(offset);
    proto.SetSize(size);
    proto.SetHex(HexDump(buf.data(), size));
    if (size >= sizeof(TDataSectorFooter)) {
        const auto* footer = reinterpret_cast<const TDataSectorFooter*>(buf.data() + size - sizeof(TDataSectorFooter));
        proto.SetNonce(footer->Nonce);
        proto.SetHash(footer->Hash);
        proto.SetEncrypted(footer->IsEncrypted());
    }
    if (decrypt && session.FormatResult.Ok) {
        auto restored = RestoreOneSector(*session.Device, session.Format, offset,
            session.Format.MagicDataChunk, session.Format.ChunkKey, true, session.Issues, "dump-sector");
        if (restored.Ok) {
            proto.SetHex(HexDump(restored.Payload.data(), restored.Payload.size()));
            proto.SetNonce(restored.Nonce);
            proto.SetEncrypted(restored.Encrypted);
        }
    }
    auto print = [](const NKikimr::NPdiskTool::TDumpSectorResult& p, IOutputStream& out) {
        out << "offset=" << p.GetOffset() << " size=" << p.GetSize()
            << " nonce=" << p.GetNonce() << " hash=" << p.GetHash()
            << " encrypted=" << p.GetEncrypted() << Endl;
        out << p.GetHex();
    };
    return FinishText(proto, session.Issues, g.Json(), print);
}

int CmdMetadata(const TGlobals& g) {
    TPDiskSession session;
    if (!OpenSession(g, session)) {
        return 1;
    }
    NKikimr::NPdiskTool::TMetadataResult proto;
    ReadMetadata(*session.Device, session.Opts.MainKey, session.FormatResult,
        session.SysLogRaw.Ok ? &session.State : nullptr, session.Issues, proto);
    auto print = [](const NKikimr::NPdiskTool::TMetadataResult& p, IOutputStream& out) {
        if (!p.GetPresent()) {
            out << "No metadata present" << Endl;
            return;
        }
        out << "SequenceNumber: " << p.GetSequenceNumber() << Endl;
        out << "Length: " << p.GetLength() << Endl;
        out << HexDump(p.GetData().data(), Min<ui32>(p.GetData().size(), 256));
        if (p.GetData().size() > 256) {
            out << "... (" << p.GetData().size() << " bytes)" << Endl;
        }
    };
    return FinishText(proto, session.Issues, g.Json(), print);
}

static TListFilter MakeFilter(ui32 limit, const TString& from, const TString& to,
    TMaybe<ui64> tablet, TMaybe<ui32> channel, const TString& token, TIssueLog& issues)
{
    TListFilter f;
    f.Limit = limit;
    f.ContinueToken = token;
    f.TabletId = tablet;
    f.Channel = channel;
    f.From = ParseBlobId(from, issues, "from");
    f.To = ParseBlobId(to, issues, "to");
    return f;
}

int RunCommand(const TString& command, int argc, char** argv) {
    TGlobals g;
    try {
        if (command == "format") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdFormat(g);
        } else if (command == "syslog") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdSysLog(g);
        } else if (command == "owners") {
            bool withChunks = false;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("chunks", "include per-owner chunk list").NoArgument().SetFlag(&withChunks);
            TOptsParseResult(&opts, argc, argv);
            return CmdOwners(g, withChunks);
        } else if (command == "chunks") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdChunks(g);
        } else if (command == "log-chunks") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdLogChunks(g);
        } else if (command == "starting-points") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdStartingPoints(g);
        } else if (command == "export-chunk") {
            ui32 chunk = Max<ui32>();
            TString output;
            bool raw = false;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("chunk", "chunk index").RequiredArgument("N").StoreResult(&chunk).Required();
            opts.AddLongOption("output", "output file").RequiredArgument("FILE").StoreResult(&output).Required();
            opts.AddLongOption("raw", "export physical bytes").NoArgument().SetFlag(&raw);
            TOptsParseResult(&opts, argc, argv);
            return CmdExportChunk(g, chunk, output, raw);
        } else if (command == "export-log") {
            TString output;
            TMaybe<ui32> owner;
            ui32 ownerVal = 0;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("output", "output file").RequiredArgument("FILE").StoreResult(&output).Required();
            opts.AddLongOption("owner", "filter by owner id").RequiredArgument("ID").StoreResult(&ownerVal);
            TOptsParseResult res(&opts, argc, argv);
            if (res.Has("owner")) {
                owner = ownerVal;
            }
            return CmdExportLog(g, output, owner);
        } else if (command == "parse-log") {
            TString input;
            ui32 ownerVal = 0;
            TMaybe<ui32> owner;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("input", "log export file (otherwise --device)").RequiredArgument("FILE").StoreResult(&input);
            opts.AddLongOption("owner", "filter by owner id").RequiredArgument("ID").StoreResult(&ownerVal);
            TOptsParseResult res(&opts, argc, argv);
            if (res.Has("owner")) {
                owner = ownerVal;
            }
            return CmdParseLog(g, input, owner);
        } else if (command == "blobs" || command == "barriers" || command == "blocks") {
            TString vdisk;
            TString erasure;
            ui32 ownerVal = 0;
            TMaybe<ui32> owner;
            ui32 limit = 10000;
            TString from, to, token;
            ui64 tablet = 0;
            ui32 channel = 0;
            TMaybe<ui64> tabletId;
            TMaybe<ui32> channelId;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("vdisk", "VDisk id [group:gen:realm:domain:vdisk]").RequiredArgument("ID").StoreResult(&vdisk);
            opts.AddLongOption("owner", "PDisk owner id").RequiredArgument("ID").StoreResult(&ownerVal);
            opts.AddLongOption("erasure", "group erasure (none, block-4-2, ...)").RequiredArgument("NAME").StoreResult(&erasure);
            opts.AddLongOption("tablet", "filter tablet id").RequiredArgument("ID").StoreResult(&tablet);
            opts.AddLongOption("limit", "max rows").RequiredArgument("N").StoreResult(&limit);
            opts.AddLongOption("continue-token", "paging token").RequiredArgument("TOK").StoreResult(&token);
            // Only offer the options the command actually honours: a block is keyed by tablet alone,
            // and the blob-id range and the data-only default are specific to blobs.
            if (command != "blocks") {
                opts.AddLongOption("channel", "filter channel").RequiredArgument("N").StoreResult(&channel);
            }
            if (command == "blobs") {
                opts.AddLongOption("from", "from LogoBlobID").RequiredArgument("ID").StoreResult(&from);
                opts.AddLongOption("to", "to LogoBlobID").RequiredArgument("ID").StoreResult(&to);
                opts.AddLongOption("all", "also list blobs whose data this VDisk does not hold").NoArgument();
            }
            TOptsParseResult res(&opts, argc, argv);
            if (res.Has("owner")) {
                owner = ownerVal;
            }
            if (res.Has("tablet")) {
                tabletId = tablet;
            }
            if (command != "blocks" && res.Has("channel")) {
                channelId = channel;
            }
            TIssueLog filterIssues;
            auto filter = MakeFilter(limit, from, to, tabletId, channelId, token, filterIssues);
            if (filterIssues.HasErrors()) {
                PrintIssues(filterIssues, Cerr);
                return 1;
            }
            filter.DataOnly = command == "blobs" && !res.Has("all");
            if (command == "blobs") {
                return CmdBlobs(g, vdisk, owner, erasure, filter);
            } else if (command == "barriers") {
                return CmdBarriers(g, vdisk, owner, erasure, filter);
            }
            return CmdBlocks(g, vdisk, owner, erasure, filter);
        } else if (command == "export-blob") {
            TString vdisk, erasure, id, from, to, output;
            ui32 ownerVal = 0;
            TMaybe<ui32> owner;
            ui64 tablet = 0;
            ui32 channel = 0;
            TMaybe<ui64> tabletId;
            TMaybe<ui32> channelId;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("vdisk", "VDisk id").RequiredArgument("ID").StoreResult(&vdisk);
            opts.AddLongOption("owner", "PDisk owner id").RequiredArgument("ID").StoreResult(&ownerVal);
            opts.AddLongOption("erasure", "group erasure").RequiredArgument("NAME").StoreResult(&erasure);
            opts.AddLongOption("id", "single LogoBlobID").RequiredArgument("ID").StoreResult(&id);
            opts.AddLongOption("from", "from LogoBlobID").RequiredArgument("ID").StoreResult(&from);
            opts.AddLongOption("to", "to LogoBlobID").RequiredArgument("ID").StoreResult(&to);
            opts.AddLongOption("tablet", "filter tablet id").RequiredArgument("ID").StoreResult(&tablet);
            opts.AddLongOption("channel", "filter channel").RequiredArgument("N").StoreResult(&channel);
            opts.AddLongOption("output", "output directory").RequiredArgument("DIR").StoreResult(&output).Required();
            TOptsParseResult res(&opts, argc, argv);
            if (res.Has("owner")) {
                owner = ownerVal;
            }
            if (res.Has("tablet")) {
                tabletId = tablet;
            }
            if (res.Has("channel")) {
                channelId = channel;
            }
            TIssueLog filterIssues;
            auto filter = MakeFilter(Max<ui32>(), TString(), TString(), tabletId, channelId, TString(), filterIssues);
            if (filterIssues.HasErrors()) {
                PrintIssues(filterIssues, Cerr);
                return 1;
            }
            return CmdExportBlob(g, vdisk, owner, erasure, id, from, to, filter, output);
        } else if (command == "restore-tablet") {
            TRestoreTabletArgs args;
            TOpts opts = TOpts::Default();
            // This command reads directories, not a device, so the device and key options do not apply.
            opts.AddLongOption("format", "text or json").RequiredArgument("FMT").StoreResult(&g.Format);
            opts.AddLongOption("strict", "fail on the first error").NoArgument().SetFlag(&g.Strict);
            opts.AddHelpOption();
            opts.AddLongOption("tablet", "tablet id").RequiredArgument("ID")
                .StoreResult(&args.TabletId).Required();
            opts.AddLongOption("blobs", "export-blob output directory (repeatable)")
                .RequiredArgument("DIR").Handler1T<TString>([&args](const TString& value) {
                    args.Dirs.push_back(value);
                });
            opts.AddLongOption("erasure", "group erasure (none, block-4-2, ...)")
                .RequiredArgument("NAME").StoreResult(&args.Erasure);
            opts.AddLongOption("output", "output directory").RequiredArgument("DIR")
                .StoreResult(&args.Dump.Output).Required();
            opts.AddLongOption("max-generation", "ignore log entries above this generation")
                .RequiredArgument("N").StoreResult(&args.MaxGeneration);
            opts.AddLongOption("csv", "comma separated instead of tab separated")
                .NoArgument().SetFlag(&args.Dump.Csv);
            opts.AddLongOption("tables-only", "describe the tables and stop")
                .NoArgument().SetFlag(&args.Dump.TablesOnly);
            opts.AddLongOption("include-erased", "keep erased rows, with a leading op column")
                .NoArgument().SetFlag(&args.Dump.IncludeErased);
            TOptsParseResult(&opts, argc, argv);
            if (args.Dirs.empty()) {
                Cerr << "--blobs is required at least once" << Endl;
                return 1;
            }
            return CmdRestoreTablet(g, args);
        } else if (command == "verify") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdVerify(g);
        } else if (command == "dump-sector") {
            ui64 offset = 0;
            ui32 size = 0;
            bool decrypt = false;
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            opts.AddLongOption("offset", "byte offset").RequiredArgument("N").StoreResult(&offset).Required();
            opts.AddLongOption("size", "bytes to dump (default sector size)").RequiredArgument("N").StoreResult(&size);
            opts.AddLongOption("decrypt", "decrypt as a data sector").NoArgument().SetFlag(&decrypt);
            TOptsParseResult(&opts, argc, argv);
            return CmdDumpSector(g, offset, size, decrypt);
        } else if (command == "metadata") {
            TOpts opts = TOpts::Default();
            AddGlobals(opts, g);
            TOptsParseResult(&opts, argc, argv);
            return CmdMetadata(g);
        } else if (command == "help" || command == "--help" || command == "-h") {
            PrintUsage("pdisktool");
            return 0;
        } else {
            Cerr << "Unknown command: " << command << Endl;
            PrintUsage("pdisktool");
            return 1;
        }
    } catch (const TUsageException& e) {
        Cerr << e.what() << Endl;
        return 2;
    } catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
}

} // namespace NKikimr::NPDiskTool

#include <ydb/tools/pdisktool/lib/blobsource.h>
#include <ydb/tools/pdisktool/lib/tabletdb.h>
#include <ydb/tools/pdisktool/lib/tabletdump.h>
#include <ydb/tools/pdisktool/lib/tabletlog.h>

#include <ydb/core/base/logoblob.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_boot_cookie.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/tablet_flat/flat_part_scheme.h>
#include <ydb/core/tablet_flat/flat_part_writer.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_store_solid.h>
#include <ydb/core/tablet_flat/flat_update_op.h>
#include <ydb/core/tablet_flat/flat_writer_bundle.h>
#include <ydb/core/tablet_flat/flat_writer_conf.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/cast.h>
#include <util/stream/file.h>
#include <util/string/split.h>

using namespace NKikimr;
using namespace NKikimr::NPDiskTool;

namespace {

using NTabletFlatExecutor::NBoot::TCookie;
using EIdx = TCookie::EIdx;

constexpr ui64 Tablet = 72075186224037888ull;
constexpr ui32 TableId = 1;

TString Pack(const TString& raw) {
    return NBlockCodecs::Codec("lz4fast")->Encode(raw);
}

ui32 CookieFor(EIdx index, ui32 sub = 0) {
    return TCookie(TCookie::EType::Log, index, sub).Raw;
}

// An `export-blob` output directory built by hand: the same `[id].part<N>[.copy<M>]` names the tool
// writes, so the store under test sees exactly what it sees in the field.
class TExport {
public:
    const TString& Path() const {
        return Dir.Name();
    }

    void PutPart(const TLogoBlobID& id, ui32 partId, const TString& body, ui32 copy = 0) {
        TStringBuilder name;
        name << id.FullID().ToString() << ".part" << partId;
        if (copy) {
            name << ".copy" << copy;
        }
        TFileOutput out((Dir.Path() / TString(name)).GetPath());
        out.Write(body);
        out.Finish();
    }

    // A whole body under part 1, which is what a store without an erasure species can use.
    TLogoBlobID PutWhole(ui32 gen, ui32 step, ui32 channel, ui32 cookie, const TString& body) {
        const TLogoBlobID id(Tablet, gen, step, channel, body.size(), cookie);
        PutPart(id, 1, body);
        return id;
    }

    void PutGlob(const NPageCollection::TGlobId& glob, const TString& body) {
        PutPart(glob.Logo, 1, body);
    }

private:
    TTempDir Dir;
};

void PutLogoBlobId(NKikimrProto::TLogoBlobID* proto, const TLogoBlobID& id) {
    LogoBlobIDFromLogoBlobID(id, proto);
}

struct TLogEntryBuilder {
    NKikimrTabletBase::TTabletLogEntry Entry;

    TLogEntryBuilder(ui32 snapGen, ui32 snapStep) {
        Entry.SetSnapshot(MakeGenStepPair(snapGen, snapStep));
    }

    TLogEntryBuilder& Confirmed(ui32 step) {
        Entry.SetConfirmed(step);
        return *this;
    }

    TLogEntryBuilder& Snapshot(bool total = false) {
        Entry.SetIsSnapshot(true);
        if (total) {
            Entry.SetIsTotalSnapshot(true);
        }
        return *this;
    }

    TLogEntryBuilder& Ref(const TLogoBlobID& id) {
        PutLogoBlobId(Entry.AddReferences(), id);
        return *this;
    }

    TLogEntryBuilder& Body(const TString& body) {
        Entry.SetEmbeddedLogBody(body);
        return *this;
    }

    TLogEntryBuilder& DependsOn(ui32 step) {
        Entry.AddDependsOn(step);
        return *this;
    }

    TString Serialize() const {
        return Entry.SerializeAsString();
    }
};

// The zero entry of a generation: it confirms a prefix of an earlier generation and accepts or declines
// the steps after it one bit at a time.
TString ZeroEntry(ui32 snapGen, ui32 snapStep, ui32 confirmedGen, ui32 confirmedStep,
        const TVector<bool>& tail)
{
    NKikimrTabletBase::TTabletLogEntry entry;
    entry.SetSnapshot(MakeGenStepPair(snapGen, snapStep));
    entry.SetZeroConfirmed(MakeGenStepPair(confirmedGen, confirmedStep));
    entry.SetZeroTailSz(tail.size());
    for (size_t word = 0; word < (tail.size() + 63) / 64; ++word) {
        ui64 bits = 0;
        for (size_t bit = 0; bit < 64 && word * 64 + bit < tail.size(); ++bit) {
            if (tail[word * 64 + bit]) {
                bits |= ui64(1) << bit;
            }
        }
        entry.AddZeroTailBitmask(bits);
    }
    return entry.SerializeAsString();
}

TString Complaints(const TIssueLog& issues) {
    TStringBuilder out;
    for (const auto& one : issues.Items) {
        out << one.Severity << ": [" << one.Location << "] " << one.Message << "\n";
    }
    return out;
}

TVector<TString> ReadLines(const TString& path) {
    TVector<TString> lines;
    TFileInput in(path);
    TString line;
    while (in.ReadLine(line)) {
        lines.push_back(line);
    }
    return lines;
}

// The four stages of the command, run the way CmdRestoreTablet runs them.
struct TRestored {
    TIssueLog Issues;
    TTempDir Output;
    TDumpOptions Options;
    TTabletLogHistory History;
    TDumpStats Dump;
    bool Ok = false;

    TVector<TString> Lines(ui32 table, const TString& name) const {
        const TString file = ToString(table) + "_" + name + (Options.Csv ? ".csv" : ".tsv");
        return ReadLines((Output.Path() / file).GetPath());
    }
};

void Restore(const TString& dir, TRestored& out, TMaybe<TErasureType> erasure = {}) {
    TBlobStore store(erasure, out.Issues);
    UNIT_ASSERT(store.AddDirectory(dir));
    store.FlushIssues();

    out.History = RebuildTabletHistory(store, Tablet, 0, out.Issues);
    if (!out.History.Ok) {
        return;
    }

    TTabletBoot boot(store, Tablet, out.Issues);
    if (!boot.Run(out.History)) {
        return;
    }

    out.Options.Output = out.Output.Name();
    out.Ok = DumpTablet(boot.Database(), boot.Pages(), out.Options, out.Issues, out.Dump);
}

// A database to take scheme deltas and redo bodies from, standing in for the tablet that wrote the log.
struct TSourceDb {
    struct TNoPages : public NTable::IPages {
        TResult Locate(const NTable::TMemTable*, ui64, ui32) override {
            return {true, nullptr};
        }

        TResult Locate(const NTable::TPart*, ui64, NTable::ELargeObj) override {
            return {true, nullptr};
        }

        const TSharedData* TryGetPage(const NTable::TPart*, TPageId, TGroupId) override {
            return nullptr;
        }
    };

    TNoPages Env;
    NTable::TDatabase Db;

    TString MakeScheme(ui32 gen, ui32 step) {
        Db.Begin(NTable::TTxStamp(gen, step), Env);
        Db.Alter()
            .AddTable("data", TableId)
            .AddColumn(TableId, "key", 1, NScheme::NTypeIds::Uint64, false, false)
            .AddColumn(TableId, "value", 2, NScheme::NTypeIds::String, false, false)
            .AddColumnToKey(TableId, 1);
        auto prod = Db.Commit(NTable::TTxStamp(gen, step), true);
        return prod.Change->Scheme;
    }

    void Upsert(ui64 key, const TString& value) {
        const TRawTypeValue keyValue(&key, sizeof(key), NScheme::NTypeIds::Uint64);
        const NTable::TUpdateOp op(2, NTable::ECellOp::Set,
            TRawTypeValue(value.data(), value.size(), NScheme::NTypeIds::String));
        Db.Update(TableId, NTable::ERowOp::Upsert, {&keyValue, 1}, {&op, 1});
    }

    void Erase(ui64 key) {
        const TRawTypeValue keyValue(&key, sizeof(key), NScheme::NTypeIds::Uint64);
        Db.Update(TableId, NTable::ERowOp::Erase, {&keyValue, 1}, {});
    }

    template<class TFunc>
    TString MakeRedo(ui32 gen, ui32 step, TFunc&& func) {
        Db.Begin(NTable::TTxStamp(gen, step), Env);
        func();
        auto prod = Db.Commit(NTable::TTxStamp(gen, step), true);
        return prod.Change->Redo;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TPDiskToolBlobStore) {
    Y_UNIT_TEST(ParseExportedName) {
        TLogoBlobID id;
        ui32 partId = 0;

        UNIT_ASSERT(ParseExportedBlobName("[1:2:3:4:5:6:0].part2", id, partId));
        UNIT_ASSERT_VALUES_EQUAL(id.ToString(), "[1:2:3:4:5:6:0]");
        UNIT_ASSERT_VALUES_EQUAL(partId, 2u);

        UNIT_ASSERT(ParseExportedBlobName("[1:2:3:4:5:6:0].part1.copy3", id, partId));
        UNIT_ASSERT_VALUES_EQUAL(partId, 1u);

        // A part id of zero means the whole blob, which is never a file name, and everything the
        // exporter does not write is skipped rather than guessed at.
        UNIT_ASSERT(!ParseExportedBlobName("[1:2:3:4:5:6:0].part0", id, partId));
        UNIT_ASSERT(!ParseExportedBlobName("[1:2:3:4:5:6:0]", id, partId));
        UNIT_ASSERT(!ParseExportedBlobName("[1:2:3:4:5:6:0].part1.foo", id, partId));
        UNIT_ASSERT(!ParseExportedBlobName("blobs.txt", id, partId));
    }

    Y_UNIT_TEST(RestoreFromEveryMinimalSubset) {
        const TErasureType erasure(TErasureType::Erasure4Plus2Block);
        const TString data = TString(3000, 'x') + TString(1000, 'y');
        const TLogoBlobID id(Tablet, 1, 1, 1, data.size(), 0);

        TDataPartSet split;
        erasure.SplitData(TErasureType::CrcModeNone, data, split);
        UNIT_ASSERT_VALUES_EQUAL(split.Parts.size(), erasure.TotalPartCount());

        TVector<TString> parts;
        for (const auto& part : split.Parts) {
            parts.push_back(part.OwnedString.ConvertToString());
        }

        const ui32 total = erasure.TotalPartCount();
        const ui32 minimal = erasure.MinimalRestorablePartCount();
        UNIT_ASSERT_VALUES_EQUAL(minimal, 4u);

        for (ui32 mask = 0; mask < (1u << total); ++mask) {
            if (ui32(std::popcount(mask)) != minimal) {
                continue;
            }
            TExport dir;
            for (ui32 part = 0; part < total; ++part) {
                if (mask & (1u << part)) {
                    dir.PutPart(id, part + 1, parts[part]);
                }
            }

            TIssueLog issues;
            TBlobStore store(erasure, issues);
            UNIT_ASSERT(store.AddDirectory(dir.Path()));
            UNIT_ASSERT_C(store.CanRestore(id), "mask " << mask);
            const TString* body = store.Get(id);
            UNIT_ASSERT_C(body, "mask " << mask);
            UNIT_ASSERT_VALUES_EQUAL_C(*body, data, "mask " << mask);
        }
    }

    Y_UNIT_TEST(TooFewPartsIsUnrecoverable) {
        const TErasureType erasure(TErasureType::Erasure4Plus2Block);
        const TString data(4096, 'z');
        const TLogoBlobID id(Tablet, 1, 1, 1, data.size(), 0);

        TDataPartSet split;
        erasure.SplitData(TErasureType::CrcModeNone, data, split);

        TExport dir;
        for (ui32 part = 0; part < 3; ++part) {
            dir.PutPart(id, part + 1, split.Parts[part].OwnedString.ConvertToString());
        }

        TIssueLog issues;
        TBlobStore store(erasure, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));
        UNIT_ASSERT(!store.CanRestore(id));
        UNIT_ASSERT(!store.Get(id));
        UNIT_ASSERT_VALUES_EQUAL(store.Stats().Unrecoverable, 1u);
        UNIT_ASSERT_VALUES_EQUAL(store.Unrecoverable().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(store.Unrecoverable()[0], id);
    }

    Y_UNIT_TEST(PartOfWrongSizeIsIgnored) {
        const TErasureType erasure(TErasureType::Erasure4Plus2Block);
        const TString data(4096, 'z');
        const TLogoBlobID id(Tablet, 1, 1, 1, data.size(), 0);

        TDataPartSet split;
        erasure.SplitData(TErasureType::CrcModeNone, data, split);

        TExport dir;
        for (ui32 part = 0; part < 4; ++part) {
            TString body = split.Parts[part].OwnedString.ConvertToString();
            if (part == 3) {
                body.resize(body.size() / 2); // a truncated part must not reach the erasure code
            }
            dir.PutPart(id, part + 1, body);
        }
        dir.PutPart(id, 5, split.Parts[4].OwnedString.ConvertToString());

        TIssueLog issues;
        TBlobStore store(erasure, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));
        const TString* body = store.Get(id);
        UNIT_ASSERT(body);
        UNIT_ASSERT_VALUES_EQUAL(*body, data);
        UNIT_ASSERT_VALUES_EQUAL(store.Stats().WrongSizeParts, 1u);
    }

    Y_UNIT_TEST(DisagreeingCopiesAreReported) {
        const TString first(64, 'a');
        TString second(64, 'a');
        second[10] = 'b';

        TExport dir;
        const TLogoBlobID id(Tablet, 1, 1, 1, first.size(), 0);
        dir.PutPart(id, 1, first, 1);
        dir.PutPart(id, 1, second, 2);

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));
        UNIT_ASSERT_VALUES_EQUAL(store.Stats().DisagreeingParts, 1u);

        const TString* body = store.Get(id);
        UNIT_ASSERT(body);
        // Which one wins is up to the order the directory lists its files in, but it is one of them.
        UNIT_ASSERT(*body == first || *body == second);
    }

    Y_UNIT_TEST(LargeGlobIdSpansSeveralBlobs) {
        const TString body = TString(100, 'a') + TString(100, 'b') + TString(40, 'c');

        TExport dir;
        const TLogoBlobID lead(Tablet, 1, 1, 1, 100, CookieFor(EIdx::Pack, 0));
        dir.PutPart(lead, 1, body.substr(0, 100));
        dir.PutPart(TLogoBlobID(Tablet, 1, 1, 1, 100, CookieFor(EIdx::Pack, 1)), 1, body.substr(100, 100));
        dir.PutPart(TLogoBlobID(Tablet, 1, 1, 1, 40, CookieFor(EIdx::Pack, 2)), 1, body.substr(200));

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));

        const NPageCollection::TLargeGlobId largeGlobId(0, lead, body.size());
        TString out;
        UNIT_ASSERT(store.Get(largeGlobId, out));
        UNIT_ASSERT_VALUES_EQUAL(out, body);

        // The last blob of the run is missing, so the body cannot be handed out at all.
        TExport partial;
        partial.PutPart(lead, 1, body.substr(0, 100));
        partial.PutPart(TLogoBlobID(Tablet, 1, 1, 1, 100, CookieFor(EIdx::Pack, 1)), 1,
            body.substr(100, 100));

        TBlobStore incomplete({}, issues);
        UNIT_ASSERT(incomplete.AddDirectory(partial.Path()));
        UNIT_ASSERT(!incomplete.Get(largeGlobId, out));
    }
}

Y_UNIT_TEST_SUITE(TPDiskToolTabletLog) {
    Y_UNIT_TEST(ConfirmedChainAndTail) {
        TExport dir;
        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body("one").Serialize());
        dir.PutWhole(1, 3, 0, 0, TLogEntryBuilder(1, 1).Confirmed(2).Body("two").Serialize());

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));

        const auto history = RebuildTabletHistory(store, Tablet, 0, issues);
        UNIT_ASSERT(history.Ok);
        UNIT_ASSERT_VALUES_EQUAL(history.KeyEntry.Generation(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(history.KeyEntry.Step(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(history.Snapshot.first, 1u);
        UNIT_ASSERT_VALUES_EQUAL(history.Snapshot.second, 1u);
        UNIT_ASSERT_VALUES_EQUAL(history.Confirmed.second, 2u);
        UNIT_ASSERT(!history.Stats.GapsTolerated);

        const auto& entries = history.Graph->Entries;
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 3u);
        UNIT_ASSERT(entries[0].IsSnapshot);
        UNIT_ASSERT_VALUES_EQUAL(entries[1].EmbeddedLogBody, "one");
        UNIT_ASSERT_VALUES_EQUAL(entries[2].EmbeddedLogBody, "two");
    }

    Y_UNIT_TEST(TailDeclinedByZeroEntry) {
        TExport dir;
        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body("kept").Serialize());
        dir.PutWhole(1, 3, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body("lost").Serialize());
        // The new generation confirms 1:2 and declines the one step after it.
        dir.PutWhole(2, 0, 0, 0, ZeroEntry(1, 1, 1, 2, {false}));
        dir.PutWhole(2, 1, 0, 0, TLogEntryBuilder(1, 1).Confirmed(0).Body("next").Serialize());

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));

        const auto history = RebuildTabletHistory(store, Tablet, 0, issues);
        UNIT_ASSERT(history.Ok);
        UNIT_ASSERT(!history.Stats.GapsTolerated);
        UNIT_ASSERT_VALUES_EQUAL(history.Stats.ZeroEntries, 1u);
        UNIT_ASSERT_VALUES_EQUAL(history.Stats.DeclinedEntries, 1u);

        const auto& entries = history.Graph->Entries;
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(entries[1].EmbeddedLogBody, "kept");
        UNIT_ASSERT_VALUES_EQUAL(entries[2].EmbeddedLogBody, "next");
    }

    Y_UNIT_TEST(TailDeclinedByMissingReference) {
        const TLogoBlobID missing(Tablet, 1, 2, 0, 100, CookieFor(EIdx::RedoLz4));

        TExport dir;
        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Ref(missing).Serialize());

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));

        const auto history = RebuildTabletHistory(store, Tablet, 0, issues);
        UNIT_ASSERT(history.Ok);
        UNIT_ASSERT_VALUES_EQUAL(history.Stats.MissingReferences, 1u);
        UNIT_ASSERT_VALUES_EQUAL(history.Stats.DeclinedEntries, 1u);
        UNIT_ASSERT_VALUES_EQUAL(history.Graph->Entries.size(), 1u);
    }

    Y_UNIT_TEST(GarbageCandidateIsSkipped) {
        TExport dir;
        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body("one").Serialize());
        // A redo body sits on channel 0 too and sorts above the log entry of its own step, so it is
        // tried as a key entry first and has to be rejected on its own.
        dir.PutWhole(1, 2, 0, CookieFor(EIdx::RedoLz4), TString(40, '\xff'));

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));

        const auto history = RebuildTabletHistory(store, Tablet, 0, issues);
        UNIT_ASSERT(history.Ok);
        UNIT_ASSERT_VALUES_EQUAL(history.KeyEntry.Step(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(history.KeyEntry.Cookie(), 0u);
        UNIT_ASSERT(history.Stats.CandidatesTried > 1);
    }

    Y_UNIT_TEST(NoLogEntryAtAll) {
        TExport dir;
        dir.PutWhole(1, 1, 1, 0, "not a log entry");

        TIssueLog issues;
        TBlobStore store({}, issues);
        UNIT_ASSERT(store.AddDirectory(dir.Path()));

        const auto history = RebuildTabletHistory(store, Tablet, 0, issues);
        UNIT_ASSERT(!history.Ok);
        UNIT_ASSERT(issues.HasErrors());
    }
}

Y_UNIT_TEST_SUITE(TPDiskToolRestoreTablet) {
    Y_UNIT_TEST(QuotingKeepsFilesParseable) {
        UNIT_ASSERT_VALUES_EQUAL(QuoteField("plain", '\t'), "plain");
        UNIT_ASSERT_VALUES_EQUAL(QuoteField("a\tb", '\t'), "\"a\tb\"");
        UNIT_ASSERT_VALUES_EQUAL(QuoteField("a\tb", ','), "\"a\tb\"");
        UNIT_ASSERT_VALUES_EQUAL(QuoteField("a,b", '\t'), "a,b");
        UNIT_ASSERT_VALUES_EQUAL(QuoteField("a\"b", '\t'), "\"a\"\"b\"");
        UNIT_ASSERT_VALUES_EQUAL(QuoteField("a\nb", ','), "\"a\nb\"");
    }

    // A log of an executor snapshot naming a scheme blob, plus two commits carried in the log entries:
    // the second one adds a row and erases one of the first one's.
    TExport MakeRedoLog() {
        TSourceDb source;
        const TString scheme = source.MakeScheme(1, 1);
        const TString first = source.MakeRedo(1, 2, [&]() {
            source.Upsert(1, "one");
            source.Upsert(2, "two");
        });
        const TString second = source.MakeRedo(1, 3, [&]() {
            source.Upsert(3, "three");
            source.Erase(2);
        });
        UNIT_ASSERT(scheme);
        UNIT_ASSERT(first);
        UNIT_ASSERT(second);

        TExport dir;
        const auto alter = dir.PutWhole(1, 1, 0, CookieFor(EIdx::Alter), scheme);

        NKikimrExecutorFlat::TLogSnapshot snap;
        snap.SetSerial(1);
        PutLogoBlobId(snap.AddSchemeInfoBodies(), alter);
        const auto snapId = dir.PutWhole(1, 1, 0, CookieFor(EIdx::SnapLz4),
            Pack(snap.SerializeAsString()));

        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Ref(snapId).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body(Pack(first)).Serialize());
        dir.PutWhole(1, 3, 0, 0, TLogEntryBuilder(1, 1).Confirmed(2).Body(Pack(second)).Serialize());
        return dir;
    }

    Y_UNIT_TEST(RedoOnlyEndToEnd) {
        const TExport dir = MakeRedoLog();

        TRestored restored;
        Restore(dir.Path(), restored);
        UNIT_ASSERT_C(restored.Ok, Complaints(restored.Issues));
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Tables.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Tables[0].Name, "data");
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Tables[0].Rows, 2u);
        UNIT_ASSERT(restored.Dump.Tables[0].Complete);
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Incomplete, 0u);

        const auto lines = restored.Lines(TableId, "data");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(lines[0], "key\tvalue");
        UNIT_ASSERT_VALUES_EQUAL(lines[1], "1\tone");
        UNIT_ASSERT_VALUES_EQUAL(lines[2], "3\tthree");

        // The description names the tables and their columns, so the files can be interpreted.
        const TString description = TFileInput(restored.Dump.Description).ReadAll();
        UNIT_ASSERT(description.Contains("table 1 data"));
        UNIT_ASSERT(description.Contains("column 0 id 1 key Uint64 key 0"));
        UNIT_ASSERT(description.Contains("column 1 id 2 value String"));
    }

    Y_UNIT_TEST(CsvWithErasedRows) {
        const TExport dir = MakeRedoLog();

        TRestored restored;
        restored.Options.Csv = true;
        restored.Options.IncludeErased = true;
        Restore(dir.Path(), restored);
        UNIT_ASSERT_C(restored.Ok, Complaints(restored.Issues));
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Tables[0].File, "1_data.csv");
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Tables[0].Rows, 2u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Tables[0].Erased, 1u);

        const auto lines = restored.Lines(TableId, "data");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(lines[0], "op,key,value");
        UNIT_ASSERT_VALUES_EQUAL(lines[1], "upsert,1,one");
        UNIT_ASSERT_VALUES_EQUAL(lines[2], "erase,2,");
        UNIT_ASSERT_VALUES_EQUAL(lines[3], "upsert,3,three");
    }

    Y_UNIT_TEST(TablesOnlyWritesNoData) {
        const TExport dir = MakeRedoLog();

        TRestored restored;
        restored.Options.TablesOnly = true;
        Restore(dir.Path(), restored);
        UNIT_ASSERT(restored.Ok);
        UNIT_ASSERT(restored.Dump.Tables.empty());
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Rows, 0u);

        const TString description = TFileInput(restored.Dump.Description).ReadAll();
        UNIT_ASSERT(description.Contains("table 1 data"));
        UNIT_ASSERT(!TFsPath(restored.Output.Path() / "1_data.tsv").Exists());
    }

    // The same log with the last commit body left out of the input: what came before it is still there.
    Y_UNIT_TEST(MissingRedoBodyIsSkipped) {
        TSourceDb source;
        const TString scheme = source.MakeScheme(1, 1);
        const TString first = source.MakeRedo(1, 2, [&]() { source.Upsert(1, "one"); });
        const TString second = source.MakeRedo(1, 3, [&]() { source.Upsert(2, "two"); });

        TExport dir;
        const auto alter = dir.PutWhole(1, 1, 0, CookieFor(EIdx::Alter), scheme);

        NKikimrExecutorFlat::TLogSnapshot snap;
        snap.SetSerial(1);
        PutLogoBlobId(snap.AddSchemeInfoBodies(), alter);
        const auto snapId = dir.PutWhole(1, 1, 0, CookieFor(EIdx::SnapLz4),
            Pack(snap.SerializeAsString()));

        // The second commit is referenced instead of embedded, and its body is not in the input.
        const TString packed = Pack(second);
        const TLogoBlobID redo(Tablet, 1, 3, 0, packed.size(), CookieFor(EIdx::RedoLz4));

        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Ref(snapId).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body(Pack(first)).Serialize());
        dir.PutWhole(1, 3, 0, 0, TLogEntryBuilder(1, 1).Confirmed(3).Ref(redo).Serialize());

        TRestored restored;
        Restore(dir.Path(), restored);
        UNIT_ASSERT(restored.Ok);

        const auto lines = restored.Lines(TableId, "data");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(lines[1], "1\tone");
    }

    // A real page-collection backed part, written the way compaction writes one, referenced from the
    // executor snapshot: this exercises the loader and the offline page reads.
    struct TWrittenPart {
        TVector<NPageCollection::TGlob> Blobs;
        TVector<NPageCollection::TLargeGlobId> PageCollections;
        TString Opaque;
    };

    TWrittenPart WritePart(TIntrusiveConstPtr<NTable::TRowScheme> rowScheme,
            const TVector<std::pair<ui64, TString>>& rows)
    {
        NTabletFlatExecutor::NWriter::TConf conf;
        conf.Groups[0].Channel = 1;
        conf.Groups[0].MaxBlobSize = 4096;
        conf.BlobsChannels = {1};
        conf.OuterChannel = 1;
        conf.ExtraChannel = 1;
        conf.ChannelsShares = NUtil::TChannelsShares({{1, 1.0f}});
        conf.Slots = {{ui8(1), ui32(0)}};

        const TLogoBlobID base(Tablet, 1, 1, 0, 0, 0);
        NTabletFlatExecutor::NWriter::TBundle bundle(base, conf);

        NTable::NPage::TConf pageConf{true, 4096};
        const auto tags = rowScheme->Tags();
        NTable::TPartWriter writer(new NTable::TPartScheme(rowScheme->Cols), tags, bundle, pageConf,
            NTable::TEpoch::Zero());

        for (const auto& [key, value] : rows) {
            const TCell keyCell(reinterpret_cast<const char*>(&key), sizeof(key));
            NTable::TRowState row(tags.size());
            row.Touch(NTable::ERowOp::Upsert);
            row.Set(1, NTable::ECellOp::Set, TCell(value.data(), value.size()));
            writer.BeginKey({&keyCell, 1});
            writer.AddKeyVersion(row, TRowVersion::Min());
            writer.EndKey();
        }
        const auto written = writer.Finish();
        UNIT_ASSERT_VALUES_EQUAL(written.Parts, 1u);

        TWrittenPart out;
        out.Blobs = bundle.GetBlobsToSave();
        auto results = bundle.Results();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        for (const auto& one : results[0].PageCollections) {
            const auto* pack = CheckedCast<const NPageCollection::TPageCollection*>(
                one.PageCollection.Get());
            out.PageCollections.push_back(pack->LargeGlobId);
        }
        out.Opaque = results[0].Overlay;
        return out;
    }

    void FillBundle(NKikimrExecutorFlat::TLogTableSnap* snap, const TWrittenPart& part) {
        snap->SetTable(TableId);
        snap->SetCompactionLevel(255);
        auto* bundle = snap->AddBundles();
        for (const auto& largeGlobId : part.PageCollections) {
            NTabletFlatExecutor::TLargeGlobIdProto::Put(
                *bundle->AddPageCollections()->MutableLargeGlobId(), largeGlobId);
        }
        if (part.Opaque) {
            bundle->SetOpaque(part.Opaque);
        }
        bundle->SetEpoch(NTable::TEpoch::Zero().ToProto());
    }

    Y_UNIT_TEST(PartEndToEnd) {
        TSourceDb source;
        const TString scheme = source.MakeScheme(1, 1);
        const auto rowScheme = source.Db.GetRowScheme(TableId);

        const TVector<std::pair<ui64, TString>> rows{{1, "one"}, {2, "two"}, {3, "three"}};
        const auto part = WritePart(rowScheme, rows);
        UNIT_ASSERT(part.Blobs);
        UNIT_ASSERT(part.PageCollections);

        TExport dir;
        for (const auto& glob : part.Blobs) {
            dir.PutGlob(glob.GId, glob.Data);
        }
        const auto alter = dir.PutWhole(1, 1, 0, CookieFor(EIdx::Alter), scheme);

        NKikimrExecutorFlat::TLogSnapshot snap;
        snap.SetSerial(1);
        PutLogoBlobId(snap.AddSchemeInfoBodies(), alter);
        FillBundle(snap.AddDbParts(), part);
        const auto snapId = dir.PutWhole(1, 2, 0, CookieFor(EIdx::SnapLz4),
            Pack(snap.SerializeAsString()));

        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 2).Snapshot().Confirmed(0).Ref(snapId).Serialize());

        TRestored restored;
        Restore(dir.Path(), restored);
        UNIT_ASSERT_C(restored.Ok, Complaints(restored.Issues));

        const auto lines = restored.Lines(TableId, "data");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1u + rows.size());
        UNIT_ASSERT_VALUES_EQUAL(lines[0], "key\tvalue");
        UNIT_ASSERT_VALUES_EQUAL(lines[1], "1\tone");
        UNIT_ASSERT_VALUES_EQUAL(lines[2], "2\ttwo");
        UNIT_ASSERT_VALUES_EQUAL(lines[3], "3\tthree");
    }

    // The same part with its data blobs withheld: the part is dropped, the run reports it and the
    // remaining tables are still written.
    Y_UNIT_TEST(PartWithHeldBackBlobs) {
        TSourceDb source;
        const TString scheme = source.MakeScheme(1, 1);
        const auto rowScheme = source.Db.GetRowScheme(TableId);

        const TVector<std::pair<ui64, TString>> rows{{1, "one"}, {2, "two"}};
        const auto part = WritePart(rowScheme, rows);

        TExport dir;
        // Only the page collection meta blobs are kept, so the loader cannot read the part.
        THashSet<TLogoBlobID> meta;
        for (const auto& largeGlobId : part.PageCollections) {
            for (const auto& id : largeGlobId.Blobs()) {
                meta.insert(id);
            }
        }
        for (const auto& glob : part.Blobs) {
            if (meta.contains(glob.GId.Logo)) {
                dir.PutGlob(glob.GId, glob.Data);
            }
        }
        const auto alter = dir.PutWhole(1, 1, 0, CookieFor(EIdx::Alter), scheme);

        NKikimrExecutorFlat::TLogSnapshot snap;
        snap.SetSerial(1);
        PutLogoBlobId(snap.AddSchemeInfoBodies(), alter);
        FillBundle(snap.AddDbParts(), part);
        const auto snapId = dir.PutWhole(1, 2, 0, CookieFor(EIdx::SnapLz4),
            Pack(snap.SerializeAsString()));

        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 2).Snapshot().Confirmed(0).Ref(snapId).Serialize());

        TRestored restored;
        Restore(dir.Path(), restored);
        UNIT_ASSERT(restored.Ok);

        // The scheme is known, so the table is there, only without the rows of the dropped part.
        const auto lines = restored.Lines(TableId, "data");
        UNIT_ASSERT_VALUES_EQUAL(lines.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Dump.Rows, 0u);
    }

    Y_UNIT_TEST(NoSchemeInTheInput) {
        TSourceDb source;
        source.MakeScheme(1, 1);
        const TString redo = source.MakeRedo(1, 2, [&]() { source.Upsert(1, "one"); });

        TExport dir;
        // The scheme blob is referenced by the snapshot but is not in the input.
        const TLogoBlobID alter(Tablet, 1, 1, 0, 100, CookieFor(EIdx::Alter));

        NKikimrExecutorFlat::TLogSnapshot snap;
        snap.SetSerial(1);
        PutLogoBlobId(snap.AddSchemeInfoBodies(), alter);
        const auto snapId = dir.PutWhole(1, 1, 0, CookieFor(EIdx::SnapLz4),
            Pack(snap.SerializeAsString()));

        dir.PutWhole(1, 1, 0, 0, TLogEntryBuilder(1, 1).Snapshot().Confirmed(0).Ref(snapId).Serialize());
        dir.PutWhole(1, 2, 0, 0, TLogEntryBuilder(1, 1).Confirmed(1).Body(Pack(redo)).Serialize());

        TRestored restored;
        Restore(dir.Path(), restored);
        UNIT_ASSERT(!restored.Ok);
        UNIT_ASSERT(restored.Issues.HasErrors());
    }
}

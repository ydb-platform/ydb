#include <ydb/core/tablet_flat/util_fmt_desc.h>
#include <ydb/core/tablet_flat/util_fmt_flat.h>
#include <ydb/core/tablet_flat/flat_redo_player.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/tablet_flat/flat_part_overlay.h>
#include <ydb/core/protos/scheme_log.pb.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/util/pb.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/blockcodecs/codecs.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <array>

namespace NKikimr {
namespace NTable {
namespace NTest {

    struct TNames {

        static const char* Do(ERowOp rop)
        {
            static const std::array<const char*, 6> names { {
                "Absent",
                "Upsert",
                "Erase",
                "Reset",
                "Update",
                "Insert"
            }};

            return ui8(rop) < names.size() ? names[ui8(rop)] : "?error";
        }
    };

    class TDumpValue {
    public:
        TDumpValue(const TRawTypeValue& value)
            : Value(value)
        { }

        friend IOutputStream& operator<<(IOutputStream& out, const TDumpValue& v) {
            if (!v.Value.Data()) {
                out << "NULL";
                return out;
            }
            TCell cell = { static_cast<const char*>(v.Value.Data()), v.Value.Size() };
            switch (v.Value.Type()) {
                case NScheme::NTypeIds::Uint8: out << cell.AsValue<ui8>(); break;
                case NScheme::NTypeIds::Uint16: out << cell.AsValue<ui16>(); break;
                case NScheme::NTypeIds::Uint32: out << cell.AsValue<ui32>(); break;
                case NScheme::NTypeIds::Uint64: out << cell.AsValue<ui64>(); break;
                case NScheme::NTypeIds::Int8: out << cell.AsValue<i8>(); break;
                case NScheme::NTypeIds::Int16: out << cell.AsValue<i16>(); break;
                case NScheme::NTypeIds::Int32: out << cell.AsValue<i32>(); break;
                case NScheme::NTypeIds::Int64: out << cell.AsValue<i64>(); break;
                case NScheme::NTypeIds::Float: out << cell.AsValue<float>(); break;
                case NScheme::NTypeIds::Double: out << cell.AsValue<double>(); break;
                default: out << v.Value; break;
            }
            return out;
        }

    private:
        const TRawTypeValue& Value;
    };

    struct TDump {
        using TKeys = TArrayRef<const TRawTypeValue>;
        using TOps = TArrayRef<const TUpdateOp>;

        TDump(IOutputStream &out) : Out(out) { }

        bool NeedIn(ui32) noexcept
        {
            return true;
        }

        void DoBegin(ui32 tail, ui32 head, ui64 serial, ui64 stamp) noexcept
        {
            Out
                << " +Begin ABI[" << tail << ", " << head << "]"
                << " {" << serial << " " << NFmt::TStamp(stamp) << "}"
                << Endl;
        }

        void DoAnnex(TArrayRef<const TStdPad<NPageCollection::TGlobId>> annex) noexcept
        {
            Out << " | Annex " << annex.size() << " items" << Endl;

            for (auto &one: annex)
                Out << " | " << (*one).Group << " : " << (*one).Logo << Endl;
        }

        void DoUpdate(ui32 tid, ERowOp rop, TKeys key, TOps ops, TRowVersion rowVersion) noexcept
        {
            ui32 keyBytes = 0, opsBytes = 0;

            for (auto &one: key) keyBytes += one.Size();
            for (auto &one: ops) opsBytes += one.AsRef().size();

            Updates++, KeyBytes += keyBytes, OpsBytes += opsBytes;
            KeyItems += key.size(), OpsItems += ops.size();

            Out
                << " | Update " << tid
                << ": " << TNames::Do(rop) << "." << unsigned(rop)
                << ", " << key.size() << " keys (" << keyBytes << "b)"
                << " " << ops.size() << " ops (" << opsBytes << "b)"
                << " " << rowVersion
                << Endl;

            for (const auto& keyValue : key) {
                Out << "   > key: " << TDumpValue(keyValue) << " <" << Endl;
            }
            for (const auto& op : ops) {
                Out << "   > col " << op.Tag << ": " << TDumpValue(op.Value) << " <" << Endl;
            }
        }

        void DoUpdateTx(ui32 tid, ERowOp rop, TKeys key, TOps ops, ui64 txId) noexcept
        {
            ui32 keyBytes = 0, opsBytes = 0;

            for (auto &one: key) keyBytes += one.Size();
            for (auto &one: ops) opsBytes += one.AsRef().size();

            Updates++, KeyBytes += keyBytes, OpsBytes += opsBytes;
            KeyItems += key.size(), OpsItems += ops.size();

            Out
                << " | UpdateTx " << tid
                << ": " << TNames::Do(rop) << "." << unsigned(rop)
                << ", " << key.size() << " keys (" << keyBytes << "b)"
                << " " << ops.size() << " ops (" << opsBytes << "b)"
                << " txId " << txId
                << Endl;

            for (const auto& keyValue : key) {
                Out << "   > key: " << TDumpValue(keyValue) << " <" << Endl;
            }
            for (const auto& op : ops) {
                Out << "   > col " << op.Tag << ": " << TDumpValue(op.Value) << " <" << Endl;
            }
        }

        void DoCommitTx(ui32 tid, ui64 txId, TRowVersion rowVersion) noexcept
        {
            Out << " | CommitTx " << tid << " txId " << txId << " @" << rowVersion << Endl;
        }

        void DoRemoveTx(ui32 tid, ui64 txId) noexcept
        {
            Out << " | RemoveTx " << tid << " txId " << txId << Endl;
        }

        void DoFlush(ui32 tid, ui64 stamp, TEpoch epoch) noexcept
        {
            Out
                << " | Flush " << tid << ", " << epoch << " eph"
                << ", stamp " << NFmt::TStamp(stamp) << Endl;
        }

        void Finalize() const noexcept
        {
            Out
                << " ' Stats: " << Updates << " ups"
                << ", keys " << KeyItems << " " << KeyBytes << "b"
                << ", ops " << OpsItems << " " << OpsBytes << "b"
                << Endl;
        }

    private:
        IOutputStream &Out;

        ui64 Updates = 0;
        ui64 KeyItems = 0;
        ui64 KeyBytes = 0;
        ui64 OpsItems = 0;
        ui64 OpsBytes = 0;
    };

    template<typename TProto>
    struct TProtoDump {
        TProtoDump(IOutputStream &out) : Out(out) { }

        void Do(TArrayRef<const char> plain)
        {
            TProto proto;

            if (!ParseFromStringNoSizeLimit(proto, plain)) {
                throw yexception() << "Failed to parse proto blob";
            } else {
                Out << proto.DebugString();
            }
        }

    private:
        IOutputStream &Out;
    };

    void DumpSnapshot(const NKikimrExecutorFlat::TLogMemSnap& snap) {
        Cerr << " | Table " << snap.GetTable() << " snapshot " << snap.GetGeneration() << ":" << snap.GetStep()
             << " epoch " << snap.GetHead() << Endl;
    }

    void DumpPart(const NKikimrExecutorFlat::TLogTableSnap& part) {
        Cerr << " | Table " << part.GetTable() << " level " << part.GetCompactionLevel() << Endl;
        for (auto& bundle : part.GetBundles()) {
            Cerr << " | + bundle";
            for (auto& pageCollection : bundle.GetPageCollections()) {
                Y_ABORT_UNLESS(pageCollection.HasLargeGlobId(), "Found a page collection without a largeGlobId");
                Cerr << " " << LogoBlobIDFromLogoBlobID(pageCollection.GetLargeGlobId().GetLead());
            }
            if (bundle.HasEpoch()) {
                Cerr << " epoch " << bundle.GetEpoch();
            }
            Cerr << Endl;
            if (bundle.HasLegacy() || bundle.HasOpaque()) {
                auto overlay = NKikimr::NTable::TOverlay::Decode(bundle.GetLegacy(), bundle.GetOpaque());
                if (overlay.Screen) {
                    for (auto& hole : *overlay.Screen) {
                        Cerr << " |   screen [" << hole.Begin << ", " << hole.End << ")" << Endl;
                    }
                }
                for (auto& slice : *overlay.Slices) {
                    Cerr << " |   slice ";
                    Cerr << (slice.FirstInclusive ? '[' : '(');
                    Cerr << "row " << slice.FirstRowId;
                    for (auto cell : slice.FirstKey.GetCells()) {
                        Cerr << " cell " << TString{cell.AsBuf()}.Quote();
                    }
                    Cerr << ", ";
                    Cerr << "row " << slice.LastRowId;
                    for (auto cell : slice.LastKey.GetCells()) {
                        Cerr << " cell " << TString{cell.AsBuf()}.Quote();
                    }
                    Cerr << (slice.LastInclusive ? ']' : ')');
                    Cerr << Endl;
                }
            }
        }
    }

    void DumpPart(const NKikimrExecutorFlat::TLogTxStatusSnap& part) {
        Cerr << " | Table " << part.GetTable() << Endl;
        for (auto& txStatus : part.GetTxStatus()) {
            Cerr << " | + tx status " << LogoBlobIDFromLogoBlobID(txStatus.GetDataId().GetLead())
                 << " epoch " << txStatus.GetEpoch() << Endl;
        }
    }

}
}
}

int main(int argc, char *argv[])
{
    using namespace NLastGetopt;
    using namespace NKikimr;
    using namespace NKikimr::NTable;

    TOpts opts;

    opts.AddLongOption("path", "path to blob")
        .RequiredArgument("FILE").Required();
    opts.AddLongOption("blob", "type of blob to read")
        .RequiredArgument("STR").Required();
    opts.AddLongOption("codec", "blob encoding: lz4, plain")
        .RequiredArgument("STR").DefaultValue("plain");

    THolder<TOptsParseResult> res(new TOptsParseResult(&opts, argc, argv));

    auto raw = TFileInput(res->Get("path")).ReadAll();

    if (!res->Has("codec") || !strcmp(res->Get("codec"), "plain")) {
        /* Blob is already decoded, nothing to do */
    } else if (!strcmp(res->Get("codec"), "lz4")) {
        raw = NBlockCodecs::Codec("lz4fast")->Decode(raw);
    } else {
        throw yexception() << "Unknown blob encoding " << res->Get("codec");
    }

    Cerr << " decoded blob has " << raw.size() << " bytes" << Endl;

    if (!strcmp(res->Get("blob"), "redo")) {
        NTest::TDump dump(Cerr);

        NRedo::TPlayer<NTest::TDump>(dump).Replay(raw);

        dump.Finalize();

    } else if (!strcmp(res->Get("blob"), "alter")) {
        NTest::TProtoDump<NTabletFlatScheme::TSchemeChanges>(Cerr).Do(raw);
    } else if (!strcmp(res->Get("blob"), "snap")) {
        NTest::TProtoDump<NKikimrExecutorFlat::TLogSnapshot>(Cerr).Do(raw);

        TProtoBox<NKikimrExecutorFlat::TLogSnapshot> proto(raw);

        for (const auto &ref : proto.GetNonSnapLogBodies()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | NonSnapLog " << logo << Endl;
        }

        for (const auto &ref : proto.GetSchemeInfoBodies()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | SchemeInfo " << logo << Endl;
        }

        for (const auto &ref : proto.GetBorrowInfoIds()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | BorrowInfo " << logo << Endl;
        }

        for (const auto& ref : proto.GetGcSnapDiscovered()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | GC+ " << logo << Endl;
        }

        for (const auto& ref : proto.GetGcSnapLeft()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | GC- " << logo << Endl;
        }

        for (const auto& part : proto.GetDbParts()) {
            NTest::DumpPart(part);
        }

        for (const auto& part : proto.GetTxStatusParts()) {
            NTest::DumpPart(part);
        }

        for (const auto& log : proto.GetEmbeddedLogBodies()) {
            Cerr << " | >>>>>>>> Log entry " << log.GetGeneration() << ":" << log.GetStep() << Endl;
            Cerr << " --------------------" << Endl;
            auto logBody = NBlockCodecs::Codec("lz4fast")->Decode(log.GetBody());
            NTest::TDump dump(Cerr);
            NRedo::TPlayer<NTest::TDump>(dump).Replay(logBody);
            dump.Finalize();
            Cerr << " --------------------" << Endl;
        }

    } else if (!strcmp(res->Get("blob"), "switch")) {
        NTest::TProtoDump<NKikimrExecutorFlat::TTablePartSwitch>(Cerr).Do(raw);

        TProtoBox<NKikimrExecutorFlat::TTablePartSwitch> proto(raw);
        if (proto.HasTableSnapshoted()) {
            NTest::DumpSnapshot(proto.GetTableSnapshoted());
        }
        if (proto.HasIntroducedParts()) {
            NTest::DumpPart(proto.GetIntroducedParts());
        }
        for (const auto& label : proto.GetLeavingBundles()) {
            Cerr << " | - bundle " << LogoBlobIDFromLogoBlobID(label) << Endl;
        }

    } else if (!strcmp(res->Get("blob"), "borrow")) {
        NTest::TProtoDump<NKikimrExecutorFlat::TBorrowedPart>(Cerr).Do(raw);
    } else if (!strcmp(res->Get("blob"), "tablet")) {
        NTest::TProtoDump<NKikimrTabletBase::TTabletLogEntry>(Cerr).Do(raw);

        TProtoBox<NKikimrTabletBase::TTabletLogEntry> proto(raw);

        for (const auto &ref : proto.GetReferences()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | Ref " << logo << Endl;
        }

        for (const auto &ref : proto.GetGcDiscovered()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | GC+ " << logo << Endl;
        }

        for (const auto &ref : proto.GetGcLeft()) {
            auto logo = LogoBlobIDFromLogoBlobID(ref);

            Cerr << " | GC- " << logo << Endl;
        }

        if (auto rawBody = proto.GetEmbeddedLogBody()) {
            Cerr << " | Embedded log body" << Endl;
            auto logBody = NBlockCodecs::Codec("lz4fast")->Decode(rawBody);
            NTest::TDump dump(Cerr);
            NRedo::TPlayer<NTest::TDump>(dump).Replay(logBody);
            dump.Finalize();
        }

    } else if (!strcmp(res->Get("blob"), "unlog")) {
        TProtoBox<NKikimrTabletBase::TTabletLogEntry> proto(raw);

        if (proto.HasEmbeddedLogBody()) Cout << proto.GetEmbeddedLogBody();
    } else {
        throw yexception() << "Unknown blob type " <<  res->Get("blob");
    }
}

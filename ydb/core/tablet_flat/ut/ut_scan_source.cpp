#include <ydb/core/tablet_flat/flat_scan_feed.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <optional>
#include <utility>

namespace NKikimr::NTable {
namespace {

    struct TFixture {
        NTest::TLayoutCook Layout;
        ui32 Serial = 1;

        TFixture() {
            Layout.Col(0, 1, NScheme::NTypeIds::Uint32)
                .Col(0, 2, NScheme::NTypeIds::Uint32)
                .Col(0, 3, NScheme::NTypeIds::Uint32)
                .Key({1});
        }

        NTest::TPartCook Cook(i64 epoch) {
            NPage::TConf conf;
            conf.Final = false;
            return NTest::TPartCook(Layout, conf,
                TLogoBlobID(1, 1, Serial++, 1, 0, 0), TEpoch::FromIndex(epoch));
        }

        TIntrusivePtr<TMemTable> Memory(i64 epoch) const {
            return new TMemTable(Layout.RowScheme(), TEpoch::FromIndex(epoch), 0);
        }

        void Write(TMemTable& memory, ui32 key, ui32 value, TRowVersion version) const {
            const auto scheme = Layout.RowScheme();
            const auto row = *NTest::TSchemedCookRow(*scheme).Col(key, value, value * 10);
            const auto split = NTest::TRowTool(*scheme).Split(row, true, true);
            memory.Update(ERowOp::Upsert, split.Key, split.Ops, {}, version, nullptr);
        }

        void Lock(TMemTable& memory, ui32 key, ui64 txId) const {
            const auto scheme = Layout.RowScheme();
            const auto row = *NTest::TSchemedCookRow(*scheme).Col(key);
            const auto split = NTest::TRowTool(*scheme).Split(row, true, true);
            memory.LockRow(ELockMode::Exclusive, split.Key, txId);
        }
    };

    enum class EEvent {
        BeginKey,
        BeginDeltas,
        Delta,
        Lock,
        EndDeltas,
        Row,
        EndKey,
    };

    struct TEvent {
        EEvent Kind;
        ui32 Key;
        TVersionScanSource Source;
        TRowVersion Version = TRowVersion::Min();
        ui64 TxId = 0;
        std::optional<ui32> Value = {};
        std::optional<ui32> Base = {};
    };

    class TRecordingScan final : public IVersionScan {
    public:
        explicit TRecordingScan(const TRowScheme& scheme)
            : Tags(scheme.Tags())
        {
        }

        void SetSource(const TVersionScanSource& source) override {
            UNIT_ASSERT(KeyActive);
            UNIT_ASSERT(!HasSource);
            UNIT_ASSERT(bool(source.MemTable) != bool(source.Part));
            Source = source;
            HasSource = true;
            ++Sources;
        }

        TInitialState Prepare(IDriver*, TIntrusiveConstPtr<TScheme>) override {
            UNIT_FAIL("This test uses TFeed directly");
            return {EScan::Final, {}};
        }

        EScan Seek(TLead& lead, ui64 sequence) override {
            lead.To(Tags, {}, ESeek::Lower);
            return sequence == 0 ? EScan::Feed : EScan::Final;
        }

        EScan BeginKey(TArrayRef<const TCell> key) override {
            UNIT_ASSERT(!KeyActive);
            UNIT_ASSERT(!HasSource);
            Key = key[0].AsValue<ui32>();
            UNIT_ASSERT(!PreviousKey || *PreviousKey < Key);
            PreviousKey = Key;
            KeyActive = true;
            LastVersion = TRowVersion::Max();
            Events.push_back({EEvent::BeginKey, Key, {}});
            return EScan::Feed;
        }

        EScan BeginDeltas() override {
            UNIT_ASSERT(KeyActive);
            Events.push_back({EEvent::BeginDeltas, Key, {}});
            return EScan::Feed;
        }

        EScan Feed(const TRowState& row, ui64 txId) override {
            auto& event = Record(EEvent::Delta);
            event.TxId = txId;
            ReadValues(event, row);
            return EScan::Feed;
        }

        EScan Feed(ELockMode mode, ui64 txId) override {
            UNIT_ASSERT(mode != ELockMode::None);
            Record(EEvent::Lock).TxId = txId;
            return EScan::Feed;
        }

        EScan EndDeltas() override {
            UNIT_ASSERT(KeyActive);
            UNIT_ASSERT(!HasSource);
            Events.push_back({EEvent::EndDeltas, Key, {}});
            return EScan::Feed;
        }

        EScan Feed(const TRowState& row, TRowVersion& version) override {
            UNIT_ASSERT(version < LastVersion);
            LastVersion = version;
            auto& event = Record(EEvent::Row);
            event.Version = version;
            ReadValues(event, row);
            if (FirstRow) {
                FirstRow = false;
                if (LowerFirstVersion) {
                    version = *LowerFirstVersion;
                }
                if (PauseAfterFirstRow) {
                    Paused = true;
                    return EScan::Sleep;
                }
            }
            return EScan::Feed;
        }

        EScan EndKey() override {
            UNIT_ASSERT(KeyActive);
            UNIT_ASSERT(!HasSource);
            Events.push_back({EEvent::EndKey, Key, {}});
            KeyActive = false;
            return EScan::Feed;
        }

        EScan PageFault() override {
            ++PageFaults;
            return EScan::Feed;
        }

        TAutoPtr<IDestructable> Finish(EStatus) override {
            return nullptr;
        }

        void Describe(IOutputStream& out) const override {
            out << "RecordingVersionScan";
        }

        TVector<TEvent> Events;
        ui32 Sources = 0;
        ui32 PageFaults = 0;
        bool PauseAfterFirstRow = false;
        bool Paused = false;
        std::optional<TRowVersion> LowerFirstVersion;

    private:
        TEvent& Record(EEvent kind) {
            UNIT_ASSERT(KeyActive);
            UNIT_ASSERT(HasSource);
            HasSource = false;
            Events.push_back({kind, Key, Source});
            return Events.back();
        }

        static void ReadValues(TEvent& event, const TRowState& row) {
            if (row != ERowOp::Erase) {
                if (!row.Get(1).IsNull()) {
                    event.Value = row.Get(1).AsValue<ui32>();
                }
                if (!row.Get(2).IsNull()) {
                    event.Base = row.Get(2).AsValue<ui32>();
                }
            }
        }

        const TVector<ui32> Tags;
        ui32 Key = 0;
        std::optional<ui32> PreviousKey;
        TRowVersion LastVersion = TRowVersion::Max();
        TVersionScanSource Source;
        bool HasSource = false;
        bool KeyActive = false;
        bool FirstRow = true;
    };

    class TTestFeed final : public TFeed {
    public:
        TTestFeed(IScan* scan, const TSubset& subset, IPages* env)
            : TFeed(scan, subset)
            , Env(env)
        {
            Conf.NoErased = false;
        }

        IPages* MakeEnv() override {
            return Env;
        }

        TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>&) override {
            UNIT_FAIL("Unloaded parts are not used in this test");
            return {};
        }

    private:
        IPages* const Env;
    };

    class TOnceFailEnv final : public NTest::TTestEnv {
    public:
        const TSharedData* TryGetPage(const TPart* part, const NPage::TPageLocation& location,
                TGroupId group) override {
            return std::exchange(FailNext, false) ? nullptr
                : TTestEnv::TryGetPage(part, location, group);
        }

        const TSharedData* TryGetIndexPage(const TPart* part, const NPage::TPageLocation& location,
                TGroupId group) override {
            return std::exchange(FailNext, false) ? nullptr
                : TTestEnv::TryGetIndexPage(part, location, group);
        }

    private:
        bool FailNext = true;
    };

    void Run(TRecordingScan& scan, const TSubset& subset, IPages& env) {
        TTestFeed driver(&scan, subset, &env);
        for (ui32 step = 0; step < 10000; ++step) {
            const auto ready = driver.Process();
            if (ready == EReady::Gone) {
                ui32 feeds = 0;
                for (const auto& event : scan.Events) {
                    feeds += event.Kind == EEvent::Row || event.Kind == EEvent::Delta || event.Kind == EEvent::Lock;
                }
                UNIT_ASSERT_VALUES_EQUAL(scan.Sources, feeds);
                return;
            }
            UNIT_ASSERT(ready == EReady::Data || ready == EReady::Page);
            if (scan.Paused) {
                scan.Paused = false;
                driver.Resume(EScan::Feed);
            }
        }
        UNIT_FAIL("Version scan made no progress");
    }

    TVector<const TEvent*> Events(const TRecordingScan& scan, EEvent kind) {
        TVector<const TEvent*> result;
        for (const auto& event : scan.Events) {
            if (event.Kind == kind) {
                result.push_back(&event);
            }
        }
        return result;
    }

}

Y_UNIT_TEST_SUITE(TVersionScanSourceTest) {

    Y_UNIT_TEST(KeysAndVersionsFollowTheDocumentedOrder) {
        TFixture f;
        auto oldest = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100))
            .AddN(ui32(2), ui32(10), ui32(100)).AddN(ui32(3), ui32(10), ui32(100)).Finish().ToPartView();
        auto middle = f.Cook(20).Ver({20, 1}).AddN(ui32(1), ui32(20), ui32(200))
            .AddN(ui32(3), ui32(20), ui32(200)).Finish().ToPartView();
        auto newest = f.Cook(30).Ver({30, 1}).AddN(ui32(1), ui32(30), ui32(300))
            .Ver({20, 1}).AddN(ui32(2), ui32(20), ui32(200)).Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {middle, oldest, newest};
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 7);
        const ui32 keys[] = {1, 1, 1, 2, 2, 3, 3};
        const ui64 versions[] = {30, 20, 10, 20, 10, 20, 10};
        const TPart* sources[] = {newest.Part.Get(), middle.Part.Get(), oldest.Part.Get(),
            newest.Part.Get(), oldest.Part.Get(), middle.Part.Get(), oldest.Part.Get()};
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Key, keys[i]);
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Version, TRowVersion(versions[i], 1));
            UNIT_ASSERT(rows[i]->Source.Part == sources[i]);
            UNIT_ASSERT(!rows[i]->Source.MemTable);
        }
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginKey).size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 3);
    }

    Y_UNIT_TEST(PartialUpsertKeepsTheNewestSourceAfterRestoringItsBase) {
        TFixture f;
        auto base = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100)).Finish().ToPartView();
        auto partial = f.Cook(20).Ver({20, 1}).AddN(ui32(1), ui32(20)).Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {base, partial};
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        UNIT_ASSERT(rows[0]->Source.Part == partial.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Value.value(), 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Base.value(), 100);
        UNIT_ASSERT(rows[1]->Source.Part == base.Part.Get());
    }

    Y_UNIT_TEST(UncommittedMemoryDeltaAndCommittedPartHaveDistinctSources) {
        TFixture f;
        auto base = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100)).Finish().ToPartView();
        auto memory = f.Memory(20);
        f.Write(*memory, 1, 20, TRowVersion(Max<ui64>(), 123));
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {base};
        subset.Frozen.emplace_back(memory, memory->Immediate());
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        const auto deltas = Events(scan, EEvent::Delta);
        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0]->TxId, 123);
        UNIT_ASSERT(deltas[0]->Source.MemTable == memory.Get());
        UNIT_ASSERT(!deltas[0]->Source.Part);
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT(rows[0]->Source.Part == base.Part.Get());
        UNIT_ASSERT(!rows[0]->Source.MemTable);
        const auto deltaPos = std::find_if(scan.Events.begin(), scan.Events.end(),
            [](const auto& event) { return event.Kind == EEvent::Delta; });
        const auto rowPos = std::find_if(scan.Events.begin(), scan.Events.end(),
            [](const auto& event) { return event.Kind == EEvent::Row; });
        UNIT_ASSERT(deltaPos < rowPos);
    }

    Y_UNIT_TEST(CommittedTransactionDeltaKeepsItsPartSource) {
        TFixture f;
        auto base = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100)).Finish().ToPartView();
        auto delta = f.Cook(30).Delta(123).AddN(ui32(1), ui32(30), ui32(300))
            .Ver({20, 1}).AddN(ui32(1), ui32(20), ui32(200)).Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {delta, base};
        subset.CommittedTransactions.Add(123, {30, 1});
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        UNIT_ASSERT(Events(scan, EEvent::Delta).empty());
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Version, TRowVersion(30, 1));
        UNIT_ASSERT(rows[0]->Source.Part == delta.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(rows[1]->Version, TRowVersion(20, 1));
        UNIT_ASSERT(rows[1]->Source.Part == delta.Part.Get());
        UNIT_ASSERT(rows[2]->Source.Part == base.Part.Get());
    }

    Y_UNIT_TEST(LocksAndLockOnlyKeysReportTheirMemorySource) {
        TFixture f;
        auto memory = f.Memory(20);
        f.Write(*memory, 1, 20, {20, 1});
        f.Lock(*memory, 1, 123);
        f.Lock(*memory, 2, 124);
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Frozen.emplace_back(memory, memory->Immediate());
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        const auto locks = Events(scan, EEvent::Lock);
        UNIT_ASSERT_VALUES_EQUAL(locks.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(locks[0]->TxId, 123);
        UNIT_ASSERT_VALUES_EQUAL(locks[1]->TxId, 124);
        for (const auto* event : locks) {
            UNIT_ASSERT(event->Source.MemTable == memory.Get());
            UNIT_ASSERT(!event->Source.Part);
        }
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Key, 1);
        UNIT_ASSERT(rows[0]->Source.MemTable == memory.Get());
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 2);
    }

    Y_UNIT_TEST(PageFaultAndPausePreserveSourcesAndVersionOrder) {
        TFixture f;
        auto base = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100)).Finish().ToPartView();
        auto newer = f.Cook(20).Ver({20, 1}).AddN(ui32(1), ui32(20), ui32(200)).Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {newer, base};
        TRecordingScan scan(*subset.Scheme);
        scan.PauseAfterFirstRow = true;
        TOnceFailEnv env;
        Run(scan, subset, env);

        UNIT_ASSERT_VALUES_EQUAL(scan.PageFaults, 1);
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        UNIT_ASSERT(rows[0]->Source.Part == newer.Part.Get());
        UNIT_ASSERT(rows[1]->Source.Part == base.Part.Get());
    }

    Y_UNIT_TEST(ConsumerCanLowerTheVersionWithoutReorderingLaterRows) {
        TFixture f;
        auto part = f.Cook(30).Ver({30, 1}).AddN(ui32(1), ui32(30), ui32(300))
            .Ver({20, 1}).AddN(ui32(1), ui32(20), ui32(200))
            .Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100)).Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {part};
        TRecordingScan scan(*subset.Scheme);
        scan.LowerFirstVersion = TRowVersion(15, 1);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Version, TRowVersion(30, 1));
        UNIT_ASSERT_VALUES_EQUAL(rows[1]->Version, TRowVersion(10, 1));
        UNIT_ASSERT(rows[0]->Source.Part == part.Part.Get());
        UNIT_ASSERT(rows[1]->Source.Part == part.Part.Get());
    }

}

}

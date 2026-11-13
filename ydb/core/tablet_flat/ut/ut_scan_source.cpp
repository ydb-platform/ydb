#include <ydb/core/tablet_flat/flat_scan_feed.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <functional>
#include <optional>
#include <utility>
#include <variant>

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
            UNIT_ASSERT(std::visit([](const auto* pointer) { return pointer != nullptr; }, source));
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
        TTestFeed(IScan* scan, const TSubset& subset, IPages* env,
                TRowVersion snapshot = TRowVersion::Max())
            : TFeed(scan, subset, snapshot)
            , Env(env)
        {
            Conf.NoErased = false;
        }

        IPages* MakeEnv() override {
            return Env;
        }

        TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>& part) override {
            UNIT_ASSERT_C(PartLoader, "Missing cold part loader");
            return PartLoader(part);
        }

        std::function<TPartView(const TIntrusiveConstPtr<TColdPart>&)> PartLoader;

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

    void Run(TRecordingScan& scan, TTestFeed& driver) {
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

    void Run(TRecordingScan& scan, const TSubset& subset, IPages& env,
            TRowVersion snapshot = TRowVersion::Max()) {
        TTestFeed driver(&scan, subset, &env, snapshot);
        Run(scan, driver);
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
            UNIT_ASSERT(std::get<const TPart*>(rows[i]->Source) == sources[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginKey).size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 3);
    }

    Y_UNIT_TEST(VersionedScanReadsHistoryBeyondSnapshotBound) {
        TFixture f;
        auto oldest = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100))
            .Finish().ToPartView();
        auto middle = f.Cook(20).Ver({20, 1}).AddN(ui32(1), ui32(20), ui32(200))
            .Finish().ToPartView();
        auto newest = f.Cook(30).Ver({30, 1}).AddN(ui32(1), ui32(30), ui32(300))
            .Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {middle, newest, oldest};
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env, {25, 1});

        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Version, TRowVersion(30, 1));
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Value.value(), 30);
        UNIT_ASSERT(std::get<const TPart*>(rows[0]->Source) == newest.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(rows[1]->Version, TRowVersion(20, 1));
        UNIT_ASSERT_VALUES_EQUAL(rows[1]->Value.value(), 20);
        UNIT_ASSERT(std::get<const TPart*>(rows[1]->Source) == middle.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(rows[2]->Version, TRowVersion(10, 1));
        UNIT_ASSERT_VALUES_EQUAL(rows[2]->Value.value(), 10);
        UNIT_ASSERT(std::get<const TPart*>(rows[2]->Source) == oldest.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginKey).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 1);
    }

    Y_UNIT_TEST(SourceTracksSliceReuseAndPartSwitches) {
        TFixture f;
        auto sliced = f.Cook(10)
            .Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100))
            .Ver({9, 1}).AddN(ui32(1), ui32(9), ui32(90))
            .Ver({10, 1}).AddN(ui32(2), ui32(999), ui32(999))
            .AddN(ui32(3), ui32(10), ui32(100))
            .Ver({9, 1}).AddN(ui32(3), ui32(9), ui32(90))
            .Finish().ToPartView();
        auto other = f.Cook(20)
            .Ver({20, 1}).AddN(ui32(4), ui32(20), ui32(200))
            .Ver({19, 1}).AddN(ui32(4), ui32(19), ui32(190))
            .Finish().ToPartView();
        auto newest = f.Cook(30)
            .Ver({30, 1}).AddN(ui32(1), ui32(30), ui32(300))
            .AddN(ui32(3), ui32(30), ui32(300))
            .AddN(ui32(4), ui32(30), ui32(300))
            .Finish().ToPartView();

        const auto key = [](ui32 value) {
            return TSerializedCellVec(TVector<TCell>{TCell::Make(value)});
        };
        TIntrusivePtr<TSlices> slices = new TSlices;
        slices->emplace_back(key(1), key(1), 0, 0, true, true);
        slices->emplace_back(key(3), key(3), 2, 2, true, true);
        sliced.Slices = slices;
        sliced.Screen = slices->ToScreen();

        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {newest, other, sliced};

        // Verify that the older run really switches slices and then parts.
        TLevels levels(subset.Scheme->Keys);
        levels.AddContiguous(sliced.Part, sliced.Slices);
        levels.AddContiguous(other.Part, other.Slices);
        UNIT_ASSERT_VALUES_EQUAL(levels.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(levels.begin()->size(), 3);
        levels.AddContiguous(newest.Part, newest.Slices);
        UNIT_ASSERT_VALUES_EQUAL(levels.size(), 2);

        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 9);
        const ui32 keys[] = {1, 1, 1, 3, 3, 3, 4, 4, 4};
        const ui64 versions[] = {30, 10, 9, 30, 10, 9, 30, 20, 19};
        const TPart* sources[] = {
            newest.Part.Get(), sliced.Part.Get(), sliced.Part.Get(),
            newest.Part.Get(), sliced.Part.Get(), sliced.Part.Get(),
            newest.Part.Get(), other.Part.Get(), other.Part.Get(),
        };
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Key, keys[i]);
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Version, TRowVersion(versions[i], 1));
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Value.value(), versions[i]);
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Base.value(), versions[i] * 10);
            UNIT_ASSERT(std::get<const TPart*>(rows[i]->Source) == sources[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginKey).size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 3);
    }

    Y_UNIT_TEST(LoadedColdPartReportsItsSourceAndRestoresPartialUpserts) {
        TFixture f;
        auto cold = f.Cook(10).Ver({10, 1}).AddN(ui32(1), ui32(10), ui32(100))
            .AddN(ui32(2), ui32(10), ui32(200)).Finish().ToPartView();
        auto hot = f.Cook(20).Ver({20, 1}).AddN(ui32(1), ui32(20))
            .AddN(ui32(2), ui32(20)).Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {hot};
        subset.ColdParts.push_back(new TColdPart(cold->Label, cold->Epoch));
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        TTestFeed driver(&scan, subset, &env);
        ui32 loads = 0;
        driver.PartLoader = [&](const TIntrusiveConstPtr<TColdPart>& part) -> TPartView {
            UNIT_ASSERT(part.Get() == subset.ColdParts.front().Get());
            UNIT_ASSERT_VALUES_EQUAL(part->Label, cold->Label);
            UNIT_ASSERT_VALUES_EQUAL(part->Epoch, cold->Epoch);
            // The first attempt starts loading; the next one supplies the part.
            return ++loads == 1 ? TPartView{} : cold;
        };
        Run(scan, driver);

        UNIT_ASSERT_VALUES_EQUAL(loads, 2);
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
        const ui32 keys[] = {1, 1, 2, 2};
        const ui64 versions[] = {20, 10, 20, 10};
        const ui32 values[] = {20, 10, 20, 10};
        const ui32 bases[] = {100, 100, 200, 200};
        const TPart* sources[] = {hot.Part.Get(), cold.Part.Get(), hot.Part.Get(), cold.Part.Get()};
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Key, keys[i]);
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Version, TRowVersion(versions[i], 1));
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Value.value(), values[i]);
            UNIT_ASSERT_VALUES_EQUAL(rows[i]->Base.value(), bases[i]);
            UNIT_ASSERT(std::get<const TPart*>(rows[i]->Source) == sources[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginKey).size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 2);
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
        UNIT_ASSERT(std::get<const TPart*>(rows[0]->Source) == partial.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Value.value(), 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Base.value(), 100);
        UNIT_ASSERT(std::get<const TPart*>(rows[1]->Source) == base.Part.Get());
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
        UNIT_ASSERT(std::get<const TMemTable*>(deltas[0]->Source) == memory.Get());
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT(std::get<const TPart*>(rows[0]->Source) == base.Part.Get());
        const auto deltaPos = std::find_if(scan.Events.begin(), scan.Events.end(),
            [](const auto& event) { return event.Kind == EEvent::Delta; });
        const auto rowPos = std::find_if(scan.Events.begin(), scan.Events.end(),
            [](const auto& event) { return event.Kind == EEvent::Row; });
        UNIT_ASSERT(deltaPos < rowPos);
    }

    Y_UNIT_TEST(DeltaOnlyKeysReportTheirMemoryAndPartSources) {
        TFixture f;
        auto memory = f.Memory(20);
        f.Write(*memory, 1, 20, TRowVersion(Max<ui64>(), 123));
        auto part = f.Cook(30).Delta(124).AddN(ui32(2), ui32(30), ui32(300))
            .Finish().ToPartView();
        TSubset subset(TEpoch::Zero(), f.Layout.RowScheme());
        subset.Flatten = {part};
        subset.Frozen.emplace_back(memory, memory->Immediate());
        TRecordingScan scan(*subset.Scheme);
        NTest::TTestEnv env;
        Run(scan, subset, env);

        UNIT_ASSERT(Events(scan, EEvent::Row).empty());
        const auto deltas = Events(scan, EEvent::Delta);
        UNIT_ASSERT_VALUES_EQUAL(deltas.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0]->Key, 1);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0]->TxId, 123);
        UNIT_ASSERT_VALUES_EQUAL(deltas[0]->Value.value(), 20);
        UNIT_ASSERT(std::get<const TMemTable*>(deltas[0]->Source) == memory.Get());
        UNIT_ASSERT_VALUES_EQUAL(deltas[1]->Key, 2);
        UNIT_ASSERT_VALUES_EQUAL(deltas[1]->TxId, 124);
        UNIT_ASSERT_VALUES_EQUAL(deltas[1]->Value.value(), 30);
        UNIT_ASSERT(std::get<const TPart*>(deltas[1]->Source) == part.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginKey).size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndKey).size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::BeginDeltas).size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Events(scan, EEvent::EndDeltas).size(), 2);
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
        UNIT_ASSERT(std::get<const TPart*>(rows[0]->Source) == delta.Part.Get());
        UNIT_ASSERT_VALUES_EQUAL(rows[1]->Version, TRowVersion(20, 1));
        UNIT_ASSERT(std::get<const TPart*>(rows[1]->Source) == delta.Part.Get());
        UNIT_ASSERT(std::get<const TPart*>(rows[2]->Source) == base.Part.Get());
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
            UNIT_ASSERT(std::get<const TMemTable*>(event->Source) == memory.Get());
        }
        const auto rows = Events(scan, EEvent::Row);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]->Key, 1);
        UNIT_ASSERT(std::get<const TMemTable*>(rows[0]->Source) == memory.Get());
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
        UNIT_ASSERT(std::get<const TPart*>(rows[0]->Source) == newer.Part.Get());
        UNIT_ASSERT(std::get<const TPart*>(rows[1]->Source) == base.Part.Get());
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
        UNIT_ASSERT(std::get<const TPart*>(rows[0]->Source) == part.Part.Get());
        UNIT_ASSERT(std::get<const TPart*>(rows[1]->Source) == part.Part.Get());
    }

}

}

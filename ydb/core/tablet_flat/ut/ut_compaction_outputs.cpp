#include <ydb/core/tablet_flat/flat_compaction_outputs.h>
#include <ydb/core/tablet_flat/flat_compaction_config.h>
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_page_label.h>
#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_ops_compact.h>
#include <ydb/core/tablet_flat/flat_scan_actor.h>
#include <ydb/core/tablet_flat/flat_table.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

#include <algorithm>
#include <cstring>

namespace NKikimr::NTabletFlatExecutor {
namespace {
    using namespace NTable;

    const NTest::TLayoutCook& Layout() {
        static const auto layout = [] {
            NTest::TLayoutCook result;
            result.Col(0, 0, NScheme::NTypeIds::Uint32)
                .Col(0, 1, NScheme::NTypeIds::String)
                .Key({0});
            return result;
        }();
        return layout;
    }

    struct TOutputConfigFixture {
        static constexpr ui32 ColdTier = 42;
        static constexpr ui32 ArchiveTier = 7;

        NTest::TLayoutCook RowLayout;
        TScheme::TTableInfo Table{"table", 1};
        TCompactionOutputSettings Settings;

        TOutputConfigFixture() {
            RowLayout.Col(0, 0, NScheme::NTypeIds::Uint32)
                .Col(3, 1, NScheme::NTypeIds::String)
                .Key({0});
            Table.Rooms[0].Main = 1;
            Table.Rooms[0].Outer = 2;
            Table.Rooms[0].Blobs = {3};
            Table.Families[0].Small = 16;
            Table.Families[0].Large = 32;
            Table.Rooms[3].Main = 4;
            Table.Families[3].Room = 3;
            Table.Families[3].Codec = NPage::ECodec::LZ4;
            Table.Families[3].Cache = NPage::ECache::Ever;

            Table.Rooms[ColdTier].Main = 5;
            Table.Rooms[ColdTier].Outer = 6;
            Table.Rooms[ColdTier].Blobs = {7, 8};
            auto& cold = Table.Families[ColdTier];
            cold.Room = ColdTier;
            cold.Codec = NPage::ECodec::LZ4;
            cold.Small = 64;
            cold.Large = 128;
            cold.GroupIndexesInDescriptor = false;
            Table.Rooms[ArchiveTier].Main = 9;
            Table.Rooms[ArchiveTier].Outer = 10;
            Table.Rooms[ArchiveTier].Blobs = {11};
            Table.Families[ArchiveTier].Room = ArchiveTier;

            auto policy = NLocalDb::CreateDefaultUserTablePolicy();
            policy->Generations[1].TierId = ColdTier;
            policy->Generations[2].TierId = ArchiveTier;
            policy->Generations.push_back(policy->Generations.back());
            Table.CompactionPolicy = policy;
            Table.TieringPolicy = TTieringPolicy{
                .ColumnTag = 0,
                .Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
                .Rules = {{.TierId = ColdTier, .Ranges = {{
                    .To = TTierRelativeBound{TDuration::Days(7)}, .ToInclusive = true}}}},
            };
            Settings.MaxBlobSize = 256;
        }

        void Prepare(TCompactCfg& conf, const TSubset* preparedSubset = nullptr) const {
            const auto scheme = RowLayout.RowScheme();
            TSubset subset(TEpoch::Zero(), scheme);
            PrepareCompactionOutputs(conf, Table, *scheme,
                preparedSubset ? *preparedSubset : subset, Settings,
                [](ui8 channel) { return ui32(channel) + 10; });
        }
    };

    void AddOutput(TCompactCfg& conf, ui32 id, ui8 channel = 1) {
        auto& output = conf.Outputs.emplace_back();
        output.OutputId = id;
        output.Layout.Final = false;
        output.Layout.Group(0).PageRows = 2;
        output.Writer.Groups[0].Channel = channel;
        output.Writer.Groups[0].Cache = NPage::ECache::Ever;
        output.Writer.Groups[0].MaxBlobSize = 128;
        output.Writer.OuterChannel = channel;
        output.Writer.ExtraChannel = channel;
        output.Writer.BlobsChannels = {channel};
        output.Writer.Slots = {{channel, ui32(channel) + 10}};
    }

    class TParityRouter final : public ICompactionRouter {
    public:
        explicit TParityRouter(ui32& calls)
            : Calls(calls)
        {
        }

        ui32 RouteKey(TArrayRef<const TCell> key, const TRowState*, ui32) override {
            ++Calls;
            return key[0].AsValue<ui32>() % 2;
        }

    private:
        ui32& Calls;
    };

    void WriteKey(TCompactionOutputs& outputs, ui32 key, const TString& value,
            bool withHistory = false) {
        const TCell keyCell = TCell::Make(key);
        outputs.BeginKey({&keyCell, 1});
        TRowState row(2);
        row.Touch(ERowOp::Upsert);
        row.Set(0, ECellOp::Set, keyCell);
        row.Set(1, ECellOp::Set, TCell(value.data(), value.size()));
        if (withHistory) {
            outputs.AddKeyLock(ELockMode::Exclusive, 100 + key);
            outputs.AddKeyDelta(row, 200 + key);
        }
        outputs.AddKeyVersion(row, TRowVersion(20, 1));
        if (withHistory) {
            outputs.AddKeyVersion(row, TRowVersion(10, 1));
        }
        outputs.EndKey();
    }

    TPartView LoadPart(const TCompactionOutputs::TResult& result) {
        TVector<TIntrusivePtr<TLoader::TPageCollection>> collections;
        for (const auto& written : result.PageCollections) {
            collections.push_back(MakeIntrusive<TLoader::TPageCollection>(written.PageCollection));
        }
        TPartComponents components{
            .PageCollectionComponents = TVector<TPageCollectionComponents>(collections.size()),
            .Opaque = result.Overlay,
            .Epoch = TEpoch::Max(),
        };
        TLoader loader(std::move(components), std::move(collections));
        while (auto fetch = loader.Run({.PreloadIndex = false, .PreloadData = false})) {
            TVector<NSharedCache::TEvResult::TLoaded> loaded;
            for (const auto& location : fetch.Pages) {
                const TSharedData* data = nullptr;
                for (const auto& written : result.PageCollections) {
                    if (written.PageCollection->Label() != fetch.PageCollection->Label()) {
                        continue;
                    }
                    for (const auto* pages : {&written.StickyPages, &written.RegularPages}) {
                        for (const auto& page : *pages) {
                            if (page.Location.Offset == location.Offset) {
                                data = &page.Data;
                            }
                        }
                    }
                }
                UNIT_ASSERT_C(data, "Missing freshly written page " << location);
                loaded.emplace_back(location.Offset, location.Size,
                    NSharedCache::TSharedPageRef::MakePrivate(*data));
            }
            loader.Save(std::move(loaded));
        }
        return loader.Result();
    }

    ui32 CheckKeys(const TCompactionOutputs::TResult& result, bool parity) {
        const auto scheme = MakeIntrusive<TPartScheme>(Layout().RowScheme()->Cols);
        const auto& keyColumn = scheme->Groups[0].ColsKeyData[0];
        ui32 count = 0;
        for (const auto& page : result.PageCollections[0].StickyPages) {
            if (page.Location.Type != NPage::EPage::DataPage) {
                continue;
            }
            NPage::TDataPage data(&page.Data);
            for (ui32 i = 0; i < data->Count; ++i) {
                const auto key = data->Record(i)->Cell(keyColumn).AsValue<ui32>();
                UNIT_ASSERT_VALUES_EQUAL(result.OutputId, parity ? (key % 2 ? 31 : 17) : 17);
                ++count;
            }
        }
        return count;
    }

    class TWrittenPages final : public IPages {
    public:
        explicit TWrittenPages(const THashMap<TLogoBlobID, TSharedData>* blobs = nullptr)
            : Blobs(blobs)
        { }

        void Add(const TPart* part) {
            const auto* store = dynamic_cast<const NTable::TPartStore*>(part);
            UNIT_ASSERT(store);
            auto& slots = Pages[part];
            slots.resize(store->PageCollections.size());
            for (ui32 slot = 0; slot < slots.size(); ++slot) {
                for (const auto& page : store->PageCollections[slot]->GetPageMap()) {
                    auto body = page->SharedBody;
                    UNIT_ASSERT(body.Use());
                    slots[slot][page->Offset] = NSharedCache::TPinnedPageRef(body).GetData();
                }
            }
        }

        void Add(const TPart* part, const TCompactionOutputs::TResult& result) {
            auto& slots = Pages[part];
            slots.resize(result.PageCollections.size());
            for (ui32 slot = 0; slot < slots.size(); ++slot) {
                const auto& collection = result.PageCollections[slot];
                for (const auto* pages : {&collection.StickyPages, &collection.RegularPages}) {
                    for (const auto& page : *pages) {
                        slots[slot][page.Location.Offset] = page.Data;
                    }
                }
            }
        }

        TResult Locate(const TMemTable*, ui64, ui32) override {
            UNIT_FAIL("Unexpected memtable blob");
            return {};
        }

        TResult Locate(const TPart* part, ui64 ref, ELargeObj kind) override {
            UNIT_ASSERT_C(Blobs, "Unexpected outlined value without captured blob storage");
            UNIT_ASSERT(ref <= Max<ui32>());
            if (kind == ELargeObj::Extern) {
                ++ExternalReads;
                return {true, &Blobs->at(part->Blobs->Glob(ui32(ref)).Logo)};
            }
            UNIT_ASSERT(kind == ELargeObj::Outer);
            ++OuterReads;
            return {true, GetPage(part, part->OuterSlot(),
                part->GetPageCollection(part->OuterSlot())->GetLocation(ui32(ref)))};
        }

        const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location,
                TGroupId groupId) override {
            return GetPage(part, part->DataSlot(groupId), location);
        }

        const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location,
                TGroupId groupId) override {
            return GetPage(part, part->IndexSlot(groupId), location);
        }

        ui32 ExternalReads = 0;
        ui32 OuterReads = 0;
        ui32 StoragePageReads = 0;

    private:
        const TSharedData* GetPage(const TPart* part, ui32 slot, const TPageLocation& location) {
            auto& slots = Pages[part];
            if (slots.empty()) {
                const auto* store = dynamic_cast<const TPartStore*>(part);
                UNIT_ASSERT(store);
                slots.resize(store->PageCollections.size());
            }
            auto& pages = slots.at(slot);
            if (const auto it = pages.find(location.Offset); it != pages.end()) {
                return &it->second;
            }
            UNIT_ASSERT_C(Blobs, "Missing freshly written page " << location);
            const auto* collection = part->GetPageCollection(slot);
            const auto bounds = collection->Bounds(location);
            auto data = TSharedData::Uninitialized(bounds.Bytes);
            size_t copied = 0;
            for (ui32 index = bounds.Lo.Blob; copied < data.size(); ++index) {
                UNIT_ASSERT(index <= bounds.Up.Blob);
                const auto& blob = Blobs->at(collection->Glob(index).Logo);
                const size_t skip = index == bounds.Lo.Blob ? bounds.Lo.Skip : 0;
                UNIT_ASSERT(skip < blob.size());
                const size_t size = Min(data.size() - copied, blob.size() - skip);
                memcpy(data.mutable_data() + copied, blob.data() + skip, size);
                copied += size;
            }
            UNIT_ASSERT(collection->Verify(location, data));
            ++StoragePageReads;
            return &pages.emplace(location.Offset, std::move(data)).first->second;
        }

        const THashMap<TLogoBlobID, TSharedData>* Blobs;
        THashMap<const TPart*, TVector<THashMap<NPage::TPageOffset, TSharedData>>> Pages;
    };

    // Run the production scan actor/driver, retaining BS requests until the test
    // confirms them. Input comes from a real memtable; output uses the real bundle
    // and loader, with cached pages available for normal table iterators.
    class TCompactionScanEnv {
        class TReceiver : public NActors::TActor<TReceiver> {
        public:
            explicit TReceiver(TCompactionScanEnv& env)
                : TActor(&TReceiver::Receive)
                , Env(env)
            {}

        private:
            void Receive(TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvBlobStorage::TEvPut::EventType: {
                        const auto& put = *ev->Get<TEvBlobStorage::TEvPut>();
                        UNIT_ASSERT_VALUES_EQUAL(put.Buffer.size(), put.Id.BlobSize());
                        UNIT_ASSERT(Env.WrittenBlobs.insert(put.Id).second);
                        Env.WrittenBlobData.emplace(put.Id, TSharedData::Copy(put.Buffer.ConvertToString()));
                        Env.PendingPuts.emplace_back(ev.Release());
                        break;
                    }
                    case NOps::TEvResult::EventType:
                        UNIT_ASSERT(!Env.Result);
                        Env.Result.Reset(ev.Release());
                        break;
                    case NOps::TEvScanStat::EventType:
                    case NBlockIO::TEvStat::EventType:
                        break;
                    default:
                        UNIT_FAIL("Unexpected compaction test event " << ev->GetTypeName());
                }
            }

            TCompactionScanEnv& Env;
        };

    public:
        TCompactionScanEnv() {
            TAppPrepare app;
            Runtime.Initialize(app.Unwrap());
            Owner = Runtime.Register(new TReceiver(*this));
            Storage = Owner;
            for (ui32 channel = 1; channel <= 11; ++channel) {
                Runtime.RegisterService(MakeBlobStorageProxyID(channel + 10), Storage);
            }

            NSharedCache::TSharedCacheConfig config;
            config.SetMemoryLimit(128 * 1024 * 1024);
            Runtime.RegisterService(NSharedCache::MakeSharedPageCacheId(),
                Runtime.Register(NSharedCache::CreateSharedPageCache(config, Runtime.GetDynamicCounters())));
        }

        void Start(THolder<TCompactCfg> conf, TIntrusivePtr<TMemTable> input,
                THolder<ICompactionRouter> router) {
            conf->Epoch = input->Epoch;
            conf->Params->Table = 101;
            const TLogoBlobID mask(1, 2, 3, 0, 0, 0);
            THolder<TScanSnapshot> snapshot(new TScanSnapshot{
                101,
                MakeIntrusive<TBarrier>(mask.Step()),
                new TSubset(TEpoch::Zero(), input->Scheme,
                    TVector<TMemTableSnapshot>{{input, input->Immediate()}}),
                TRowVersion::Max(),
            });
            TAutoPtr<TCompactCfg> scanConf = conf.Release();
            TAutoPtr<IScan> scan = new TOpsCompact(Owner, mask, scanConf, std::move(router));
            Driver = Runtime.Register(new NOps::TDriver(1, scan, {}, std::move(snapshot)),
                0, 0, NActors::TMailboxType::Simple, 0, Owner);
            Drain();
        }

        void Drain() {
            Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }

        void Confirm(size_t index = 0, NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            UNIT_ASSERT(index < PendingPuts.size());
            auto request = std::move(PendingPuts[index]);
            PendingPuts.erase(PendingPuts.begin() + index);
            const auto& put = *request->Get<TEvBlobStorage::TEvPut>();
            Runtime.Send(new IEventHandle(request->Sender, Storage,
                new TEvBlobStorage::TEvPutResult(status, put.Id,
                    TStorageStatusFlags(NKikimrBlobStorage::StatusIsValid),
                    ui32(put.Id.Channel()) + 10, 1.f), 0, request->Cookie));
            Drain();
        }

        void Complete() {
            while (!Result) {
                UNIT_ASSERT_C(!PendingPuts.empty(), "Compaction stopped without pending BS writes");
                Confirm();
            }
            UNIT_ASSERT(PendingPuts.empty());
        }

        void ConfirmChannel(ui8 channel) {
            for (;;) {
                auto it = std::find_if(PendingPuts.begin(), PendingPuts.end(), [channel](const auto& ev) {
                    return ev->template Get<TEvBlobStorage::TEvPut>()->Id.Channel() == channel;
                });
                if (it == PendingPuts.end()) {
                    return;
                }
                Confirm(it - PendingPuts.begin());
            }
        }

        const TProdCompact& Product() const {
            UNIT_ASSERT(Result);
            auto* product = dynamic_cast<TProdCompact*>(Result->Get<NOps::TEvResult>()->Result.Get());
            UNIT_ASSERT(product);
            if (product->Exception) {
                std::rethrow_exception(product->Exception);
            }
            return *product;
        }

        TTestActorRuntime Runtime;
        TActorId Owner;
        TActorId Storage;
        TActorId Driver;
        THolder<IEventHandle> Result;
        TDeque<THolder<IEventHandle>> PendingPuts;
        THashSet<TLogoBlobID> WrittenBlobs;
        THashMap<TLogoBlobID, TSharedData> WrittenBlobData;
    };

    THolder<TCompactCfg> ScanConfig(ui32 outputs = 2) {
        auto conf = MakeHolder<TCompactCfg>(MakeHolder<TCompactionParams>());
        AddOutput(*conf, 17);
        if (outputs == 2) {
            AddOutput(*conf, 31, 3);
        }
        for (auto& output : conf->Outputs) {
            output.Writer.Groups[0].MaxBlobSize = 4096;
        }
        return conf;
    }

    void AddMemRow(TMemTable& table, const NTest::TRow& row, TRowVersion version) {
        const auto split = NTest::TRowTool(*table.Scheme).Split(row, false, true);
        table.Update(ERowOp::Upsert, split.Key, split.Ops, {}, version, nullptr);
    }

    TString ScanValue(TStringBuf prefix, ui32 key) {
        return TString(prefix) + ToString(key) + TString(80, 'x');
    }

    TIntrusivePtr<TMemTable> ScanInput(ui32 keys, bool history = false, ui32 valueSize = 0) {
        auto table = MakeIntrusive<TMemTable>(Layout().RowScheme(), TEpoch(i64(7)), ui64(0));
        for (ui32 key = 0; key < keys; ++key) {
            if (history) {
                AddMemRow(*table, *NTest::TSchemedCookRow(*Layout()).Col(key, ScanValue("old", key)), {10, 1});
            }
            AddMemRow(*table, *NTest::TSchemedCookRow(*Layout()).Col(key,
                valueSize ? TString(valueSize, 'a') : ScanValue("head", key)), {20, 1});
            if (history) {
                AddMemRow(*table, *NTest::TSchemedCookRow(*Layout()).Col(key, ScanValue("delta", key)),
                    {Max<ui64>(), 200 + key});
                const auto lockRow = *NTest::TSchemedCookRow(*Layout()).Col(key);
                const auto raw = NTest::TRowTool(*Layout()).LookupKey(lockRow);
                table->LockRow(ELockMode::Exclusive, raw, 100 + key);
            }
        }
        return table;
    }

    THolder<TTable> ReadTable(const TRowScheme& scheme, TArrayRef<const TPartView> parts) {
        TScheme::TTableInfo info("test", 101);
        for (const auto& col : scheme.Cols) {
            auto& column = info.Columns[col.Tag];
            column = TColumn("", col.Tag, col.TypeInfo, "");
            if (col.IsKey()) {
                column.KeyOrder = col.Key;
                if (info.KeyColumns.size() <= col.Key) {
                    info.KeyColumns.resize(col.Key + 1);
                }
                info.KeyColumns[col.Key] = col.Tag;
            }
        }
        auto table = MakeHolder<TTable>(TEpoch::Zero());
        table->SetScheme(info);
        TSubset empty(TEpoch::Zero(), table->GetScheme());
        table->Replace(empty, parts, {});
        return table;
    }

    const NTest::TLayoutCook& TierScanLayout() {
        static const auto layout = [] {
            NTest::TLayoutCook result;
            result.Col(0, 0, NScheme::NTypeIds::Uint32)
                .Col(0, 1, NScheme::NTypeIds::Uint32)
                .Col(0, 2, NScheme::NTypeIds::String)
                .Key({0});
            return result;
        }();
        return layout;
    }

    TIntrusivePtr<TMemTable> TierScanInput() {
        return MakeIntrusive<TMemTable>(TierScanLayout().RowScheme(), TEpoch::FromIndex(7), ui64(0));
    }

    TTieringPolicy TierScanPolicy(bool archive = false) {
        TTieringPolicy policy{
            .ColumnTag = 1,
            .Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
        };
        if (archive) {
            policy.Rules.push_back({.TierId = 7, .Ranges = {{
                .To = TTierRelativeBound{TDuration::Seconds(200)}, .ToInclusive = true}}});
        }
        policy.Rules.push_back({.TierId = 42, .Ranges = {{
            .To = TTierRelativeBound{TDuration::Seconds(100)}, .ToInclusive = true}}});
        return policy;
    }

    THolder<TCompactCfg> TierScanConfig(const TIntrusivePtr<TMemTable>& input,
            TVector<TCompactionPartInfo> remaining = {}, bool archive = false) {
        auto conf = ScanConfig();
        conf->Params->Mode = TCompactionParams::EMode::TierAware;
        conf->Outputs[0].TierId = 0;
        conf->Outputs[1].TierId = 42;
        if (archive) {
            AddOutput(*conf, 73);
            conf->Outputs.back().TierId = 7;
            conf->Outputs.back().Writer.Groups[0].MaxBlobSize = 4096;
        }
        conf->Epoch = input->Epoch;
        TSubset subset(TEpoch::Zero(), input->Scheme,
            TVector<TMemTableSnapshot>{{input, input->Immediate()}});
        conf->Params->Context = TCompactionContext::Build(std::move(remaining), subset);
        return conf;
    }

    THolder<ICompactionRouter> TierScanRouter(const TCompactCfg& conf, bool archive = false) {
        return MakeHolder<TTierCompactionRouter>(conf,
            std::optional<TTieringPolicy>(TierScanPolicy(archive)), TInstant::Seconds(1000),
            archive ? TVector<ui32>{0, 42, 7} : TVector<ui32>{0, 42});
    }

    void ConfigureOutlinedTierOutputs(TCompactCfg& conf, bool sharedChannels,
            bool btreeV2 = false, bool indexInDescriptor = true) {
        for (ui32 index = 0; index < conf.Outputs.size(); ++index) {
            auto& output = conf.Outputs[index];
            const bool cold = index != 0 && !sharedChannels;
            output.Layout.SmallEdge = 32;
            output.Layout.LargeEdge = 256;
            output.Layout.MainPageCollectionEdge = 1024;
            output.Layout.WriteFlatIndex = !btreeV2;
            output.Layout.WriteBTreeIndexV2 = btreeV2;
            output.Layout.Layout.KeyIndexInDescriptor = cold ? indexInDescriptor : true;
            output.Writer.WriteBTreeIndexV2 = btreeV2;
            output.Writer.Groups[0].Channel = cold ? 3 : 1;
            output.Writer.Groups[0].MaxBlobSize = 4096;
            output.Writer.OuterChannel = cold ? 4 : 2;
            output.Writer.BlobsChannels = {ui8(cold ? 6 : 5)};
            output.Writer.ExtraChannel = 1;
            output.Writer.Slots = cold
                ? TVector<NWriter::TConf::TSlot>{{1, 11}, {3, 13}, {4, 14}, {6, 16}}
                : TVector<NWriter::TConf::TSlot>{{1, 11}, {2, 12}, {5, 15}};
            NWriter::ApplyDescriptorChannel(output.Layout, output.Writer, 1);
        }
    }

    TString OutlinedScanValue(ui32 key, bool historic) {
        const ui32 sizes[] = {16, 128, 4096};
        return TString(sizes[key % 3], char((historic ? 'a' : 'A') + key));
    }

    void CheckOutlinedTierScan(bool sharedChannels, bool btreeV2, bool indexInDescriptor) {
        TCompactionScanEnv env;
        auto input = TierScanInput();
        constexpr ui32 keyCount = 12;
        for (ui32 key = 0; key < keyCount; ++key) {
            for (bool historic : {true, false}) {
                AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                    key, ui32(key % 2 ? 100 : 950), OutlinedScanValue(key, historic)),
                    historic ? TRowVersion(10, 1) : TRowVersion(20, 1));
            }
        }
        auto conf = TierScanConfig(input);
        ConfigureOutlinedTierOutputs(*conf, sharedChannels, btreeV2, indexInDescriptor);
        auto router = TierScanRouter(*conf);
        env.Start(std::move(conf), input, std::move(router));
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);
        env.Complete();
        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        THashSet<ui32> outputs;
        // Read data, indexes and outlined cells from the captured BS payloads,
        // without preloading the writer's cached data pages into this environment.
        TWrittenPages pages(&env.WrittenBlobData);
        const TVector<ui32> tags{0, 1, 2};
        for (const auto& part : result->Parts) {
            const ui32 output = result->PartOutputIds.at(part->Label);
            outputs.insert(output);
            const bool cold = output == 31;
            const ui8 dataChannel = cold && !sharedChannels ? 3 : 1;
            const ui8 outerChannel = cold && !sharedChannels ? 4 : 2;
            const ui8 blobChannel = cold && !sharedChannels ? 6 : 5;
            const auto* store = dynamic_cast<const TPartStore*>(part.Part.Get());
            UNIT_ASSERT(store);
            UNIT_ASSERT_VALUES_EQUAL(ui32(store->GetSlotChannel(part->DescriptorSlot())), 1);
            UNIT_ASSERT_VALUES_EQUAL(ui32(store->GetSlotChannel(part->DataSlot(NPage::TGroupId(0)))),
                ui32(dataChannel));
            UNIT_ASSERT_VALUES_EQUAL(ui32(store->GetSlotChannel(part->IndexSlot(NPage::TGroupId(0)))),
                ui32(cold && !sharedChannels && !indexInDescriptor ? dataChannel : 1));
            if (part->Small) {
                UNIT_ASSERT_VALUES_EQUAL(ui32(store->GetSlotChannel(part->OuterSlot())), ui32(outerChannel));
            }
            if (part->Blobs) {
                for (ui32 ref = 0; ref < part->Blobs->Total(); ++ref) {
                    const auto blob = part->Blobs->Glob(ref).Logo;
                    UNIT_ASSERT_VALUES_EQUAL(ui32(blob.Channel()), ui32(blobChannel));
                    UNIT_ASSERT(env.WrittenBlobData.contains(blob));
                }
            }
            auto table = ReadTable(*input->Scheme, {&part, 1});
            auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
            while (iter->Next(ENext::Data) == EReady::Data) {
                const ui32 key = iter->GetKey().Cells()[0].AsValue<ui32>();
                UNIT_ASSERT_VALUES_EQUAL(output, key % 2 ? 31 : 17);
            }
        }
        UNIT_ASSERT(outputs == (THashSet<ui32>{17, 31}));

        auto table = ReadTable(*input->Scheme, result->Parts);
        for (bool historic : {false, true}) {
            const auto version = historic ? TRowVersion(10, 1) : TRowVersion::Max();
            auto forward = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, version);
            auto reverse = table->IterateReverse(TRawVals{}, tags, &pages, ESeek::Lower, version);
            for (ui32 step = 0; step < keyCount; ++step) {
                UNIT_ASSERT_VALUES_EQUAL(forward->Next(ENext::Data), EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(forward->GetKey().Cells()[0].AsValue<ui32>(), step);
                UNIT_ASSERT_VALUES_EQUAL(forward->Row().Get(2).AsBuf(), OutlinedScanValue(step, historic));
                const ui32 key = keyCount - step - 1;
                UNIT_ASSERT_VALUES_EQUAL(reverse->Next(ENext::Data), EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(reverse->GetKey().Cells()[0].AsValue<ui32>(), key);
                UNIT_ASSERT_VALUES_EQUAL(reverse->Row().Get(2).AsBuf(), OutlinedScanValue(key, historic));
            }
            UNIT_ASSERT_VALUES_EQUAL(forward->Next(ENext::Data), EReady::Gone);
            UNIT_ASSERT_VALUES_EQUAL(reverse->Next(ENext::Data), EReady::Gone);
        }
        UNIT_ASSERT(pages.ExternalReads > 0);
        UNIT_ASSERT(pages.OuterReads > 0);
        UNIT_ASSERT(pages.StoragePageReads > 0);
    }

    class TCountingTierRouter final : public ICompactionRouter {
    public:
        TCountingTierRouter(THolder<ICompactionRouter> delegate, ui32& calls)
            : Delegate(std::move(delegate))
            , Calls(calls)
        { }

        void Prepare(const TRowScheme& scheme) override {
            Delegate->Prepare(scheme);
        }

        ui32 RouteKey(TCellsRef key, const TRowState* row, ui32 sourceTier) override {
            ++Calls;
            return Delegate->RouteKey(key, row, sourceTier);
        }

        TVector<ui32> GetUnrepresentableTierIds() const override {
            return Delegate->GetUnrepresentableTierIds();
        }

    private:
        THolder<ICompactionRouter> Delegate;
        ui32& Calls;
    };
}

Y_UNIT_TEST_SUITE(TCompactionOutputConfigTest) {
    Y_UNIT_TEST(RegularDefaultPreservesColumnFamilies) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        UNIT_ASSERT(conf.Params->Mode == TCompactionParams::EMode::Regular);
        env.Prepare(conf);

        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 1);
        const auto& output = conf.Outputs[0];
        UNIT_ASSERT_VALUES_EQUAL(output.OutputId, 0);
        UNIT_ASSERT_VALUES_EQUAL(output.TierId, 0);
        UNIT_ASSERT_VALUES_EQUAL(output.Layout.Groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(output.Writer.Groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ui32(output.Writer.Groups[0].Channel), 1);
        UNIT_ASSERT_VALUES_EQUAL(ui32(output.Writer.Groups[1].Channel), 4);
        UNIT_ASSERT(output.Layout.Groups[0].Codec == NPage::ECodec::Plain);
        UNIT_ASSERT(output.Layout.Groups[1].Codec == NPage::ECodec::LZ4);
        UNIT_ASSERT(output.Writer.Groups[1].Cache == NPage::ECache::Ever);
        UNIT_ASSERT_VALUES_EQUAL(output.Layout.SmallEdge, 16);
        UNIT_ASSERT_VALUES_EQUAL(output.Layout.LargeEdge, 32);
        UNIT_ASSERT_VALUES_EQUAL(output.MaterializeExtEdge, 32);
        UNIT_ASSERT_VALUES_EQUAL(ui32(output.Writer.OuterChannel), 2);
        UNIT_ASSERT(output.Writer.BlobsChannels == TVector<ui8>{3});
        UNIT_ASSERT(!output.Layout.Layout.SeparateDescriptorCollection);
    }

    Y_UNIT_TEST(RegularColdUsesASingleTierOutput) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->TierId = TOutputConfigFixture::ColdTier;
        conf.Params->KeepInCache = true;
        env.Prepare(conf);

        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 1);
        const auto& output = conf.Outputs[0];
        UNIT_ASSERT_VALUES_EQUAL(output.OutputId, 0);
        UNIT_ASSERT_VALUES_EQUAL(output.TierId, TOutputConfigFixture::ColdTier);
        UNIT_ASSERT(output.Layout.Layout.SeparateDescriptorCollection);
        UNIT_ASSERT(output.Layout.Layout.KeyIndexInDescriptor);
        UNIT_ASSERT(!output.Layout.Layout.GroupIndexesInDescriptor);
        UNIT_ASSERT_VALUES_EQUAL(output.Writer.Groups.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ui32(output.Writer.Groups[0].Channel), 1);
        for (ui32 group = 0; group < 2; ++group) {
            UNIT_ASSERT_VALUES_EQUAL(ui32(output.Writer.Groups[group + 1].Channel), 5);
            UNIT_ASSERT(output.Writer.Groups[group + 1].Cache == NPage::ECache::Once);
            UNIT_ASSERT_VALUES_EQUAL(output.Writer.Groups[group + 1].MaxBlobSize, 256);
            UNIT_ASSERT(output.Layout.Groups[group].Codec == NPage::ECodec::LZ4);
        }
        UNIT_ASSERT_VALUES_EQUAL(output.Layout.SmallEdge, 64);
        UNIT_ASSERT_VALUES_EQUAL(output.Layout.LargeEdge, 128);
        UNIT_ASSERT_VALUES_EQUAL(output.MaterializeExtEdge, 128);
        UNIT_ASSERT_VALUES_EQUAL(ui32(output.Writer.OuterChannel), 6);
        UNIT_ASSERT(output.Writer.BlobsChannels == (TVector<ui8>{7, 8}));
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*env.RowLayout);
    }

    Y_UNIT_TEST(TierAwarePreparesAllDistinctTiers) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Mode = TCompactionParams::EMode::TierAware;
        conf.Params->TierId = TOutputConfigFixture::ColdTier;
        env.Prepare(conf);

        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 3);
        const TVector<ui32> expected = {0, TOutputConfigFixture::ColdTier, TOutputConfigFixture::ArchiveTier};
        for (ui32 i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(conf.Outputs[i].TierId, expected[i]);
            UNIT_ASSERT_VALUES_EQUAL(conf.Outputs[i].OutputId, i);
            UNIT_ASSERT_VALUES_EQUAL(conf.Outputs[i].MaterializeExtEdge, Max<ui32>());
        }
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*env.RowLayout);
        TWriteStats stats;
        UNIT_ASSERT(outputs.Finish(stats).empty());
        UNIT_ASSERT_VALUES_EQUAL(stats.Parts, 0);
    }

    Y_UNIT_TEST(SharedChannelsKeepSeparateTierOutputs) {
        TOutputConfigFixture env;
        env.Table.Families[TOutputConfigFixture::ArchiveTier].Room = TOutputConfigFixture::ColdTier;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Mode = TCompactionParams::EMode::TierAware;
        env.Prepare(conf);

        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ui32(conf.Outputs[1].Writer.Groups[1].Channel), 5);
        UNIT_ASSERT_VALUES_EQUAL(ui32(conf.Outputs[2].Writer.Groups[1].Channel), 5);
        UNIT_ASSERT(conf.Outputs[1].TierId != conf.Outputs[2].TierId);
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*env.RowLayout);
    }

    Y_UNIT_TEST(TierAwareSupportsBTreeV2WithOrWithoutShadow) {
        for (bool shadow : {false, true}) {
            TOutputConfigFixture env;
            env.Settings.WriteBTreeIndexV1 = shadow;
            env.Settings.WriteBTreeIndexV2 = true;
            TCompactCfg conf(MakeHolder<TCompactionParams>());
            conf.Params->Mode = TCompactionParams::EMode::TierAware;
            env.Prepare(conf);
            for (const auto& output : conf.Outputs) {
                UNIT_ASSERT(!output.Layout.WriteFlatIndex);
                UNIT_ASSERT_VALUES_EQUAL(output.Layout.WriteBTreeIndexV1, shadow);
                UNIT_ASSERT(output.Layout.WriteBTreeIndexV2);
            }
            TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
                MakeHolder<TDefaultCompactionRouter>());
            outputs.Start(*env.RowLayout);
        }
    }

    Y_UNIT_TEST(RegularDefaultSupportsLegacyLayoutWithoutBTree) {
        TOutputConfigFixture env;
        env.Settings.WriteBTreeIndexV1 = false;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        env.Prepare(conf);
        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 1);
        UNIT_ASSERT(!conf.Outputs[0].Layout.Layout.SeparateDescriptorCollection);
        UNIT_ASSERT(conf.Outputs[0].Layout.WriteFlatIndex);
        UNIT_ASSERT(conf.Outputs[0].Writer.StickyFlatIndex);
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*env.RowLayout);
    }

    Y_UNIT_TEST(MissingTierFamilyIsRejected) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->TierId = 100;
        UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf), yexception, "Cannot find tier family 100");
    }

    Y_UNIT_TEST(IndexPlacementRequiresSeparateDescriptor) {
        for (bool sameChannel : {false, true}) {
            TOutputConfigFixture env;
            if (sameChannel) {
                env.Table.Rooms[TOutputConfigFixture::ColdTier].Main = 1;
            } else {
                env.Settings.WriteBTreeIndexV1 = false;
            }
            TCompactCfg conf(MakeHolder<TCompactionParams>());
            conf.Params->TierId = TOutputConfigFixture::ColdTier;
            UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf), yexception, "requires a separate descriptor collection");
        }
    }

    Y_UNIT_TEST(TierAwareFulltextIsRejected) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Mode = TCompactionParams::EMode::TierAware;
        conf.IsFulltextCompact = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf), yexception, "Tier-aware fulltext compaction is not supported");
    }

    Y_UNIT_TEST(PreparedContextCreatesIndependentUnderlayCursors) {
        TOutputConfigFixture env;
        const TIntrusiveConstPtr<TColdPart> remaining = new TColdPart(
            TLogoBlobID(1, 1, 1, 1, 0, 0), TEpoch::FromIndex(10));
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Mode = TCompactionParams::EMode::TierAware;
        conf.Params->IsFinal = true;
        conf.Params->Context = TCompactionContext::Build(
            {{remaining, 2, TOutputConfigFixture::ColdTier}}, TVector<TLogoBlobID>{});
        env.Prepare(conf);

        THashSet<NPage::IKeySpace*> masks;
        for (const auto& output : conf.Outputs) {
            UNIT_ASSERT(!output.Layout.Final);
            UNIT_ASSERT(output.OwnedUnderlayMask);
            UNIT_ASSERT(output.Layout.UnderlayMask == output.OwnedUnderlayMask.Get());
            UNIT_ASSERT(masks.insert(output.Layout.UnderlayMask).second);
            const auto key = TCell::Make(ui32(1));
            UNIT_ASSERT(output.Layout.UnderlayMask->HasKey({&key, 1}));
        }
        UNIT_ASSERT_VALUES_EQUAL(masks.size(), 3);
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*env.RowLayout);
    }

    Y_UNIT_TEST(PreparedContextRespectsKeepEraseMarkers) {
        TOutputConfigFixture env;
        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->KeepEraseMarkers = true;
        env.Table.CompactionPolicy = policy;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->IsFinal = true;
        conf.Params->Context = TCompactionContext::Build({}, TVector<TLogoBlobID>{});
        env.Prepare(conf);

        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 1);
        UNIT_ASSERT(!conf.Outputs[0].Layout.Final);
        UNIT_ASSERT(!conf.Outputs[0].Layout.UnderlayMask);
    }

    Y_UNIT_TEST(PreparedContextWithNoRemainingSourcesIsFinal) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Context = TCompactionContext::Build({}, TVector<TLogoBlobID>{});
        env.Prepare(conf);

        UNIT_ASSERT(conf.Outputs[0].Layout.Final);
        UNIT_ASSERT(!conf.Outputs[0].Layout.UnderlayMask);
    }

    Y_UNIT_TEST(UnknownConcurrentMemoryPreservesEraseMarkersInEveryOutput) {
        TOutputConfigFixture env;
        for (const bool withRemaining : {false, true}) {
            TVector<TCompactionPartInfo> remaining;
            if (withRemaining) {
                const TIntrusiveConstPtr<TColdPart> part = new TColdPart(
                    TLogoBlobID(1, 1, 1, 1, 0, 0), TEpoch::FromIndex(10));
                remaining.emplace_back(part, 2, TOutputConfigFixture::ColdTier);
            }
            TCompactCfg conf(MakeHolder<TCompactionParams>());
            conf.Params->Mode = TCompactionParams::EMode::TierAware;
            conf.Params->IsFinal = true;
            conf.Params->Tiering = MakeHolder<TTieringCompaction>();
            conf.Params->Tiering->HasUnknownRemaining = true;
            conf.Params->Context = TCompactionContext::Build(std::move(remaining), TVector<TLogoBlobID>{});
            env.Prepare(conf);

            UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 3);
            for (const auto& output : conf.Outputs) {
                UNIT_ASSERT(!output.Layout.Final);
                UNIT_ASSERT(!output.Layout.UnderlayMask);
                // The preparation fixture has no input rows and therefore an
                // infinite version floor. Give this synthetic erase a valid
                // floor while retaining the prepared garbage-collection rules.
                auto layout = output.Layout;
                layout.MinRowVersion = TRowVersion::Min();
                const auto eggs = NTest::TPartCook(env.RowLayout, layout)
                    .AddOpN(ERowOp::Erase, ui32(20)).Finish();
                UNIT_ASSERT(eggs.Written);
                UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Rows, 1);
                UNIT_ASSERT_VALUES_EQUAL(eggs.Written->Drops, 1);
            }
        }
    }

    Y_UNIT_TEST(PreparedRegularContextCannotChangeItsTier) {
        TOutputConfigFixture env;
        const TIntrusiveConstPtr<TColdPart> part = new TColdPart(
            TLogoBlobID(1, 1, 1, 1, 0, 0), TEpoch::FromIndex(10));
        TSubset subset(TEpoch::Zero(), env.RowLayout.RowScheme());
        subset.ColdParts = {part};
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Epoch = part->Epoch;
        conf.Params->Context = TCompactionContext::Build(
            {{part, 2, TOutputConfigFixture::ColdTier}}, subset);

        UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf, &subset), yexception,
            "Regular compaction must stay within its input tier");
        conf.Params->TierId = TOutputConfigFixture::ColdTier;
        env.Prepare(conf, &subset);
        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(conf.Outputs[0].TierId, TOutputConfigFixture::ColdTier);
    }

    Y_UNIT_TEST(PreparedContextRejectsUnclosedInputsAndWrongWriterEpoch) {
        TOutputConfigFixture env;
        const TIntrusiveConstPtr<TColdPart> part = new TColdPart(
            TLogoBlobID(1, 1, 1, 1, 0, 0), TEpoch::FromIndex(10));
        TSubset subset(TEpoch::Zero(), env.RowLayout.RowScheme());
        subset.ColdParts = {part};
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Context = TCompactionContext::Build({{part, 1, 0}}, subset);
        UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf), yexception,
            "does not cover the closed epoch interval");
        conf.Epoch = TEpoch::FromIndex(11);
        UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf, &subset), yexception,
            "writer epoch differs from its prepared interval");
    }

    Y_UNIT_TEST(PreparedContextFulltextIsRejected) {
        TOutputConfigFixture env;
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Params->Context = TCompactionContext::Build({}, TVector<TLogoBlobID>{});
        conf.IsFulltextCompact = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(env.Prepare(conf), yexception,
            "Prepared compaction context is not supported for fulltext");
    }
}

Y_UNIT_TEST_SUITE(TCompactionOutputsTest) {
    Y_UNIT_TEST(RouteAlternatingKeysWithSharedChannels) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Epoch = TEpoch(i64(7));
        AddOutput(conf, 17);
        AddOutput(conf, 31);
        for (auto& output : conf.Outputs) {
            output.Layout.MainPageCollectionEdge = 512;
        }
        ui32 calls = 0;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TParityRouter>(calls));
        outputs.Start(*Layout());
        THashSet<TLogoBlobID> blobs;
        auto drain = [&] {
            for (const auto& blob : outputs.GetBlobsToSave()) {
                UNIT_ASSERT(blobs.insert(blob.GId.Logo).second);
                UNIT_ASSERT_VALUES_EQUAL(blob.GId.Group, 11);
            }
        };
        for (ui32 key = 0; key < 40; ++key) {
            WriteKey(outputs, key, TString(80, 'a'));
            drain();
        }
        TWriteStats stats;
        const auto results = outputs.Finish(stats);
        drain();
        UNIT_ASSERT_VALUES_EQUAL(calls, 40);
        UNIT_ASSERT_VALUES_EQUAL(stats.Rows, 40);
        UNIT_ASSERT_VALUES_EQUAL(stats.Parts, results.size());
        UNIT_ASSERT(results.size() > 2);
        ui32 keys = 0;
        for (const auto& result : results) {
            keys += CheckKeys(result, true);
            const auto part = LoadPart(result);
            UNIT_ASSERT_VALUES_EQUAL(part.Epoch(), conf.Epoch);
        }
        UNIT_ASSERT_VALUES_EQUAL(keys, 40);
        UNIT_ASSERT(!blobs.empty());
    }

    Y_UNIT_TEST(DefaultRouterLeavesOtherOutputsEmpty) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31, 3);
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*Layout());
        for (ui32 key = 0; key < 4; ++key) {
            WriteKey(outputs, key, "value");
        }
        TWriteStats stats;
        const auto results = outputs.Finish(stats);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(CheckKeys(results[0], false), 4);
        for (const auto& blob : outputs.GetBlobsToSave()) {
            UNIT_ASSERT_VALUES_EQUAL(blob.GId.Logo.Channel(), 1);
        }
    }

    Y_UNIT_TEST(MergedReadWithInterleavedKeyRanges) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        conf.Epoch = TEpoch(i64(7));
        AddOutput(conf, 17);
        AddOutput(conf, 31);
        ui32 calls = 0;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TParityRouter>(calls));
        outputs.Start(*Layout());
        for (ui32 key = 0; key < 40; ++key) {
            WriteKey(outputs, key, "value");
        }
        TWriteStats stats;
        const auto results = outputs.Finish(stats);
        TVector<TPartView> parts;
        TWrittenPages pages;
        for (const auto& result : results) {
            parts.push_back(LoadPart(result));
            pages.Add(parts.back().Part.Get(), result);
        }

        TScheme::TTableInfo scheme("test", 1);
        scheme.Columns.emplace(0, TColumn("key", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), ""));
        scheme.Columns.at(0).KeyOrder = 0;
        scheme.KeyColumns = {0};
        scheme.Columns.emplace(1, TColumn("value", 1, NScheme::TTypeInfo(NScheme::NTypeIds::String), ""));
        TTable table(TEpoch::Zero());
        table.SetScheme(scheme);
        TSubset empty(TEpoch::Zero(), table.GetScheme());
        table.Replace(empty, parts, {});

        const TVector<ui32> tags = {0, 1};
        auto iter = table.Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
        for (ui32 key = 0; key < 40; ++key) {
            UNIT_ASSERT_VALUES_EQUAL(iter->Next(ENext::Data), EReady::Data);
            UNIT_ASSERT_VALUES_EQUAL(iter->GetKey().Cells()[0].AsValue<ui32>(), key);
            UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsBuf(), "value");
        }
        UNIT_ASSERT_VALUES_EQUAL(iter->Next(ENext::Data), EReady::Gone);
    }

    Y_UNIT_TEST(LocksDeltasAndHistoryStayInOneOutput) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31, 3);
        ui32 calls = 0;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TParityRouter>(calls));
        outputs.Start(*Layout());
        for (ui32 key = 0; key < 4; ++key) {
            WriteKey(outputs, key, "value", true);
        }
        TWriteStats stats;
        const auto results = outputs.Finish(stats);
        UNIT_ASSERT_VALUES_EQUAL(calls, 4);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.Rows, 4);
        UNIT_ASSERT_VALUES_EQUAL(stats.HiddenRows, 4);
        for (const auto& result : results) {
            const auto part = LoadPart(result);
            UNIT_ASSERT_VALUES_EQUAL(part->Stat.Rows, 2);
            UNIT_ASSERT_VALUES_EQUAL(part->Stat.HiddenRows, 2);
            UNIT_ASSERT(part->TxIdStats);
            const ui8 channel = result.OutputId == 17 ? 1 : 3;
            UNIT_ASSERT_VALUES_EQUAL(part->Label.Channel(), channel);
        }
    }

    Y_UNIT_TEST(EmptyOutputsCanWriteTransactionStatus) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31, 3);
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.Start(*Layout());
        TWriteStats stats;
        UNIT_ASSERT(outputs.Finish(stats).empty());
        UNIT_ASSERT(outputs.GetBlobsToSave().empty());
        const TString status = "transaction status";
        const auto id = outputs.WriteExtra(status);
        UNIT_ASSERT_VALUES_EQUAL(id.Bytes, status.size());
        UNIT_ASSERT(!outputs.GetBlobsToSave().empty());
    }

    Y_UNIT_TEST(UnfinishedPagesProduceNoFlightBytes) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31);
        for (auto& output : conf.Outputs) {
            output.Layout.Group(0).PageRows = 100;
            output.Writer.Groups[0].MaxBlobSize = 1024 * 1024;
        }
        ui32 calls = 0;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TParityRouter>(calls));
        outputs.Start(*Layout());
        WriteKey(outputs, 0, "value");
        WriteKey(outputs, 1, "value");
        UNIT_ASSERT(outputs.GetBlobsToSave().empty());
        TWriteStats stats;
        UNIT_ASSERT_VALUES_EQUAL(outputs.Finish(stats).size(), 2);
        UNIT_ASSERT(!outputs.GetBlobsToSave().empty());
    }

    Y_UNIT_TEST(MaterializationUsesAllOutputsAndPhysicalBytes) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31);
        for (auto& output : conf.Outputs) {
            output.MaterializeExtEdge = 512;
            output.Layout.LargeEdge = 512;
        }
        conf.Outputs[1].MaterializeExtEdge = 1024;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        UNIT_ASSERT_VALUES_EQUAL(outputs.GetScanLargeEdge(), 1024);
        conf.Outputs[0].MaterializeExtEdge = Max<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(outputs.GetScanLargeEdge(), Max<ui32>());
    }

    Y_UNIT_TEST(MaterializationIsIndependentOfSerialization) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        conf.Outputs[0].MaterializeExtEdge = 512;
        conf.Outputs[0].Layout.LargeEdge = 1024;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        UNIT_ASSERT_VALUES_EQUAL(outputs.GetScanLargeEdge(), 512);
        conf.Outputs[0].Layout.LargeEdge = Max<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(outputs.GetScanLargeEdge(), 512);
        conf.Outputs[0].MaterializeExtEdge = 0;
        UNIT_ASSERT_VALUES_EQUAL(outputs.GetScanLargeEdge(), 0);
    }

    Y_UNIT_TEST(LegacyMaterializationAfterThresholdChange) {
        class TLoadingQueue final : public NFwd::IPageLoadingQueue {
        public:
            ui64 AddToQueue(NPage::TPageOffset, EPage, ui64 size, ui32) override {
                ++Requests;
                return size;
            }

            ui32 Requests = 0;
        };

        for (ui32 size : {503u, 504u, 508u, 511u, 512u}) {
            TCompactCfg conf(MakeHolder<TCompactionParams>());
            AddOutput(conf, 17);
            conf.Outputs[0].Layout.LargeEdge = 256;
            conf.Outputs[0].MaterializeExtEdge = 256;
            TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
                MakeHolder<TDefaultCompactionRouter>());
            outputs.Start(*Layout());
            WriteKey(outputs, 0, TString(size, 'a'));
            TWriteStats stats;
            const auto results = outputs.Finish(stats);
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
            const auto part = LoadPart(results[0]);
            UNIT_ASSERT(part->Blobs);

            // Raising the leader family's large edge must retain the old scan
            // decision: compare the wrapped blob size, not its payload size.
            conf.Outputs[0].Layout.LargeEdge = 512;
            conf.Outputs[0].MaterializeExtEdge = 512;
            UNIT_ASSERT_VALUES_EQUAL(outputs.GetScanLargeEdge(), 512);
            NFwd::TBlobs blobs(part->Large, part.Slices,
                TVector<ui32>(part->Large->Stats().Tags.size(), outputs.GetScanLargeEdge()),
                false, part->Blobs);
            TLoadingQueue queue;
            const auto got = blobs.Get(&queue, NPage::TPageOffset::FromPageIndex(0), EPage::Opaque, 0);
            const bool materialize = size + sizeof(NPage::TLabel) < 512;
            UNIT_ASSERT_VALUES_EQUAL_C(got.Need, materialize, "payload size " << size);
            UNIT_ASSERT_VALUES_EQUAL(queue.Requests, materialize ? 1 : 0);
        }
    }

    Y_UNIT_TEST(IndependentSerializationThresholds) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31, 3);
        conf.Outputs[1].Layout.LargeEdge = 1024;
        ui32 calls = 0;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TParityRouter>(calls));
        outputs.Start(*Layout());
        ui32 key = 0;
        for (ui32 size : {100u, 700u, 1023u, 1024u, 1500u}) {
            const TString value(size, 'a');
            WriteKey(outputs, key++, value);
            WriteKey(outputs, key++, value);
        }
        TWriteStats stats;
        const auto results = outputs.Finish(stats);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        for (const auto& result : results) {
            const auto part = LoadPart(result);
            UNIT_ASSERT(!part->Small);
            if (result.OutputId == 17) {
                UNIT_ASSERT(!part->Blobs);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.OutputId, 31);
                UNIT_ASSERT(part->Blobs);
                UNIT_ASSERT_VALUES_EQUAL(part->Blobs->Total(), 2);
            }
        }
    }

    Y_UNIT_TEST(ChannelSharesAreUpdatedForEveryOutput) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31, 3);
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>());
        outputs.UpdateChannelsShares(3, 0.5f);
        for (const auto& output : conf.Outputs) {
            UNIT_ASSERT_VALUES_EQUAL(output.Writer.ChannelsShares.Shares.at(3),
                NUtil::NormalizeFreeSpaceShare(0.5f));
        }
    }

    Y_UNIT_TEST(RejectDuplicateIdsAndConflictingChannels) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 17);
        const TLogoBlobID mask(1, 2, 3, 0, 0, 0);
        UNIT_ASSERT_EXCEPTION(TCompactionOutputs(mask, conf, MakeHolder<TDefaultCompactionRouter>()), yexception);
        conf.Outputs[1].OutputId = 31;
        conf.Outputs[1].Writer.Slots[0].Group = 99;
        UNIT_ASSERT_EXCEPTION(TCompactionOutputs(mask, conf, MakeHolder<TDefaultCompactionRouter>()), yexception);
    }

    Y_UNIT_TEST(RejectInvalidRouterIndex) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        ui32 calls = 0;
        TCompactionOutputs outputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TParityRouter>(calls));
        outputs.Start(*Layout());
        UNIT_ASSERT_EXCEPTION(WriteKey(outputs, 1, "value"), yexception);
    }

    Y_UNIT_TEST(RejectSharedUnderlayCursors) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31);
        auto mask = MakeCompactionKeySpace(Layout().RowScheme()->Keys,
            TVector<TCompactionPartInfo>{});
        for (auto& output : conf.Outputs) {
            output.Layout.UnderlayMask = mask.Get();
        }
        UNIT_ASSERT_EXCEPTION_CONTAINS(TCompactionOutputs(TLogoBlobID(1, 2, 3, 0, 0, 0),
            conf, MakeHolder<TDefaultCompactionRouter>()), yexception, "stateful underlay cursor");
    }

    Y_UNIT_TEST(RejectMultipleFulltextOutputs) {
        TCompactCfg conf(MakeHolder<TCompactionParams>());
        AddOutput(conf, 17);
        AddOutput(conf, 31);
        conf.IsFulltextCompact = true;
        UNIT_ASSERT_EXCEPTION(TCompactionOutputs(TLogoBlobID(1, 2, 3, 0, 0, 0), conf,
            MakeHolder<TDefaultCompactionRouter>()), yexception);
    }
}

Y_UNIT_TEST_SUITE(TCompactionScanOutputsTest) {
    Y_UNIT_TEST(TierAwareOutlinedValuesUseTheirOwnChannelsAndPreserveMvccHistory) {
        for (bool btreeV2 : {false, true}) {
            for (bool indexInDescriptor : {false, true}) {
                CheckOutlinedTierScan(false, btreeV2, indexInDescriptor);
            }
        }
    }

    Y_UNIT_TEST(TierAwareOutlinedValuesRemainReadableWithSharedChannels) {
        CheckOutlinedTierScan(true, false, true);
        CheckOutlinedTierScan(true, true, true);
    }

    Y_UNIT_TEST(TierAwareWriteErrorAndCancellationDiscardEveryChannelOutput) {
        for (bool cancel : {false, true}) {
            TCompactionScanEnv env;
            auto input = TierScanInput();
            for (ui32 key = 0; key < 12; ++key) {
                AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                    key, ui32(key % 2 ? 100 : 950), OutlinedScanValue(key, false)), {20, 1});
            }
            auto conf = TierScanConfig(input);
            ConfigureOutlinedTierOutputs(*conf, false);
            auto router = TierScanRouter(*conf);
            env.Start(std::move(conf), input, std::move(router));
            env.ConfirmChannel(1);
            env.ConfirmChannel(2);
            env.ConfirmChannel(5);
            UNIT_ASSERT(!env.PendingPuts.empty());
            UNIT_ASSERT(!env.Result);
            if (cancel) {
                env.Runtime.Send(new IEventHandle(env.Driver, env.Owner, new TEvents::TEvPoison));
                env.Drain();
                UNIT_ASSERT(env.Result);
                UNIT_ASSERT_VALUES_EQUAL(env.Result->Get<NOps::TEvResult>()->Status, EStatus::Term);
            } else {
                const auto pending = std::find_if(env.PendingPuts.begin(), env.PendingPuts.end(),
                    [](const auto& ev) {
                        return ev->template Get<TEvBlobStorage::TEvPut>()->Id.Channel() == 3;
                    });
                UNIT_ASSERT(pending != env.PendingPuts.end());
                env.Confirm(pending - env.PendingPuts.begin(), NKikimrProto::ERROR);
            }
            const auto& product = env.Product();
            UNIT_ASSERT(!product.Success);
            UNIT_ASSERT(product.Results.empty());
            UNIT_ASSERT(product.TxStatus.empty());
        }
    }

    Y_UNIT_TEST(NewestCommittedValueSelectsTheTierForAllVersions) {
        TCompactionScanEnv env;
        auto input = TierScanInput();
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(1), ui32(100), TString("old-cold")), {10, 1});
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(1), ui32(950), TString("new-hot")), {20, 1});
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(2), ui32(950), TString("old-hot")), {10, 1});
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(2), ui32(100), TString("new-cold")), {20, 1});
        auto conf = TierScanConfig(input);
        ui32 calls = 0;
        auto router = MakeHolder<TCountingTierRouter>(TierScanRouter(*conf), calls);
        env.Start(std::move(conf), input, std::move(router));
        env.Complete();

        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        TWrittenPages pages;
        THashSet<ui32> keys;
        for (const auto& part : result->Parts) {
            pages.Add(part.Part.Get());
            auto table = ReadTable(*input->Scheme, {&part, 1});
            const TVector<ui32> tags{0, 1, 2};
            auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
            while (iter->Next(ENext::Data) == EReady::Data) {
                const auto key = iter->GetKey().Cells()[0].AsValue<ui32>();
                UNIT_ASSERT(keys.insert(key).second);
                UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(part->Label), key == 1 ? 17 : 31);
                UNIT_ASSERT_VALUES_EQUAL(iter->GetRowVersion(), TRowVersion(20, 1));
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsValue<ui32>(), key == 1 ? 950 : 100);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), key == 1 ? "new-hot" : "new-cold");
                UNIT_ASSERT_VALUES_EQUAL(iter->SkipToRowVersion({10, 1}), EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsValue<ui32>(), key == 1 ? 100 : 950);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), key == 1 ? "old-cold" : "old-hot");
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(calls, 2);
    }

    Y_UNIT_TEST(LocksAndDeltasFollowTheFirstCommittedTierDecision) {
        TCompactionScanEnv env;
        auto input = TierScanInput();
        constexpr ui32 keyCount = 8;
        for (ui32 key = 0; key < keyCount; ++key) {
            AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                key, ui32(950), ScanValue("old", key)), {10, 1});
            AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                key, ui32(100), ScanValue("head", key)), {20, 1});
            AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                key, ui32(950), ScanValue("delta", key)), {Max<ui64>(), 200 + key});
            const auto lockRow = *NTest::TSchemedCookRow(*TierScanLayout()).Col(key);
            input->LockRow(ELockMode::Exclusive,
                NTest::TRowTool(*TierScanLayout()).LookupKey(lockRow), 100 + key);
        }
        auto conf = TierScanConfig(input);
        auto router = TierScanRouter(*conf);
        env.Start(std::move(conf), input, std::move(router));
        env.Complete();

        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        TWrittenPages pages;
        THashSet<ui32> keys;
        for (const auto& part : result->Parts) {
            UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(part->Label), 31);
            pages.Add(part.Part.Get());
            auto table = ReadTable(*input->Scheme, {&part, 1});
            const TVector<ui32> tags{0, 1, 2};
            auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
            while (iter->Next(ENext::Uncommitted) == EReady::Data) {
                const auto key = iter->GetKey().Cells()[0].AsValue<ui32>();
                UNIT_ASSERT(keys.insert(key).second);
                UNIT_ASSERT(iter->IsUncommitted());
                UNIT_ASSERT_VALUES_EQUAL(iter->GetUncommittedTxId(), 200 + key);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsValue<ui32>(), 950);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), ScanValue("delta", key));
                const auto [mode, txId] = iter->GetLockInfo();
                UNIT_ASSERT_VALUES_EQUAL(mode, ELockMode::Exclusive);
                UNIT_ASSERT_VALUES_EQUAL(txId, 100 + key);
                UNIT_ASSERT_VALUES_EQUAL(iter->SkipUncommitted(), EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(iter->GetRowVersion(), TRowVersion(20, 1));
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsValue<ui32>(), 100);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), ScanValue("head", key));
                UNIT_ASSERT_VALUES_EQUAL(iter->SkipToRowVersion({10, 1}), EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsValue<ui32>(), 950);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), ScanValue("old", key));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), keyCount);
    }

    Y_UNIT_TEST(DeltaOnlyAndLockOnlyKeysRetainTheirSourceTier) {
        TCompactionScanEnv env;
        auto input = TierScanInput();
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(1), ui32(100), TString("uncommitted-cold-value")), {Max<ui64>(), 201});
        for (ui32 key : {1, 2}) {
            const auto lockRow = *NTest::TSchemedCookRow(*TierScanLayout()).Col(key);
            input->LockRow(ELockMode::Exclusive,
                NTest::TRowTool(*TierScanLayout()).LookupKey(lockRow), 100 + key);
        }
        auto conf = TierScanConfig(input);
        ui32 calls = 0;
        auto router = MakeHolder<TCountingTierRouter>(TierScanRouter(*conf), calls);
        env.Start(std::move(conf), input, std::move(router));
        env.Complete();

        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        TWrittenPages pages;
        THashSet<ui32> keys;
        for (const auto& part : result->Parts) {
            UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(part->Label), 17);
            pages.Add(part.Part.Get());
            auto table = ReadTable(*input->Scheme, {&part, 1});
            const TVector<ui32> tags{0, 1, 2};
            auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
            while (iter->Next(ENext::Uncommitted) == EReady::Data) {
                const auto key = iter->GetKey().Cells()[0].AsValue<ui32>();
                UNIT_ASSERT(keys.insert(key).second);
                UNIT_ASSERT(iter->IsUncommitted());
                const auto [mode, txId] = iter->GetLockInfo();
                UNIT_ASSERT_VALUES_EQUAL(mode, ELockMode::Exclusive);
                UNIT_ASSERT_VALUES_EQUAL(txId, 100 + key);
                if (key == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(iter->GetUncommittedTxId(), 201);
                    UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), "uncommitted-cold-value");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(key, 2);
                    UNIT_ASSERT(iter->Row() == ERowOp::Absent);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(calls, 2);
    }

    Y_UNIT_TEST(NullAndEraseDoNotRouteUsingAnOlderColdValue) {
        TCompactionScanEnv env;
        auto input = TierScanInput();
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(1), ui32(100), TString("old-cold")), {10, 1});
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(1), nullptr, TString("new-null")), {20, 1});
        AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
            ui32(2), ui32(100), TString("erased-cold")), {10, 1});
        const auto erasedKey = *NTest::TSchemedCookRow(*TierScanLayout()).Col(ui32(2));
        input->Update(ERowOp::Erase, NTest::TRowTool(*TierScanLayout()).LookupKey(erasedKey),
            {}, {}, {20, 1}, nullptr);
        auto conf = TierScanConfig(input);
        auto router = TierScanRouter(*conf);
        env.Start(std::move(conf), input, std::move(router));
        env.Complete();

        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        TWrittenPages pages;
        THashSet<ui32> keys;
        for (const auto& part : result->Parts) {
            UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(part->Label), 17);
            pages.Add(part.Part.Get());
            auto table = ReadTable(*input->Scheme, {&part, 1});
            const TVector<ui32> tags{0, 1, 2};
            auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
            while (iter->Next(ENext::All) == EReady::Data) {
                const auto key = iter->GetKey().Cells()[0].AsValue<ui32>();
                UNIT_ASSERT(keys.insert(key).second);
                if (key == 1) {
                    UNIT_ASSERT(iter->Row().Get(1).IsNull());
                    UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(2).AsBuf(), "new-null");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(key, 2);
                    UNIT_ASSERT(iter->Row() == ERowOp::Erase);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
    }

    Y_UNIT_TEST(TierAwareScanStopsAtTheFirstPossibleIntermediateTier) {
        for (bool intermediate : {false, true}) {
            TCompactionScanEnv env;
            auto input = TierScanInput();
            AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                ui32(1), ui32(100), TString("archive-candidate")), {20, 1});
            TVector<TCompactionPartInfo> remaining;
            if (intermediate) {
                const TIntrusiveConstPtr<TColdPart> unknown = new TColdPart(
                    TLogoBlobID(2, 1, 1, 1, 0, 0), TEpoch::FromIndex(1));
                remaining.emplace_back(unknown, 2, 42);
            }
            auto conf = TierScanConfig(input, std::move(remaining), true);
            auto router = TierScanRouter(*conf, true);
            env.Start(std::move(conf), input, std::move(router));
            env.Complete();
            const auto& product = env.Product();
            UNIT_ASSERT(product.Success);
            const auto result = TProdCompact::MakeCompactionResult(product.Results);
            UNIT_ASSERT_VALUES_EQUAL(result->Parts.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(result->Parts.front()->Label),
                intermediate ? 31 : 73);
        }
    }

    Y_UNIT_TEST(TierAwareRoutingWritesBeforeAllInputKeysAreScanned) {
        TCompactionScanEnv env;
        auto input = TierScanInput();
        constexpr ui32 keyCount = 64;
        for (ui32 key = 0; key < keyCount; ++key) {
            AddMemRow(*input, *NTest::TSchemedCookRow(*TierScanLayout()).Col(
                key, ui32(100), TString(512 * 1024, 'a')), {20, 1});
        }
        auto conf = TierScanConfig(input);
        for (auto& output : conf->Outputs) {
            output.Layout.Group(0).PageRows = 2;
            output.Layout.MainPageCollectionEdge = 512 * 1024;
            output.Writer.Groups[0].MaxBlobSize = 1024 * 1024;
        }
        ui32 calls = 0;
        auto router = MakeHolder<TCountingTierRouter>(TierScanRouter(*conf), calls);
        env.Start(std::move(conf), input, std::move(router));
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);
        UNIT_ASSERT(calls > 0);
        UNIT_ASSERT(calls < keyCount);
        env.Complete();
        UNIT_ASSERT(env.Product().Success);
        UNIT_ASSERT_VALUES_EQUAL(calls, keyCount);
    }

    Y_UNIT_TEST(RouteWholeKeysAndPreserveOutputIds) {
        TCompactionScanEnv env;
        auto conf = ScanConfig();
        for (auto& output : conf->Outputs) {
            output.Layout.MainPageCollectionEdge = 512;
        }
        auto input = ScanInput(40, true);
        ui32 calls = 0;
        env.Start(std::move(conf), input, MakeHolder<TParityRouter>(calls));
        UNIT_ASSERT_VALUES_EQUAL(calls, 40);
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);

        env.ConfirmChannel(1);
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);
        while (env.PendingPuts.size() > 1) {
            env.Confirm();
            UNIT_ASSERT(!env.Result);
        }
        env.Complete();
        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);

        // Exercise the same conversion used to deliver results to the strategy.
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        UNIT_ASSERT_VALUES_EQUAL(result->Epoch, input->Epoch);
        UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.size(), result->Parts.size());
        THashMap<ui32, ui32> partsPerOutput;
        THashSet<ui32> keys;
        TWrittenPages pages;
        for (const auto& part : result->Parts) {
            const auto outputId = result->PartOutputIds.at(part->Label);
            UNIT_ASSERT(outputId == 17 || outputId == 31);
            UNIT_ASSERT_VALUES_EQUAL(ui32(part->Label.Channel()), outputId == 17 ? 1 : 3);
            ++partsPerOutput[outputId];
            pages.Add(part.Part.Get());
            auto table = ReadTable(*input->Scheme, {&part, 1});
            const TVector<ui32> tags{0, 1};
            auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
            while (iter->Next(ENext::Uncommitted) == EReady::Data) {
                const ui32 key = iter->GetKey().Cells()[0].AsValue<ui32>();
                UNIT_ASSERT(key < 40);
                UNIT_ASSERT(keys.insert(key).second);
                UNIT_ASSERT_VALUES_EQUAL(outputId, key % 2 ? 31 : 17);
                UNIT_ASSERT(iter->IsUncommitted());
                UNIT_ASSERT_VALUES_EQUAL(iter->GetUncommittedTxId(), 200 + key);
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsBuf(), ScanValue("delta", key));
                const auto [lockMode, lockTxId] = iter->GetLockInfo();
                UNIT_ASSERT_VALUES_EQUAL(lockMode, ELockMode::Exclusive);
                UNIT_ASSERT_VALUES_EQUAL(lockTxId, 100 + key);

                UNIT_ASSERT_VALUES_EQUAL(iter->SkipUncommitted(), EReady::Data);
                UNIT_ASSERT(!iter->IsUncommitted());
                UNIT_ASSERT_VALUES_EQUAL(iter->GetRowVersion(), TRowVersion(20, 1));
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsBuf(), ScanValue("head", key));
                UNIT_ASSERT_VALUES_EQUAL(iter->SkipToRowVersion({10, 1}), EReady::Data);
                UNIT_ASSERT_VALUES_EQUAL(iter->GetRowVersion(), TRowVersion(10, 1));
                UNIT_ASSERT_VALUES_EQUAL(iter->Row().Get(1).AsBuf(), ScanValue("old", key));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 40);
        UNIT_ASSERT(partsPerOutput.at(17) > 1);
        UNIT_ASSERT(partsPerOutput.at(31) > 1);
        UNIT_ASSERT_VALUES_EQUAL(calls, 40);
    }

    Y_UNIT_TEST(EmptyFirstOutputPreservesTransactionStatus) {
        TCompactionScanEnv env;
        auto conf = ScanConfig();
        auto input = ScanInput(0);
        AddMemRow(*input, *NTest::TSchemedCookRow(*Layout()).Col(ui32(1), ScanValue("head", 1)), {20, 1});
        input->CommitTx(777, {30, 1});
        conf->Frozen.push_back(input);
        ui32 calls = 0;
        env.Start(std::move(conf), input, MakeHolder<TParityRouter>(calls));
        env.Complete();
        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        UNIT_ASSERT_VALUES_EQUAL(result->Parts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(result->Parts[0]->Label), 31);
        UNIT_ASSERT_VALUES_EQUAL(product.TxStatus.size(), 1);
        const auto committed = product.TxStatus[0]->TxStatusPage->GetCommittedItems();
        UNIT_ASSERT_VALUES_EQUAL(committed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(committed[0].GetTxId(), 777);
        UNIT_ASSERT_VALUES_EQUAL(committed[0].GetRowVersion(), TRowVersion(30, 1));
    }

    Y_UNIT_TEST(WriteErrorDiscardsAllOutputs) {
        TCompactionScanEnv env;
        ui32 calls = 0;
        env.Start(ScanConfig(), ScanInput(40), MakeHolder<TParityRouter>(calls));
        env.ConfirmChannel(1);
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);
        env.Confirm(0, NKikimrProto::ERROR);
        const auto& product = env.Product();
        UNIT_ASSERT(!product.Success);
        UNIT_ASSERT(product.Results.empty());
    }

    Y_UNIT_TEST(CancellationDiscardsAllOutputs) {
        TCompactionScanEnv env;
        ui32 calls = 0;
        env.Start(ScanConfig(), ScanInput(40), MakeHolder<TParityRouter>(calls));
        env.ConfirmChannel(1);
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);
        env.Runtime.Send(new IEventHandle(env.Driver, env.Owner, new TEvents::TEvPoison));
        env.Drain();
        UNIT_ASSERT(env.Result);
        UNIT_ASSERT_VALUES_EQUAL(env.Result->Get<NOps::TEvResult>()->Status, EStatus::Term);
        const auto& product = env.Product();
        UNIT_ASSERT(!product.Success);
        UNIT_ASSERT(product.Results.empty());
    }

    Y_UNIT_TEST(UnfinishedPagesDoNotBlockTheScan) {
        TCompactionScanEnv env;
        auto conf = ScanConfig();
        for (auto& output : conf->Outputs) {
            output.Layout.Group(0).PageSize = 32 * 1024 * 1024;
            output.Layout.Group(0).PageRows = Max<ui32>();
            output.Writer.Groups[0].MaxBlobSize = 8 * 1024 * 1024;
        }
        // More than MaxFlight bytes are buffered across the two unfinished pages.
        // The scan must reach EOF to flush them instead of waiting for nonexistent I/O.
        constexpr ui32 keys = 24;
        ui32 calls = 0;
        env.Start(std::move(conf), ScanInput(keys, false, 1024 * 1024), MakeHolder<TParityRouter>(calls));
        UNIT_ASSERT_VALUES_EQUAL(calls, keys);
        UNIT_ASSERT(!env.PendingPuts.empty());
        UNIT_ASSERT(!env.Result);
        env.Complete();
        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        ui64 rows = 0;
        for (const auto& result : product.Results) {
            rows += result.Part->Stat.Rows;
        }
        UNIT_ASSERT_VALUES_EQUAL(rows, keys);
    }

    Y_UNIT_TEST(FirstBlobMayExceedFlightLimit) {
        TCompactionScanEnv env;
        auto conf = ScanConfig(1);
        auto& output = conf->Outputs.front();
        output.Layout.Group(0).PageSize = 32 * 1024 * 1024;
        output.Layout.Group(0).PageRows = Max<ui32>();
        output.Writer.Groups[0].MaxBlobSize = 32 * 1024 * 1024;
        env.Start(std::move(conf), ScanInput(24, false, 1024 * 1024), MakeHolder<TDefaultCompactionRouter>());
        UNIT_ASSERT_VALUES_EQUAL(env.PendingPuts.size(), 1);
        UNIT_ASSERT(env.PendingPuts.front()->Get<TEvBlobStorage::TEvPut>()->Id.BlobSize() > TOpsCompact::MaxFlight);
        UNIT_ASSERT(!env.Result);
        env.Complete();
        UNIT_ASSERT(env.Product().Success);
    }

    Y_UNIT_TEST(SingleOutputFulltextUsesTheOutputWriter) {
        NTest::TLayoutCook layout;
        layout.Col(0, 1, NScheme::NTypeIds::String)
            .Col(0, 2, NScheme::NTypeIds::Uint64)
            .Col(0, 3, NScheme::NTypeIds::Uint64)
            .Col(0, 4, NScheme::NTypeIds::Bool)
            .Col(0, 5, NScheme::NTypeIds::String)
            .Key({1, 2, 3});
        auto input = MakeIntrusive<TMemTable>(layout.RowScheme(), TEpoch(i64(7)), ui64(0));
        auto segment = [](std::initializer_list<ui64> ids) {
            NFulltext::TDeltaWriter writer;
            writer.Reset(false, false);
            for (ui64 id : ids) {
                writer.Add(id, 1);
            }
            const auto data = writer.GetBuf();
            return TString(reinterpret_cast<const char*>(data.data()), data.size());
        };
        AddMemRow(*input, *NTest::TSchemedCookRow(*layout).Col(
            TString("hello"), ui64(100), ui64(5), true, segment({1, 5})), {50, 1});
        AddMemRow(*input, *NTest::TSchemedCookRow(*layout).Col(
            TString("hello"), ui64(101), ui64(10), true, segment({5, 10})), {80, 1});

        TCompactionScanEnv env;
        auto conf = ScanConfig(1);
        conf->Params->IsFinal = true;
        conf->IsFulltextCompact = true;
        conf->FulltextAddedTag = 4;
        conf->FulltextSegmentTag = 5;
        conf->Outputs[0].OutputId = 73;
        conf->Outputs[0].Layout.Final = true;
        conf->Outputs[0].Layout.MinRowVersion = {100, 1};
        env.Start(std::move(conf), input, MakeHolder<TDefaultCompactionRouter>());
        env.Complete();
        const auto& product = env.Product();
        UNIT_ASSERT(product.Success);
        const auto result = TProdCompact::MakeCompactionResult(product.Results);
        UNIT_ASSERT_VALUES_EQUAL(result->Parts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->PartOutputIds.at(result->Parts[0]->Label), 73);
        TWrittenPages pages;
        pages.Add(result->Parts[0].Part.Get());
        auto table = ReadTable(*input->Scheme, result->Parts);
        const TVector<ui32> tags{1, 2, 3, 4, 5};
        auto iter = table->Iterate(TRawVals{}, tags, &pages, ESeek::Lower, TRowVersion::Max());
        UNIT_ASSERT_VALUES_EQUAL(iter->Next(ENext::Data), EReady::Data);
        const auto key = iter->GetKey().Cells();
        UNIT_ASSERT_VALUES_EQUAL(key[0].AsBuf(), "hello");
        UNIT_ASSERT_VALUES_EQUAL(key[1].AsValue<ui64>(), Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(key[2].AsValue<ui64>(), 10);
        UNIT_ASSERT(iter->Row().Get(3).AsValue<bool>());
        const auto data = iter->Row().Get(4).AsBuf();
        NFulltext::TDeltaReader reader(
            TConstArrayRef<ui8>(reinterpret_cast<const ui8*>(data.data()), data.size()), false, false);
        TVector<ui64> ids;
        ui64 id;
        ui32 frequency;
        while (reader.Read(id, frequency)) {
            ids.push_back(id);
        }
        UNIT_ASSERT(ids == (TVector<ui64>{1, 5, 10}));
        UNIT_ASSERT_VALUES_EQUAL(iter->Next(ENext::Data), EReady::Gone);
    }
}

}

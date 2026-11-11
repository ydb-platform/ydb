#include <ydb/core/tablet_flat/flat_compaction_outputs.h>
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_page_label.h>
#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_ops_compact.h>
#include <ydb/core/tablet_flat/flat_scan_actor.h>
#include <ydb/core/tablet_flat/flat_table.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

#include <algorithm>

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

        ui32 RouteKey(TArrayRef<const TCell> key) override {
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

        TResult Locate(const TPart*, ui64, ELargeObj) override {
            UNIT_FAIL("Unexpected outlined value");
            return {};
        }

        const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location,
                TGroupId groupId) override {
            return &Pages.at(part).at(part->DataSlot(groupId)).at(location.Offset);
        }

        const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location,
                TGroupId groupId) override {
            return &Pages.at(part).at(part->IndexSlot(groupId)).at(location.Offset);
        }

    private:
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
            Runtime.RegisterService(MakeBlobStorageProxyID(11), Storage);
            Runtime.RegisterService(MakeBlobStorageProxyID(13), Storage);

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

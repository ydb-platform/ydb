#include <ydb/core/tablet_flat/flat_compaction_outputs.h>
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_page_label.h>
#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_table.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <library/cpp/testing/unittest/registar.h>

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
        auto writer = outputs.BeginKey({&keyCell, 1});
        TRowState row(2);
        row.Touch(ERowOp::Upsert);
        row.Set(0, ECellOp::Set, keyCell);
        row.Set(1, ECellOp::Set, TCell(value.data(), value.size()));
        if (withHistory) {
            writer->AddKeyLock(ELockMode::Exclusive, 100 + key);
            writer->AddKeyDelta(row, 200 + key);
        }
        writer->AddKeyVersion(row, TRowVersion(20, 1));
        if (withHistory) {
            writer->AddKeyVersion(row, TRowVersion(10, 1));
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

}

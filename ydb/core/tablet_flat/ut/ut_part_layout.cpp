#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_part_writer.h>
#include <ydb/core/tablet_flat/flat_writer_bundle.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NTable {

namespace {
    using namespace NTest;

    enum class EIndex {
        V1,
        V2,
        V2WithV1Shadow,
    };

    const TVector<TPartLayout> SeparateLayouts = {
        { .SeparateDescriptorCollection = true, .KeyIndexInDescriptor = false, .GroupIndexesInDescriptor = false },
        { .SeparateDescriptorCollection = true, .GroupIndexesInDescriptor = false },
        { .SeparateDescriptorCollection = true, .KeyIndexInDescriptor = false },
        { .SeparateDescriptorCollection = true },
    };

    TString Describe(const TPartLayout& layout, EIndex index) {
        TStringBuilder out;
        out << "Layout{";
        if (layout.SeparateDescriptorCollection) {
            out << " SeparateDescriptorCollection";
        }
        if (!layout.KeyIndexInDescriptor) {
            out << " !KeyIndexInDescriptor";
        }
        if (!layout.GroupIndexesInDescriptor) {
            out << " !GroupIndexesInDescriptor";
        }
        out << " } " << (index == EIndex::V1 ? "V1" : index == EIndex::V2 ? "V2" : "V2WithV1Shadow");
        return out;
    }

    void SetIndex(NPage::TConf& conf, EIndex index) {
        conf.WriteFlatIndex = false;
        conf.WriteBTreeIndexV1 = index != EIndex::V2;
        conf.WriteBTreeIndexV2 = index != EIndex::V1;
    }

    NPage::TConf MassConf(size_t groups, const TPartLayout& layout, EIndex index) {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).BTreeIndexNodeTargetSize = 512; /* Should cover up/down moves */
        }
        conf.SmallEdge = 19;  /* Packed to page collection large cell values */
        conf.LargeEdge = 29;  /* Large values placed to single blobs */
        conf.SliceSize = conf.Group(0).PageSize * 4;
        conf.Layout = layout;
        SetIndex(conf, index);

        return conf;
    }

    const TMass& Mass() {
        static const TMass mass(new TModelStd(true), 6000);
        return mass;
    }

    TPartEggs MassEggs(const TPartLayout& layout, EIndex index) {
        return TPartCook::Make(Mass(), MassConf(Mass().Model->Scheme->Families.size(), layout, index));
    }

    // Keys with several versions to get historic groups with multi-level b-tree indexes
    const ui32 HistoryKeys = 200;

    ui32 VersionsCount(ui32 key) {
        return 1 + key % 3;
    }

    const TLayoutCook& HistoryLayout() {
        static const TLayoutCook lay = [] {
            TLayoutCook lay;
            lay
                .Col(0, 0, NScheme::NTypeIds::Uint32)
                .Col(0, 1, NScheme::NTypeIds::Uint64)
                .Col(1, 2, NScheme::NTypeIds::Uint64)
                .Col(2, 3, NScheme::NTypeIds::String)
                .Key({ 0 });
            return lay;
        }();
        return lay;
    }

    TRow HistoryRow(ui32 key, ui64 version) {
        return *TSchemedCookRow(*HistoryLayout()).Col(key, ui64(key) * 10 + version, version,
            TString("v") + ToString(version) + "_" + ToString(key));
    }

    TPartEggs HistoryEggs(const TPartLayout& layout, EIndex index) {
        NPage::TConf conf;
        conf.Groups.resize(3);
        for (ui32 group : xrange(3)) {
            conf.Group(group).PageRows = 2;
            conf.Group(group).BTreeIndexNodeKeysMin = conf.Group(group).BTreeIndexNodeKeysMax = 2;
        }
        conf.Layout = layout;
        SetIndex(conf, index);

        TPartCook cook(HistoryLayout(), conf);
        for (ui32 key : xrange(HistoryKeys)) {
            for (ui64 version = VersionsCount(key); version > 0; version--) {
                cook.Ver({ 0, version }).Add(HistoryRow(key, version));
            }
        }

        return cook.Finish();
    }

    bool IsDescriptorPage(EPage type) {
        return type == EPage::Schem2 || type == EPage::Frames || type == EPage::Globs || type == EPage::Bloom
            || type == EPage::GarbageStats || type == EPage::TxIdStats;
    }

    struct TRoomPages {
        ui32 Data = 0;
        ui32 Index = 0;
        ui32 Descriptor = 0;
    };

    TVector<TRoomPages> CountRoomPages(const NTest::TPartStore& part) {
        const auto& store = *part.Store;
        TVector<TRoomPages> rooms(store.GetOuterRoom());
        for (ui32 room : xrange(store.GetOuterRoom())) {
            for (TPageId pageId : xrange(store.PageCollectionPagesCount(room))) {
                auto type = store.GetPageType(room, pageId);
                if (type == EPage::DataPage) {
                    rooms[room].Data++;
                } else if (NPage::IsIndexPage(type)) {
                    rooms[room].Index++;
                } else if (IsDescriptorPage(type)) {
                    rooms[room].Descriptor++;
                } else {
                    UNIT_FAIL("Unexpected page type " << type << " in room " << room);
                }
            }
        }
        return rooms;
    }

    bool HasIndexLevels(const TPart& part, NPage::TGroupId groupId) {
        const auto& metas = groupId.IsHistoric() ? part.IndexPages.BTreeHistoric : part.IndexPages.BTreeGroups;
        return groupId.Index < metas.size() && metas[groupId.Index].LevelCount() > 0;
    }

    // Checks that every kind of pages is written to the page collection chosen by the part layout
    void CheckPlacement(const TPartEggs& eggs, const TPartEggs& oldEggs, const TPartLayout& layout, const TString& message) {
        const auto& part = *eggs.Lone();
        const auto& oldPart = *oldEggs.Lone();

        UNIT_ASSERT_C(part.Slots.GetLayout() == layout, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part.GroupsCount, oldPart.GroupsCount, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part.Store->GetOuterRoom(), part.GroupsCount + 1, message);

        const auto rooms = CountRoomPages(part);
        const auto oldRooms = CountRoomPages(oldPart);

        // Descriptor pages are only in the first page collection, along with indexes kept in it
        UNIT_ASSERT_VALUES_EQUAL_C(rooms[0].Data, 0, message);
        UNIT_ASSERT_VALUES_EQUAL_C(rooms[0].Descriptor, oldRooms[0].Descriptor, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part.Store->GetPageType(0, part.Store->PageCollectionPagesCount(0) - 1), EPage::Schem2, message);

        ui32 indexPages = rooms[0].Index;
        bool hasDescriptorIndexLevels = false;
        for (ui32 group : xrange(part.GroupsCount)) {
            const auto& room = rooms[group + 1];
            const bool inDescriptor = group == 0 ? layout.KeyIndexInDescriptor : layout.GroupIndexesInDescriptor;
            const bool hasLevels = HasIndexLevels(part, NPage::TGroupId(group)) || HasIndexLevels(part, NPage::TGroupId(group, true));

            UNIT_ASSERT_VALUES_EQUAL_C(part.DataSlot(NPage::TGroupId(group)), group + 1, message);
            UNIT_ASSERT_VALUES_EQUAL_C(part.DataSlot(NPage::TGroupId(group, true)), group + 1, message);
            UNIT_ASSERT_VALUES_EQUAL_C(part.IndexSlot(NPage::TGroupId(group)), inDescriptor ? 0 : group + 1, message);
            UNIT_ASSERT_VALUES_EQUAL_C(part.IndexSlot(NPage::TGroupId(group, true)), inDescriptor ? 0 : group + 1, message);

            UNIT_ASSERT_VALUES_EQUAL_C(room.Data, oldRooms[group].Data, message << " group " << group);
            UNIT_ASSERT_VALUES_EQUAL_C(room.Descriptor, 0, message << " group " << group);
            if (inDescriptor) {
                UNIT_ASSERT_VALUES_EQUAL_C(room.Index, 0, message << " group " << group);
                hasDescriptorIndexLevels |= hasLevels;
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(room.Index > 0, hasLevels, message << " group " << group);
            }
            indexPages += room.Index;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(rooms[0].Index > 0, hasDescriptorIndexLevels, message);
        UNIT_ASSERT_VALUES_EQUAL_C(indexPages, oldRooms[0].Index, message);

        // Parts with a separate descriptor page collection cannot be read by older versions
        NProto::TRoot root;
        TMemoryInput stream(part.Store->GetMeta()->data(), part.Store->GetMeta()->size());
        UNIT_ASSERT(root.ParseFromArcadiaStream(&stream));
        UNIT_ASSERT_VALUES_EQUAL_C(root.GetEvol().GetTail(), 31, message);
    }

    // Reads every version of every key, historic versions are read through historic groups
    void CheckHistoryReads(const TPartEggs& eggs, TAutoPtr<IPages> env, ui64 retries) {
        TCheckIter wrap(eggs, { env, retries });

        ui32 step = 0;
        for (ui32 key : xrange(HistoryKeys)) {
            const ui64 newest = VersionsCount(key);
            wrap.To(++step).Seek(HistoryRow(key, newest), ESeek::Exact).Is(HistoryRow(key, newest)).IsVer({ 0, newest });
            for (ui64 version = newest - 1; version > 0; version--) {
                wrap.To(++step).Ver({ 0, version }).Is(HistoryRow(key, version)).IsVer({ 0, version });
            }
        }
    }

    TAutoPtr<IPages> FailEnv() {
        static TMersenne<ui64> rnd(666);
        return new TFailEnv<TMersenne<ui64>>(rnd, 0.25);
    }
}

Y_UNIT_TEST_SUITE(TPartLayout) {

    Y_UNIT_TEST(Placement) {
        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            const auto oldMass = MassEggs({ }, index);
            const auto oldHistory = HistoryEggs({ }, index);

            const auto& oldPart = *oldMass.Lone();
            UNIT_ASSERT(oldPart.Slots.GetLayout() == TPartLayout{ });
            UNIT_ASSERT_VALUES_EQUAL(oldPart.Store->GetOuterRoom(), oldPart.GroupsCount);

            for (const auto& layout : SeparateLayouts) {
                const auto message = Describe(layout, index);
                CheckPlacement(MassEggs(layout, index), oldMass, layout, "Mass " + message);
                CheckPlacement(HistoryEggs(layout, index), oldHistory, layout, "History " + message);
            }
        }
    }

    Y_UNIT_TEST(Wreck) {
        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            for (const auto& layout : SeparateLayouts) {
                Cerr << "Checking " << Describe(layout, index) << Endl;

                const auto eggs = MassEggs(layout, index);
                for (EWreck wreck : { EWreck::Cached, EWreck::Evicted, EWreck::Forward }) {
                    TWreck<TCheckIter, TPartEggs>(Mass(), 666).Do(wreck, eggs);
                }
            }
        }
    }

    Y_UNIT_TEST(History) {
        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            for (const auto& layout : SeparateLayouts) {
                const auto message = Describe(layout, index);
                Cerr << "Checking " << message << Endl;

                const auto eggs = HistoryEggs(layout, index);
                CheckHistoryReads(eggs, new TTestEnv, 0);
                CheckHistoryReads(eggs, FailEnv(), 66);
            }
        }
    }

    Y_UNIT_TEST(IndexesOutOfDescriptorRequireSeparateDescriptor) {
        for (TPartLayout layout : { TPartLayout{ .KeyIndexInDescriptor = false }, TPartLayout{ .GroupIndexesInDescriptor = false } }) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(MassEggs(layout, EIndex::V1), yexception,
                "Indexes out of the descriptor require a separate descriptor page collection");
        }
    }

    Y_UNIT_TEST(SeparateDescriptorRequiresBTreeIndexOnly) {
        auto conf = MassConf(Mass().Model->Scheme->Families.size(), SeparateLayouts[0], EIndex::V1);
        conf.WriteFlatIndex = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(TPartCook::Make(Mass(), conf), yexception,
            "Separate descriptor page collection requires b-tree index only");
    }
}

namespace {
    using namespace NTabletFlatExecutor;

    // Part written by the production page collections writer
    struct TBundlePart {
        TIntrusiveConstPtr<TRowScheme> Scheme;
        NWriter::TBundle::TResult Result;
        TVector<TRow> Rows; // newest versions of keys, in key order
    };

    // Forwards pages to the bundle and patches the root of the part, to get parts the writer refuses to write
    class TRootPatcher : public IPageWriter {
    public:
        TRootPatcher(IPageWriter& pager, std::function<void(NProto::TRoot&)> patch)
            : Pager(pager)
            , Patch(std::move(patch))
        { }

        TPageLocation Write(TSharedData page, EPage type, ui32 group) override {
            return Pager.Write(std::move(page), type, group);
        }

        TPageId WriteOuter(TSharedData page) override {
            return Pager.WriteOuter(std::move(page));
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body) override {
            NProto::TRoot root;
            Y_ENSURE(root.ParseFromArray(body.data(), body.size()));
            Patch(root);
            Pager.WriteInplace(page, root.SerializeAsString());
        }

        NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) override {
            return Pager.WriteLarge(std::move(blob), ref);
        }

        void Finish(TString overlay) override {
            Pager.Finish(std::move(overlay));
        }

        ui32 GetLastWrittenPageId(ui32 group) const noexcept override {
            return Pager.GetLastWrittenPageId(group);
        }

    private:
        IPageWriter& Pager;
        const std::function<void(NProto::TRoot&)> Patch;
    };

    TBundlePart WriteBundlePart(const TPartLayout& layout, EIndex index, std::function<void(NProto::TRoot&)> patchRoot = { }) {
        const auto& lay = HistoryLayout();

        NPage::TConf conf;
        conf.Groups.resize(3);
        for (ui32 group : xrange(3)) {
            conf.Group(group).PageRows = 2;
            conf.Group(group).BTreeIndexNodeKeysMin = conf.Group(group).BTreeIndexNodeKeysMax = 2;
        }
        // Values of keys 10..99 are packed to the outer page collection, values of keys 100..199 are external blobs
        conf.SmallEdge = 5;
        conf.LargeEdge = 6;
        conf.Layout = layout;
        SetIndex(conf, index);

        const TPartSlots slots(conf.Groups.size(), layout);

        NWriter::TConf writerConf;
        writerConf.Slots = { { 1, 11 }, { 2, 12 }, { 3, 13 } };
        writerConf.Groups.resize(slots.SlotsCount());
        for (ui32 slot : xrange(slots.SlotsCount())) {
            // Descriptor pages go to channel 1, group pages to channel 2
            writerConf.Groups[slot].Channel = layout.SeparateDescriptorCollection && slot != slots.DescriptorSlot() ? 2 : 1;
            writerConf.Groups[slot].Cache = NPage::ECache::Once; /* Keep all data pages in results */
        }
        writerConf.BlobsChannels = { 3 };
        writerConf.OuterChannel = 3;
        writerConf.ChannelsShares = NUtil::TChannelsShares({ { 3, 1.0f } });
        writerConf.WriteBTreeIndexV1 = conf.WriteBTreeIndexV1;
        writerConf.WriteBTreeIndexV2 = conf.WriteBTreeIndexV2;
        writerConf.StickyFlatIndex = false;

        NWriter::TBundle bundle(TLogoBlobID(1, 3, 7, 0, 0, 0), writerConf);

        TBundlePart result;
        result.Scheme = lay.RowScheme();

        const auto tags = result.Scheme->Tags();
        std::optional<TRootPatcher> patcher;
        if (patchRoot) {
            patcher.emplace(bundle, std::move(patchRoot));
        }
        TPartWriter writer(new TPartScheme(result.Scheme->Cols), tags,
            patcher ? static_cast<IPageWriter&>(*patcher) : static_cast<IPageWriter&>(bundle), conf, TEpoch::Zero());

        auto toState = [&](const TRow& row) {
            TRowState state(tags.size());
            state.Touch(ERowOp::Upsert);
            for (const auto& value : *row) {
                auto* info = TRowTool(*result.Scheme).ColFor(value);
                if (!info->IsKey()) {
                    state.Set(std::find(tags.begin(), tags.end(), value.Tag) - tags.begin(), value.Op, value.Cell);
                }
            }
            return state;
        };

        for (ui32 key : xrange(HistoryKeys)) {
            const TCell keyCell = TCell::Make(key);
            writer.BeginKey({ &keyCell, 1 });
            for (ui64 version = VersionsCount(key); version > 0; version--) {
                writer.AddKeyVersion(toState(HistoryRow(key, version)), TRowVersion(0, version));
            }
            writer.EndKey();
            result.Rows.push_back(HistoryRow(key, VersionsCount(key)));
        }
        writer.Finish();

        auto results = bundle.Results();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        result.Result = std::move(results[0]);
        return result;
    }

    struct TLoadStats {
        ui32 Fetches = 0;
        TSet<ui32> FetchedSlots;
    };

    // Loads the part from page collections without any cached pages, serving fetches from the written pages
    TPartView LoadBundlePart(const TBundlePart& bundlePart, bool withSlices, bool preloadData, TLoadStats& stats, ui32 dropSlots = 0) {
        const auto& result = bundlePart.Result;

        TVector<THashMap<NPage::TPageOffset, TSharedData>> pages(result.PageCollections.size());
        TVector<TIntrusivePtr<NTable::TLoader::TPageCollection>> collections;
        for (ui32 slot : xrange(result.PageCollections.size())) {
            const auto& written = result.PageCollections[slot];
            for (const auto* loaded : { &written.RegularPages, &written.StickyPages }) {
                for (const auto& page : *loaded) {
                    pages[slot][page.Location.Offset] = page.Data;
                }
            }
            collections.push_back(MakeIntrusive<NTable::TLoader::TPageCollection>(written.PageCollection));
        }
        collections.resize(collections.size() - dropSlots);

        TPartComponents components{
            .PageCollectionComponents = TVector<TPageCollectionComponents>(collections.size()),
            .Opaque = withSlices ? result.Overlay : TString(),
            .Epoch = TEpoch::Max(),
        };

        NTable::TLoader loader(std::move(components), std::move(collections));

        while (auto fetch = loader.Run({ .PreloadIndex = true, .PreloadData = preloadData })) {
            stats.Fetches++;

            ui32 slot = Max<ui32>();
            for (ui32 i : xrange(result.PageCollections.size())) {
                if (result.PageCollections[i].PageCollection->Label() == fetch.PageCollection->Label()) {
                    slot = i;
                }
            }
            UNIT_ASSERT_C(slot != Max<ui32>(), "Fetch from an unknown page collection " << fetch.PageCollection->Label());
            stats.FetchedSlots.insert(slot);

            TVector<NSharedCache::TEvResult::TLoaded> loaded;
            for (const auto& location : fetch.Pages) {
                auto* data = pages[slot].FindPtr(location.Offset);
                UNIT_ASSERT_C(data, "Fetch of an unknown page " << location << " of page collection " << slot);
                loaded.emplace_back(location.Offset, location.Size, NSharedCache::TSharedPageRef::MakePrivate(*data));
            }
            loader.Save(std::move(loaded));
        }

        return loader.Result();
    }

    void CheckLoadedPart(const TBundlePart& bundlePart, const TPartView& partView, const TPartLayout& layout, const TString& message) {
        const auto* part = partView.As<NTable::TPartStore>();
        UNIT_ASSERT_C(part, message);
        UNIT_ASSERT_C(part->Slots.GetLayout() == layout, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part->GroupsCount, 3u, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part->HistoricGroupsCount, 3u, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part->Stat.Rows, HistoryKeys, message);

        // Outer values are in the last page collection, frames and globs of large values are in the descriptor
        UNIT_ASSERT_VALUES_EQUAL_C(part->PageCollections.size(), part->Slots.SlotsCount() + 1, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part->OuterSlot(), part->Slots.SlotsCount(), message);
        UNIT_ASSERT_C(part->Small && part->Large && part->Blobs && part->Pseudo, message);

        // Slices are built from the first and the last keys of data pages
        UNIT_ASSERT_C(partView.Slices && partView.Slices->size() == 1, message);
        const auto& keyDefaults = *bundlePart.Scheme->Keys;
        const auto& slice = partView.Slices->front();
        TRowTool tool(*bundlePart.Scheme);
        UNIT_ASSERT_VALUES_EQUAL_C(slice.FirstRowId, 0u, message);
        UNIT_ASSERT_VALUES_EQUAL_C(slice.LastRowId, HistoryKeys - 1, message);
        UNIT_ASSERT_C(CompareTypedCellVectors(slice.FirstKey.GetCells().data(), tool.KeyCells(bundlePart.Rows.front()).data(),
            keyDefaults.Types.data(), keyDefaults.Size()) == 0, message);
        UNIT_ASSERT_C(CompareTypedCellVectors(slice.LastKey.GetCells().data(), tool.KeyCells(bundlePart.Rows.back()).data(),
            keyDefaults.Types.data(), keyDefaults.Size()) == 0, message);
    }
}

Y_UNIT_TEST_SUITE(TPartLayoutLoader) {

    Y_UNIT_TEST(Load) {
        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            TVector<TPartLayout> layouts = SeparateLayouts;
            layouts.insert(layouts.begin(), TPartLayout{ });

            for (const auto& layout : layouts) {
                const auto message = Describe(layout, index);
                Cerr << "Checking " << message << Endl;

                const auto bundlePart = WriteBundlePart(layout, index);
                const TPartSlots slots(3, layout);
                UNIT_ASSERT_VALUES_EQUAL_C(bundlePart.Result.PageCollections.size(), slots.OuterSlot() + 1, message);

                // Descriptor and data pages are written to their channels
                for (ui32 slot : xrange(slots.SlotsCount())) {
                    const ui8 channel = layout.SeparateDescriptorCollection && slot != slots.DescriptorSlot() ? 2 : 1;
                    UNIT_ASSERT_VALUES_EQUAL_C(bundlePart.Result.PageCollections[slot].PageCollection->Label().Channel(), channel, message);
                }

                for (bool withSlices : { true, false }) {
                    for (bool preloadData : { false, true }) {
                        TLoadStats stats;
                        auto partView = LoadBundlePart(bundlePart, withSlices, preloadData, stats);
                        CheckLoadedPart(bundlePart, partView, layout, message);

                        // Only descriptor pages and roots of b-tree indexes are loaded, data and outer pages are not
                        TSet<ui32> expected{ slots.DescriptorSlot() };
                        const auto& indexPages = partView->IndexPages;
                        for (bool historic : { false, true }) {
                            const auto& metas = historic ? indexPages.BTreeHistoric : indexPages.BTreeGroups;
                            for (ui32 group : xrange(metas.size())) {
                                if (metas[group].LevelCount()) {
                                    expected.insert(slots.IndexSlot(NPage::TGroupId(group, historic)));
                                }
                            }
                        }
                        const auto loadMessage = TStringBuilder() << message << (withSlices ? " with slices" : "")
                            << (preloadData ? " preload data" : "") << ", fetched " << JoinSeq(",", stats.FetchedSlots);
                        if (withSlices && !preloadData) {
                            UNIT_ASSERT_C(stats.FetchedSlots == expected, loadMessage);
                        } else {
                            // Keys of slices and preloaded data are read from data pages of the main group and its index
                            UNIT_ASSERT_C(stats.FetchedSlots.contains(slots.DataSlot({ })), loadMessage);
                            expected.insert(slots.DataSlot({ }));
                            expected.insert(slots.IndexSlot({ }));
                            for (ui32 slot : stats.FetchedSlots) {
                                UNIT_ASSERT_C(expected.contains(slot), loadMessage);
                            }
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(Abi) {
        for (const auto& layout : SeparateLayouts) {
            const auto bundlePart = WriteBundlePart(layout, EIndex::V2);
            const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(
                bundlePart.Result.PageCollections[0].PageCollection.Get());
            UNIT_ASSERT(packet);

            const auto& meta = packet->Meta;
            const TPageId scheme = meta.TotalPages() - 1;
            UNIT_ASSERT_VALUES_EQUAL(EPage(meta.GetPageType(scheme)), EPage::Schem2);

            NProto::TRoot root;
            auto inplace = meta.GetPageInplaceData(scheme);
            UNIT_ASSERT(root.ParseFromArray(inplace.data(), inplace.size()));
            UNIT_ASSERT_VALUES_EQUAL(root.GetEvol().GetTail(), 31);
            UNIT_ASSERT(root.GetLayout().GetSeparateDescriptorCollection());
            // Written explicitly, so that the default of the proto is not relied upon
            UNIT_ASSERT(root.GetLayout().HasKeyIndexInDescriptor());
            UNIT_ASSERT(root.GetLayout().HasGroupIndexesInDescriptor());
            UNIT_ASSERT_VALUES_EQUAL(root.GetLayout().GetKeyIndexInDescriptor(), layout.KeyIndexInDescriptor);
            UNIT_ASSERT_VALUES_EQUAL(root.GetLayout().GetGroupIndexesInDescriptor(), layout.GroupIndexesInDescriptor);
        }

        const auto oldPart = WriteBundlePart({ }, EIndex::V2);
        const auto* packet = dynamic_cast<const NPageCollection::TPageCollection*>(
            oldPart.Result.PageCollections[0].PageCollection.Get());
        NProto::TRoot root;
        auto inplace = packet->Meta.GetPageInplaceData(packet->Meta.TotalPages() - 1);
        UNIT_ASSERT(root.ParseFromArray(inplace.data(), inplace.size()));
        UNIT_ASSERT_LT(root.GetEvol().GetTail(), 31);
        UNIT_ASSERT(!root.GetLayout().HasSeparateDescriptorCollection());
        UNIT_ASSERT(!root.GetLayout().HasKeyIndexInDescriptor());
        UNIT_ASSERT(!root.GetLayout().HasGroupIndexesInDescriptor());
    }

    Y_UNIT_TEST(WrongPageCollectionsCount) {
        const auto bundlePart = WriteBundlePart(SeparateLayouts[0], EIndex::V2);

        TLoadStats stats;
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadBundlePart(bundlePart, true, false, stats, /* dropSlots */ 1),
            yexception, "invalid layout");
    }

    Y_UNIT_TEST(IndexesOutOfDescriptorRequireSeparateDescriptor) {
        const std::function<void(NProto::TRoot&)> patches[] = {
            [](NProto::TRoot& root) { root.MutableLayout()->SetKeyIndexInDescriptor(false); },
            [](NProto::TRoot& root) { root.MutableLayout()->SetGroupIndexesInDescriptor(false); },
        };
        for (const auto& patch : patches) {
            const auto bundlePart = WriteBundlePart({ }, EIndex::V2, patch);

            TLoadStats stats;
            UNIT_ASSERT_EXCEPTION_CONTAINS(LoadBundlePart(bundlePart, true, false, stats),
                yexception, "invalid layout");
        }
    }
}

namespace {
    NWriter::TConf DescriptorChannelWriterConf(ui8 mainChannel) {
        NWriter::TConf conf;
        conf.Groups.resize(2);
        conf.Groups[0].Channel = mainChannel;
        conf.Groups[0].Cache = NPage::ECache::Ever;
        conf.Groups[0].CacheMode = NPage::ECacheMode::TryKeepInMemory;
        conf.Groups[0].MaxBlobSize = 1234;
        conf.Groups[1].Channel = 5;
        return conf;
    }
}

Y_UNIT_TEST_SUITE(TDescriptorChannel) {

    Y_UNIT_TEST(SameChannel) {
        NPage::TConf layout;
        auto writer = DescriptorChannelWriterConf(1);

        UNIT_ASSERT(!NWriter::ApplyDescriptorChannel(layout, writer, 1));
        UNIT_ASSERT(layout.Layout == TPartLayout{ });
        UNIT_ASSERT(layout.WriteFlatIndex);
        UNIT_ASSERT_VALUES_EQUAL(writer.Groups.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ui32(writer.Groups[0].Channel), 1u);
    }

    Y_UNIT_TEST(NoBTreeIndex) {
        NPage::TConf layout;
        layout.WriteBTreeIndexV1 = false;
        layout.WriteBTreeIndexV2 = false;
        auto writer = DescriptorChannelWriterConf(2);

        // Flat index pages are sticky, so they would be loaded from the other channel on every part load
        UNIT_ASSERT(!NWriter::ApplyDescriptorChannel(layout, writer, 1));
        UNIT_ASSERT(layout.Layout == TPartLayout{ });
        UNIT_ASSERT(layout.WriteFlatIndex);
        UNIT_ASSERT_VALUES_EQUAL(writer.Groups.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ui32(writer.Groups[0].Channel), 2u);
    }

    Y_UNIT_TEST(OtherChannel) {
        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            NPage::TConf layout;
            layout.WriteBTreeIndexV1 = index != EIndex::V2;
            layout.WriteBTreeIndexV2 = index != EIndex::V1;
            layout.WriteFlatIndex = index == EIndex::V1;
            layout.Layout.GroupIndexesInDescriptor = false;
            auto writer = DescriptorChannelWriterConf(2);

            UNIT_ASSERT(NWriter::ApplyDescriptorChannel(layout, writer, 1));

            // Index placement flags are kept as configured
            UNIT_ASSERT(layout.Layout == (TPartLayout{ .SeparateDescriptorCollection = true, .GroupIndexesInDescriptor = false }));
            UNIT_ASSERT(!layout.WriteFlatIndex);
            UNIT_ASSERT_VALUES_EQUAL(layout.WriteBTreeIndexV1, index != EIndex::V2);
            UNIT_ASSERT_VALUES_EQUAL(layout.WriteBTreeIndexV2, index != EIndex::V1);

            // The descriptor page collection goes first, with settings of the main group in the descriptor channel
            UNIT_ASSERT_VALUES_EQUAL(writer.Groups.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(ui32(writer.Groups[0].Channel), 1u);
            UNIT_ASSERT(writer.Groups[0].Cache == NPage::ECache::Ever);
            UNIT_ASSERT(writer.Groups[0].CacheMode == NPage::ECacheMode::TryKeepInMemory);
            UNIT_ASSERT_VALUES_EQUAL(writer.Groups[0].MaxBlobSize, 1234u);
            UNIT_ASSERT_VALUES_EQUAL(ui32(writer.Groups[1].Channel), 2u);
            UNIT_ASSERT_VALUES_EQUAL(ui32(writer.Groups[2].Channel), 5u);
        }
    }
}

}
}

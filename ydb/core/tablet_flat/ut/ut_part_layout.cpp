#include <ydb/core/tablet_flat/flat_part_loader.h>
#include <ydb/core/tablet_flat/flat_part_index_iter_iface.h>
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

    TString Describe(const TPartLayout& layout) {
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
        out << " }";
        return out;
    }

    TString Describe(const TPartLayout& layout, EIndex index) {
        return TStringBuilder() << Describe(layout) << " "
            << (index == EIndex::V1 ? "V1" : index == EIndex::V2 ? "V2" : "V2WithV1Shadow");
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

    Y_UNIT_TEST(SlotLeaderGroup) {
        TVector<TPartLayout> layouts = SeparateLayouts;
        layouts.push_back({ });
        for (const auto& layout : layouts) {
            const TPartSlots slots(3, layout);
            const auto message = Describe(layout);
            for (ui32 group : xrange(3)) {
                UNIT_ASSERT_VALUES_EQUAL_C(slots.SlotLeaderGroup(slots.DataSlot(NPage::TGroupId(group))), group, message);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(slots.SlotLeaderGroup(slots.DescriptorSlot()), 0u, message);
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
        ui32 CreatedParts = 0;
        ui32 SavedReplies = 0;
    };

    class TTrackedLoaderEnv : public NTable::TLoader::TLoaderEnv {
    public:
        TTrackedLoaderEnv(TVector<TIntrusivePtr<NTable::TLoader::TPageCollection>> collections, TLoadStats& stats)
            : TLoaderEnv(std::move(collections))
            , Stats(stats)
        {}

        NTable::TPartStore* CreatePart(NTable::TPart::TParams params, NTable::TPart::TStat stat) override {
            ++Stats.CreatedParts;
            return TLoaderEnv::CreatePart(std::move(params), stat);
        }

        void Save(NActors::IEventBase& event) override {
            ++Stats.SavedReplies;
            TLoaderEnv::Save(event);
        }

    private:
        TLoadStats& Stats;
    };

    // Serves fetches from the written pages, optionally reusing page collections with cached pages
    TPartView LoadBundlePart(const TBundlePart& bundlePart, bool withSlices, bool preloadData, TLoadStats& stats,
        ui32 dropSlots = 0, const TPartView& cachedPart = {}, bool injectedEnvironment = false,
        bool prepareRootFirst = false) {
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
            collections.push_back(cachedPart ? cachedPart.As<NTable::TPrivatePartStore>()->PageCollections[slot]
                : MakeIntrusive<NTable::TLoader::TPageCollection>(written.PageCollection));
        }
        collections.resize(collections.size() - dropSlots);

        TPartComponents components{
            .PageCollectionComponents = TVector<TPageCollectionComponents>(collections.size()),
            .Opaque = withSlices ? result.Overlay : TString(),
            .Epoch = TEpoch::Max(),
        };

        THolder<NTable::TLoader::IEnv> environment;
        if (injectedEnvironment) {
            environment = MakeHolder<TTrackedLoaderEnv>(std::move(collections), stats);
        } else {
            environment = MakeHolder<NTable::TLoader::TLoaderEnv>(std::move(collections));
        }
        NTable::TLoader loader(std::move(environment), std::move(components));
        if (prepareRootFirst) {
            loader.PrepareRoot();
            loader.PrepareRoot();
        }

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
            NSharedCache::TEvResult reply(std::move(fetch.PageCollection), NKikimrProto::OK, 0);
            reply.Pages = std::move(loaded);
            loader.Save(reply);
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
        UNIT_ASSERT_VALUES_EQUAL_C(part->GetPageCollectionCount(), part->Slots.SlotsCount() + 1, message);
        UNIT_ASSERT_VALUES_EQUAL_C(part->OuterSlot(), part->Slots.SlotsCount(), message);
        UNIT_ASSERT_C(part->Small && part->Large && part->Blobs && part->GetPseudoPageCollection(), message);

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
    Y_UNIT_TEST(LegacyMetadataPhaseOwnsProgressAndCancellation) {
        struct THost {
            void LoadRawMetadata(ui32 slot, const NPageCollection::TLargeGlobId&) {
                Slots.push_back(slot);
            }

            bool SendMetadataRequest(ui32, const void*, THolder<NActors::IEventBase>) {
                UNIT_FAIL("Legacy metadata must use raw reads");
                return false;
            }

            void SendMetadataUpdate(THolder<NActors::IEventBase>) {
                UNIT_FAIL("Legacy metadata must not send cache updates");
            }

            TVector<ui32> Slots;
        };

        const auto bundlePart = WriteBundlePart(SeparateLayouts.front(), EIndex::V2);
        const auto& written = bundlePart.Result.PageCollections;
        UNIT_ASSERT(written.size() > 2);
        TVector<TIntrusivePtr<NTable::TLoader::TPageCollection>> collections(written.size());
        collections[0] = MakeIntrusive<NTable::TLoader::TPageCollection>(written[0].PageCollection);
        TVector<TSharedData> raw;
        TPartComponents components{
            .PageCollectionComponents = TVector<TPageCollectionComponents>(written.size()),
            .Epoch = TEpoch::Max(),
        };
        for (ui32 slot = 0; slot < written.size(); ++slot) {
            const auto& packet =
                *CheckedCast<const NPageCollection::TPageCollection*>(written[slot].PageCollection.Get());
            components.PageCollectionComponents[slot].LargeGlobId = packet.LargeGlobId;
            raw.push_back(packet.Meta.Raw);
        }
        components.PageCollectionComponents[1].RawMeta = raw[1];
        written[0].PageCollection->SetSkipBTreeIndexV1Shadow(true);
        auto environment = MakeHolder<NTable::TLoader::TLoaderEnv>(std::move(collections));
        THost host;
        const TMetadataLoadContext context(host, nullptr, {}, 19, NTabletFlatExecutor::NBlockIO::EPriority::Fast);
        NTable::TLoader loader(std::move(environment), std::move(components));
        loader.BeginMetadata(context);
        UNIT_ASSERT(written[0].PageCollection->SkipBTreeIndexV1Shadow());
        const auto requested = host.Slots;
        UNIT_ASSERT_VALUES_EQUAL(requested.size(), written.size() - 2);
        for (ui32 index = 0; index < requested.size(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(requested[index], index + 2);
        }
        loader.BeginMetadata(context);
        UNIT_ASSERT(host.Slots == requested);
        UNIT_ASSERT(!loader.MetadataReady());
        for (ui32 index = requested.size(); index--;) {
            const auto slot = requested[index];
            const auto result = loader.SaveMetadata(slot, raw[slot]);
            UNIT_ASSERT(result == (index ? EMetadataLoadResult::Pending : EMetadataLoadResult::Ready));
            UNIT_ASSERT(loader.SaveMetadata(slot, raw[slot]) == EMetadataLoadResult::Ignored);
        }
        UNIT_ASSERT(loader.MetadataReady());
        loader.CancelMetadata();
        loader.CancelMetadata();
        loader.BeginMetadata(context);
        UNIT_ASSERT(!loader.MetadataReady());
        UNIT_ASSERT(host.Slots == requested);
        UNIT_ASSERT(loader.SaveMetadata(2, raw[2]) == EMetadataLoadResult::Ignored);
    }

    Y_UNIT_TEST(MetadataRootCallbackAppliesSelectionToLaterSlots) {
        struct THost {
            void LoadRawMetadata(ui32, const NPageCollection::TLargeGlobId&) {
                UNIT_FAIL("This metadata policy uses prepared descriptors");
            }

            bool SendMetadataRequest(ui32, const void*, THolder<NActors::IEventBase>) {
                return true;
            }

            void SendMetadataUpdate(THolder<NActors::IEventBase>) {}
        };

        class TMetadataEnv : public NTable::TLoader::TLoaderEnv {
        public:
            TMetadataEnv(
                TVector<TIntrusivePtr<NTable::TLoader::TPageCollection>> collections, TPageCollectionComponents ready)
                : TLoaderEnv(std::move(collections))
                , Ready(std::move(ready))
            {}

            void BeginMetadata(TArrayRef<TPageCollectionComponents>, const TMetadataLoadContext&,
                TFunctionRef<bool()> prepareRoot) override {
                Outer = prepareRoot();
            }

            EMetadataLoadResult AcceptMetadata(ui32 slot, const NTabletFlatExecutor::TPageCacheMetadataResult&,
                NActors::IEventBase&, const TMetadataLoadContext&, TFunctionRef<bool()> prepareRoot) override {
                PrepareCollection(slot, Ready, false);
                GetCollection(slot)->SetSkipBTreeIndexV1Shadow(true);
                UNIT_ASSERT(prepareRoot() == Outer);
                return EMetadataLoadResult::Pending;
            }

            bool Outer = false;
            TPageCollectionComponents Ready;
        };

        const auto bundlePart = WriteBundlePart(SeparateLayouts.front(), EIndex::V2WithV1Shadow);
        TLoadStats baselineStats;
        const auto baseline = LoadBundlePart(bundlePart, false, false, baselineStats);
        UNIT_ASSERT(baseline);
        const auto& written = bundlePart.Result.PageCollections;
        const auto& packet = *CheckedCast<const NPageCollection::TPageCollection*>(written[1].PageCollection.Get());
        const bool expectedSelection = packet.SkipBTreeIndexV1Shadow();
        TVector<TIntrusivePtr<NTable::TLoader::TPageCollection>> collections(written.size());
        collections[0] = MakeIntrusive<NTable::TLoader::TPageCollection>(written[0].PageCollection);
        written[0].PageCollection->SetSkipBTreeIndexV1Shadow(true);
        auto environment = MakeHolder<TMetadataEnv>(std::move(collections), TPageCollectionComponents{
                                                                                .LargeGlobId = packet.LargeGlobId,
                                                                                .RawMeta = packet.Meta.Raw,
                                                                            });
        auto* metadata = environment.Get();
        TPartComponents components{
            .PageCollectionComponents = TVector<TPageCollectionComponents>(written.size()),
            .Epoch = TEpoch::Max(),
        };
        THost host;
        const TMetadataLoadContext context(host, nullptr, {}, 23, NTabletFlatExecutor::NBlockIO::EPriority::Fast);
        NTable::TLoader loader(std::move(environment), std::move(components));
        loader.BeginMetadata(context);
        UNIT_ASSERT(metadata->Outer);
        UNIT_ASSERT(!metadata->GetCollection(0)->SkipBTreeIndexV1Shadow());
        TEvents::TEvWakeup event;
        const NTabletFlatExecutor::TPageCacheMetadataResult result;
        UNIT_ASSERT(loader.AcceptMetadata(1, result, event, context) == EMetadataLoadResult::Pending);
        UNIT_ASSERT(metadata->GetCollection(1)->SkipBTreeIndexV1Shadow() == expectedSelection);
    }

    Y_UNIT_TEST(PrepareRootBeforeOtherCollectionsArrive) {
        class TRootEnv : public NTable::TLoader::TLoaderEnv {
        public:
            using TLoaderEnv::TLoaderEnv;

            void Initialize(TPartSlots slots) override {
                ++Initialized;
                TLoaderEnv::Initialize(slots);
            }

            void PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer) override {
                ++Prepared;
                TLoaderEnv::PrepareCollection(slot, components, outer);
            }

            ui32 Initialized = 0;
            ui32 Prepared = 0;
        };

        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            const auto bundlePart = WriteBundlePart(SeparateLayouts.front(), index);
            const auto& written = bundlePart.Result.PageCollections;
            UNIT_ASSERT(written.size() > 1);
            TVector<TIntrusivePtr<NTable::TLoader::TPageCollection>> collections(written.size());
            collections[0] = MakeIntrusive<NTable::TLoader::TPageCollection>(written[0].PageCollection);
            auto environment = MakeHolder<TRootEnv>(std::move(collections));
            auto* rootEnv = environment.Get();
            TPartComponents components{
                .PageCollectionComponents = TVector<TPageCollectionComponents>(written.size()),
                .Epoch = TEpoch::Max(),
            };
            NTable::TLoader loader(std::move(environment), std::move(components));
            loader.PrepareRoot();
            UNIT_ASSERT(loader.HasOuterCollection());
            loader.PrepareRoot();
            UNIT_ASSERT(loader.HasOuterCollection());
            UNIT_ASSERT_VALUES_EQUAL(rootEnv->Initialized, 0);
            UNIT_ASSERT_VALUES_EQUAL(rootEnv->Prepared, 0);
            for (ui32 slot = 1; slot < written.size(); ++slot) {
                UNIT_ASSERT(!rootEnv->HasCollection(slot));
            }
        }
    }

    Y_UNIT_TEST(InjectedEnvironmentCreatesPartAndSavesReplies) {
        const auto& layout = SeparateLayouts.front();
        const auto bundlePart = WriteBundlePart(layout, EIndex::V2);
        TLoadStats stats;
        const auto part = LoadBundlePart(bundlePart, true, false, stats, 0, {}, true, true);
        CheckLoadedPart(bundlePart, part, layout, "injected loader environment");
        UNIT_ASSERT_VALUES_EQUAL(stats.CreatedParts, 1);
        UNIT_ASSERT(stats.Fetches > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.SavedReplies, stats.Fetches);
    }

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

    Y_UNIT_TEST(SliceBoundsWithPendingPagesInDifferentSlots) {
        for (EIndex index : { EIndex::V1, EIndex::V2, EIndex::V2WithV1Shadow }) {
            for (const auto& layout : SeparateLayouts) {
                if (!layout.KeyIndexInDescriptor) {
                    continue;
                }
                const auto message = Describe(layout, index);
                Cerr << "Checking " << message << Endl;

                const auto bundlePart = WriteBundlePart(layout, index);
                TLoadStats initialStats;
                const auto cachedPart = LoadBundlePart(bundlePart, true, false, initialStats);
                const auto* part = cachedPart.As<NTable::TPrivatePartStore>();
                const ui32 indexSlot = part->IndexSlot({ });
                const ui32 dataSlot = part->DataSlot({ });
                UNIT_ASSERT_VALUES_UNEQUAL(indexSlot, dataSlot);
                UNIT_ASSERT(!initialStats.FetchedSlots.contains(dataSlot));

                THashMap<NPage::TPageOffset, TSharedData> indexPages;
                const auto& written = bundlePart.Result.PageCollections[indexSlot];
                for (const auto* pages : { &written.RegularPages, &written.StickyPages }) {
                    for (const auto& page : *pages) {
                        indexPages[page.Location.Offset] = page.Data;
                    }
                }

                // Cache only the rightmost index path, without loading any data pages.
                NTable::TLoader::TLoaderEnv env(part->PageCollections, part->Slots);
                env.ProvidePart(part);
                auto iter = CreateIndexIter(part, &env, { });
                ui32 indexFetches = 0;
                for (;;) {
                    const auto ready = iter->SeekLast();
                    if (ready != EReady::Page) {
                        UNIT_ASSERT_VALUES_EQUAL(ready, EReady::Data);
                        break;
                    }
                    auto fetch = env.GetFetch();
                    UNIT_ASSERT(fetch);
                    UNIT_ASSERT_VALUES_EQUAL(fetch.PageCollection->Label(), written.PageCollection->Label());
                    for (const auto& location : fetch.Pages) {
                        auto* data = indexPages.FindPtr(location.Offset);
                        UNIT_ASSERT(data);
                        env.Save({ location.Offset, location.Size, NSharedCache::TSharedPageRef::MakePrivate(*data) });
                    }
                    ++indexFetches;
                }
                UNIT_ASSERT_C(indexFetches > 0, message);
                env.EnsureNoNeedPages();

                // The last key now misses a data page, while the first key misses an index page.
                // A single keys-loader pass queues both slots, but a fetch serves only one of them.
                TLoadStats stats;
                const auto partView = LoadBundlePart(bundlePart, false, false, stats, 0, cachedPart);
                CheckLoadedPart(bundlePart, partView, layout, message);
                UNIT_ASSERT_C(stats.FetchedSlots.contains(indexSlot), message);
                UNIT_ASSERT_C(stats.FetchedSlots.contains(dataSlot), message);
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

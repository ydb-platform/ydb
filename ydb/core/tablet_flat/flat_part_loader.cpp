#include "flat_part_loader.h"
#include "flat_abi_check.h"
#include "flat_part_overlay.h"
#include "flat_part_keys.h"
#include "util_fmt_abort.h"
#include "ydb/core/base/appdata_fwd.h"
#include "ydb/core/base/feature_flags.h"

namespace NKikimr {
namespace NTable {

    TLoader::TLoader(TPartComponents components, TVector<TIntrusivePtr<TPageCollection>> prebuiltPageCollections)
    : TLoader(MakeHolder<TLoaderEnv>(std::move(prebuiltPageCollections)), std::move(components))
{}

TLoader::TLoader(THolder<IEnv> environment, TPartComponents components)
    : Components(std::move(components))
    , Legacy(std::move(Components.Legacy))
    , Opaque(std::move(Components.Opaque))
    , Deltas(std::move(Components.Deltas))
    , Epoch(Components.Epoch)
    , LoaderEnv(std::move(environment))
{
    Y_ENSURE(LoaderEnv);
    if (!LoaderEnv->GetCollectionCount() && Components.PageCollectionComponents.empty()) {
        Y_TABLET_ERROR("Cannot load TPart from " << LoaderEnv->GetCollectionCount() << " page collections");
    }
}

TLoader::~TLoader()
{
    LoaderEnv->CancelMetadata();
}

bool TLoader::BeginMetadata(const TMetadataLoadContext& context)
{
    const auto prepareRoot = [this] {
        return PrepareMetadataRoot();
    };
    return LoaderEnv->BeginMetadata(Components.PageCollectionComponents, context, prepareRoot);
}

EMetadataLoadResult TLoader::SaveMetadata(ui32 slot, TSharedData data)
{
    return LoaderEnv->SaveMetadata(Components.PageCollectionComponents, slot, std::move(data));
}

EMetadataLoadResult TLoader::AcceptMetadata(ui32 slot, const NTabletFlatExecutor::TPageCacheMetadataResult& result,
    NActors::IEventBase& event, const TMetadataLoadContext& context)
{
    const auto prepareRoot = [this] {
        return PrepareMetadataRoot();
    };
    return LoaderEnv->AcceptMetadata(slot, result, event, context, prepareRoot);
}

bool TLoader::PrepareMetadataRoot()
{
    PrepareRoot();
    ApplyIndexSelection();
    return HasOuterCollection();
}

void TLoader::ApplyIndexSelection()
{
    const auto slots = GetSlots();
    for (ui32 slot = 0; slot < LoaderEnv->GetCollectionCount() && slot < slots.SlotsCount(); ++slot) {
        if (!LoaderEnv->HasCollection(slot)) {
            continue;
        }
        LoaderEnv->SetSkipBTreeIndexV1Shadow(slot, StrippedV1ShadowSlots.Get(slot));
    }
}

void TLoader::PrepareRoot()
{
    if (Stage != EStage::Root) {
        return;
    }

    StrippedV1ShadowSlots.Clear();

    if (LoaderEnv->GetCollectionCount() < Components.PageCollectionComponents.size()) {
        LoaderEnv->ResizeCollections(Components.PageCollectionComponents.size());
    }

    // Build slot 0 first — its meta is needed for the full layout parse
    if (LoaderEnv->GetCollectionCount() > 0 && !LoaderEnv->HasCollection(0)) {
        LoaderEnv->PrepareCollection(0, Components.PageCollectionComponents[0], false);
    }

    auto* metaPacket = dynamic_cast<const NPageCollection::TPageCollection*>(LoaderEnv->GetCollection(0).Get());
    if (!metaPacket) {
        Y_TABLET_ERROR("Unexpected IPageCollection type " << TypeName(*LoaderEnv->GetCollection(0)));
    }

    auto &meta = metaPacket->Meta;

    TPageId pageId = meta.TotalPages();

    Y_ENSURE(pageId > 0, "Got page collection without pages");

    if (EPage(meta.Page(pageId - 1).Type) == EPage::Schem2) {
        /* New styled page collection with layout meta. Later root meta will
            be placed in page collection metablob, now it is placed as inplace
            data for EPage::Schem2, have to be the last page.
            */

        Rooted = true, SchemeId = pageId - 1;

        ParseMeta(meta.GetPageInplaceData(SchemeId));

        Y_ENSURE(Root.HasLayout(), "Rooted page collection has no layout");

        if (auto *abi = Root.HasEvol() ? &Root.GetEvol() : nullptr)
            TAbi().Check(abi->GetTail(), abi->GetHead(), "part");

        const auto &layout = Root.GetLayout();

        SchemeId = layout.HasScheme() ? layout.GetScheme() : SchemeId;
        GlobsId = layout.HasGlobs() ? layout.GetGlobs() : GlobsId;
        LargeId = layout.HasLarge() ? layout.GetLarge() : LargeId;
        SmallId = layout.HasSmall() ? layout.GetSmall() : SmallId;
        ByKeyId = layout.HasByKey() ? layout.GetByKey() : ByKeyId;
        ByKeyPrefixMetas.clear();
        for (const auto& meta : layout.GetByKeyPrefixes()) {
            ByKeyPrefixMetas.push_back({meta.GetPageId(), meta.GetPrefixColumns()});
        }
        GarbageStatsId = layout.HasGarbageStats() ? layout.GetGarbageStats() : GarbageStatsId;
        TxIdStatsId = layout.HasTxIdStats() ? layout.GetTxIdStats() : TxIdStatsId;

        PartLayout.SeparateDescriptorCollection = layout.GetSeparateDescriptorCollection();
        PartLayout.KeyIndexInDescriptor = layout.GetKeyIndexInDescriptor();
        PartLayout.GroupIndexesInDescriptor = layout.GetGroupIndexesInDescriptor();

        FlatGroupIndexes.clear();
        FlatHistoricIndexes.clear();
        if (layout.HasIndex() && layout.GetIndex() != Max<TPageId>()) {
            FlatGroupIndexes.push_back(layout.GetIndex());
        }
        for (ui32 id : layout.GetGroupIndexes()) {
            FlatGroupIndexes.push_back(id);
        }
        for (ui32 id : layout.GetHistoricIndexes()) {
            FlatHistoricIndexes.push_back(id);
        }

        auto loadMeta = [](const NProto::TBTreeIndexMeta& proto) -> NPage::TBtreeIndexMeta {
            ui32 lv1 = proto.HasLevelCount() ? proto.GetLevelCount() : Max<ui32>();
            ui32 lv2 = proto.HasLevelCountV2() ? proto.GetLevelCountV2() : Max<ui32>();
            auto v2RootType = (lv2 == 0 ? NPage::EPage::DataPage : NPage::EPage::BTreeIndexV2);
            auto v1Root = proto.HasRootPageId()
                ? proto.GetRootPageId()
                : Max<TPageId>();
            auto v2Root = proto.HasRootOffset()
                ? NPage::TBtreeIndexMeta::RootV2Location(proto.GetRootOffset(), proto.GetRootSize(), proto.GetRootCrc32(), v2RootType)
                : NPage::TPageLocation::Max();
            Y_ENSURE(v1Root != Max<TPageId>() || v2Root, "TBtreeIndexMeta has neither RootPageId nor RootOffset");
            Y_ENSURE(v2Root ? lv2 != Max<ui32>() : lv1 != Max<ui32>(), "TBtreeIndexMeta has no valid level count");

            return { v1Root, v2Root, proto.GetRowCount(), proto.GetDataSize(), proto.GetGroupDataSize(),
                proto.GetErasedRowCount(), lv1, lv2, proto.GetIndexSize() };
        };

        BTreeGroupIndexes.clear();
        BTreeHistoricIndexes.clear();
        if (layout.HasBTreeIndexesFormatVersion() &&
            layout.GetBTreeIndexesFormatVersion() == NPage::TBtreeIndexNode::FormatVersion) {
            for (bool history : {false, true}) {
                for (const auto& meta :
                     history ? layout.GetBTreeHistoricIndexes() : layout.GetBTreeGroupIndexes()) {
                    (history ? BTreeHistoricIndexes : BTreeGroupIndexes).push_back(loadMeta(meta));
                }
            }
        }

        // Offline readers of a page collection have no feature flags to consult, and then whatever
        // index the part holds is kept, the V2 root included.
        if (HasAppData()) {
            if (!AppData()->FeatureFlags.GetEnableLocalDBBtreeIndex() && FlatGroupIndexes) {
                BTreeGroupIndexes.clear();
                BTreeHistoricIndexes.clear();
            }
            if (!AppData()->FeatureFlags.GetEnableLocalDBFlatIndex() && BTreeGroupIndexes) {
                FlatGroupIndexes.clear();
                FlatHistoricIndexes.clear();
            }
        }
        auto forEachIndexMeta = [&](auto&& fn) {
            for (bool historic : {false, true}) {
                auto& metas = historic ? BTreeHistoricIndexes : BTreeGroupIndexes;
                for (ui32 groupIndex : xrange(metas.size())) {
                    fn(metas[groupIndex], NPage::TGroupId(groupIndex, historic));
                }
            }
        };

        if (HasAppData()) {
            if (!AppData()->FeatureFlags.GetEnableLocalDBBtreeIndexV2()) {
                // V2 read disabled: for dual-root (V2+V1) parts, strip RootV2 to use V1 index
                forEachIndexMeta([](auto& meta, NPage::TGroupId) {
                    if (meta.HasRootV2() && meta.HasRootV1()) {
                        meta.RootV2 = NPage::TPageLocation::Max();
                        meta.LevelCountV2 = Max<ui32>();
                    }
                });
                // if no RootV1, keep RootV2 even if disable
            } else {
                // For dual-root (V2+V1) parts, strip V1 tree and mark the index
                // page collection so the shared cache skips dead V1 BTreeIndex pages.
                const auto slots = GetSlots();
                forEachIndexMeta([&](auto& meta, NPage::TGroupId groupId) {
                    if (meta.HasRootV2() && meta.HasRootV1()) {
                        meta.RootV1 = Max<TPageId>();
                        meta.LevelCountV1 = Max<ui32>();
                        const ui32 slot = slots.IndexSlot(groupId);
                        if (slot < slots.SlotsCount()) {
                            StrippedV1ShadowSlots.Set(slot);
                        }
                    }
                });
            }
        }

    } else { /* legacy page collection w/o layout data, (Evolution < 14) */
        do {
            pageId--;
            auto type = EPage(meta.Page(pageId).Type);

            switch (type) {
            case EPage::Scheme: SchemeId = pageId; break;
            case EPage::FlatIndex: FlatGroupIndexes = {pageId}; break;
            /* All special pages have to be placed after the index
                page, hack is required for legacy page collections without
                topology data in metablob.
                */
            case EPage::Frames: LargeId = pageId; break;
            case EPage::Globs: GlobsId = pageId; break;
            default: continue;
            }
        } while (pageId && !HasBasics());

        ParseMeta(meta.GetPageInplaceData(SchemeId));
    }

    MinRowVersion.Step = Root.GetMinRowVersion().GetStep();
    MinRowVersion.TxId = Root.GetMinRowVersion().GetTxId();

    MaxRowVersion.Step = Root.GetMaxRowVersion().GetStep();
    MaxRowVersion.TxId = Root.GetMaxRowVersion().GetTxId();

    Stage = EStage::Meta;
}

void TLoader::StageParseMeta()
{
    const ui32 metaPages = LoaderEnv->GetCollection(0)->MetaPages();
    bool isOuterSlot = HasOuterCollection();

    for (ui32 i = 1; i < LoaderEnv->GetCollectionCount() - isOuterSlot; i++) {
        if (LoaderEnv->HasCollection(i)) {
            continue; // Prebuilt by caller
        }
        LoaderEnv->PrepareCollection(i, Components.PageCollectionComponents[i], false);
    }

    // Construct the outer blob slot if needed
    if (isOuterSlot && !LoaderEnv->HasCollection(LoaderEnv->GetCollectionCount() - 1)) {
        LoaderEnv->PrepareCollection(LoaderEnv->GetCollectionCount() - 1, Components.PageCollectionComponents.back(), true);
    }

    const auto slots = GetSlots();

    if (!HasBasics() || (Rooted && SchemeId != metaPages - 1)
        || (LargeId == Max<TPageId>()) != (GlobsId == Max<TPageId>())
        || (slots.OuterSlot() + (SmallId == Max<TPageId>() ? 0 : 1)) != LoaderEnv->GetCollectionCount()
        || (!PartLayout.SeparateDescriptorCollection && !(PartLayout.KeyIndexInDescriptor && PartLayout.GroupIndexesInDescriptor)))
    {
        Y_TABLET_ERROR("Part " << LoaderEnv->GetCollection(0)->Label() << " has"
            << " invalid layout : " << (Rooted ? "rooted" : "legacy")
            << " " << LoaderEnv->GetCollectionCount() << "s " << metaPages << "pg"
            << ", Scheme " << SchemeId
            << ", FlatIndex " << (FlatGroupIndexes.size() ? FlatGroupIndexes[0] : Max<TPageId>())
            << ", BTreeIndex " << (BTreeGroupIndexes.size() ? BTreeGroupIndexes[0].RootV1PageId() : Max<TPageId>())
            << ", Blobs " << GlobsId << ", Small " << SmallId
            << ", Large " << LargeId << ", ByKey " << ByKeyId
            << ", Garbage " << GarbageStatsId
            << ", TxIdStats " << TxIdStatsId
            << ", SeparateDescriptor " << PartLayout.SeparateDescriptorCollection
            << ", KeyIndexInDescriptor " << PartLayout.KeyIndexInDescriptor
            << ", GroupIndexesInDescriptor " << PartLayout.GroupIndexesInDescriptor);
    }

    // Shared cache skips dead V1 b-tree pages of the page collections with stripped V1 shadows
    ApplyIndexSelection();

    LoaderEnv->Initialize(slots);
}

TLoader::TFetch TLoader::StageCreatePartView(bool preloadIndex)
{
    Y_ENSURE(!PartView, "PartView already initialized in CreatePartView stage");
    Y_ENSURE(LoaderEnv->GetCollectionCount() && LoaderEnv->HasCollection(0));

    const auto slots = GetSlots();

    // Root, bloom, frames, globs and stats pages
    auto getPage = [&](TPageId pageId) {
        const ui32 slot = slots.DescriptorSlot();
        return pageId == Max<TPageId>()
            ? nullptr
            : LoaderEnv->TryGetSlotPage(slot, LoaderEnv->GetCollection(slot)->GetLocation(pageId));
    };
    auto getIndexPage = [&](TPageId pageId, NPage::TGroupId groupId) {
        return pageId == Max<TPageId>()
            ? nullptr
            : LoaderEnv->TryGetIndexPage(nullptr, LoaderEnv->GetCollection(slots.IndexSlot(groupId))->GetLocation(pageId), groupId);
    };
    auto getMetaPage = [&](const NPage::TBtreeIndexMeta& meta, NPage::TGroupId groupId) {
        return meta.HasRootV2() ? LoaderEnv->TryGetIndexPage(nullptr, meta.RootV2, groupId)
                                : getIndexPage(meta.RootV1PageId(), groupId);
    };

    if (BTreeGroupIndexes) {
        if (preloadIndex) {
            // Note: preload root nodes only because we don't want to have multiple restarts here
            for (ui32 groupIndex : xrange(BTreeGroupIndexes.size())) {
                const auto& meta = BTreeGroupIndexes[groupIndex];
                if (meta.LevelCount()) getMetaPage(meta, NPage::TGroupId(groupIndex));
            }
            for (ui32 groupIndex : xrange(BTreeHistoricIndexes.size())) {
                const auto& meta = BTreeHistoricIndexes[groupIndex];
                if (meta.LevelCount()) getMetaPage(meta, NPage::TGroupId(groupIndex, true));
            }
        }
    } else if (FlatGroupIndexes) {
        for (ui32 groupIndex : xrange(FlatGroupIndexes.size())) {
            getIndexPage(FlatGroupIndexes[groupIndex], NPage::TGroupId(groupIndex));
        }
        for (ui32 groupIndex : xrange(FlatHistoricIndexes.size())) {
            getIndexPage(FlatHistoricIndexes[groupIndex], NPage::TGroupId(groupIndex, true));
        }
    }

    for (auto pageId: { SchemeId, GlobsId, SmallId, LargeId, ByKeyId, GarbageStatsId, TxIdStatsId }) {
        Y_DEBUG_ABORT_UNLESS(pageId == Max<TPageId>() || NeedIn(EPage(LoaderEnv->GetCollection(slots.DescriptorSlot())->Page(pageId).Type)));
        getPage(pageId);
    }

    for (const auto& meta : ByKeyPrefixMetas) {
        getPage(meta.PageId);
    }

    if (auto fetch = LoaderEnv->GetFetch()) {
        return fetch;
    }

    auto *scheme = getPage(SchemeId);
    auto *large = getPage(LargeId);
    auto *small = getPage(SmallId);
    auto *blobs = getPage(GlobsId);
    auto *byKey = getPage(ByKeyId);
    auto *garbageStats = getPage(GarbageStatsId);
    auto *txIdStats = getPage(TxIdStatsId);

    if (scheme == nullptr) {
        Y_TABLET_ERROR("Scheme page is not loaded");
    } else if (ByKeyId != Max<TPageId>() && !byKey) {
        Y_TABLET_ERROR("Filter page must be loaded if it exists");
    } else if (small && LoaderEnv->GetCollectionCount() != slots.OuterSlot() + 1) {
        Y_TABLET_ERROR("TPart has small blobs, " << LoaderEnv->GetCollectionCount() << " page collections");
    }

    const auto extra = BlobsLabelFor(LoaderEnv->GetCollection(slots.DescriptorSlot())->Label());

    auto *stat = Root.HasStat() ? &Root.GetStat() : nullptr;

    // Use epoch from metadata unless it has been provided to loader externally
    TEpoch epoch = Epoch != TEpoch::Max() ? Epoch : TEpoch(Root.GetEpoch());

    // TODO: put index size to stat?
    size_t indexesRawSize = 0;
    if (BTreeGroupIndexes) {
        for (const auto &meta : BTreeGroupIndexes) {
            indexesRawSize += meta.IndexSize;
        }
        for (const auto &meta : BTreeHistoricIndexes) {
            indexesRawSize += meta.IndexSize;
        }
        // Note: although we also have flat index, it shouldn't be loaded; so let's not count it here
    } else {
        for (ui32 groupIndex : xrange(FlatGroupIndexes.size())) {
            indexesRawSize += LoaderEnv->GetCollection(slots.IndexSlot(NPage::TGroupId(groupIndex)))->Page(FlatGroupIndexes[groupIndex]).Size;
        }
        for (ui32 groupIndex : xrange(FlatHistoricIndexes.size())) {
            indexesRawSize += LoaderEnv->GetCollection(slots.IndexSlot(NPage::TGroupId(groupIndex, true)))->Page(FlatHistoricIndexes[groupIndex]).Size;
        }
    }

    auto partScheme = TPartScheme::Parse(*scheme, Rooted);

    TVector<std::pair<ui32, TIntrusiveConstPtr<NPage::TBloom>>> byKeyPrefixes;
    for (const auto& meta : ByKeyPrefixMetas) {
        if (meta.PageId != Max<TPageId>()) {
            if (auto* page = getPage(meta.PageId)) {
                byKeyPrefixes.emplace_back(meta.PrefixColumns, new NPage::TBloom(*page));
            }
        }
    }
    // Convert legacy full-key bloom to a prefix entry.
    // Skip if the same page is already referenced in ByKeyPrefixMetas (written by new code for compatibility).
    if (byKey) {
        bool alreadyPresent = std::any_of(ByKeyPrefixMetas.begin(), ByKeyPrefixMetas.end(),
            [&](const auto& meta) { return meta.PageId == ByKeyId; });
        if (!alreadyPresent) {
            ui32 keyCount = partScheme->Groups[0].KeyTypes.size();
            // Appending at the end maintains sorted order because keyCount (full key)
            // is always >= any prefix length in ByKeyPrefixMetas.
            byKeyPrefixes.emplace_back(keyCount, new NPage::TBloom(*byKey));
        }
    }

    auto *partStore = LoaderEnv->CreatePart(
        {
            epoch,
            std::move(partScheme),
            { FlatGroupIndexes, FlatHistoricIndexes, BTreeGroupIndexes, BTreeHistoricIndexes },
            blobs ? new NPage::TExtBlobs(*blobs, extra) : nullptr,
            std::move(byKeyPrefixes),
            large ? new NPage::TFrames(*large) : nullptr,
            small ? new NPage::TFrames(*small) : nullptr,
            indexesRawSize,
            MinRowVersion,
            MaxRowVersion,
            garbageStats ? new NPage::TGarbageStats(*garbageStats) : nullptr,
            txIdStats ? new NPage::TTxIdStatsPage(*txIdStats) : nullptr,
            PartLayout,
        },
        {
            (stat && stat->HasBytes()) ? stat->GetBytes() :
                Root.HasBytes() ? Root.GetBytes() : 0,
            (stat && stat->HasCoded()) ? stat->GetCoded() :
                Root.HasCoded() ? Root.GetCoded() : 0,
            (stat && stat->HasDrops()) ? stat->GetDrops() : 0,
            (stat && stat->HasRows()) ? stat->GetRows() : 0,
            (stat && stat->HasHiddenRows()) ? stat->GetHiddenRows() : 0,
            (stat && stat->HasHiddenDrops()) ? stat->GetHiddenDrops() : 0,
        }
    );

    auto overlay = TOverlay::Decode(Legacy, Opaque);

    PartView = { partStore, std::move(overlay.Screen), std::move(overlay.Slices) };

    LoaderEnv->ProvidePart(PartView.Part.Get());

    return {};
}

TLoader::TFetch TLoader::StageSliceBounds()
{
    Y_ENSURE(PartView, "Cannot generate bounds for a missing part");

    if (PartView.Slices) {
        TOverlay{ PartView.Screen, PartView.Slices }.Validate();
        return {};
    }

    // A keys-loader pass may request pages from several slots, but each fetch serves only one.
    // Drain the remaining requests before running the keys loader again.
    if (auto fetch = LoaderEnv->GetFetch()) {
        return fetch;
    }

    TKeysLoader loader(PartView.Part.Get(), LoaderEnv.Get());

    if (auto run = loader.Do(PartView.Screen)) {
        LoaderEnv->EnsureNoNeedPages(); /* On success there shouldn't be left loads */
        PartView.Slices = std::move(run);
        TOverlay{ PartView.Screen, PartView.Slices }.Validate();

        return {};
    } else if (auto fetches = LoaderEnv->GetFetch()) {
        return fetches;
    } else {
        Y_TABLET_ERROR("Screen keys loader stalled without result");
    }
}

void TLoader::StageDeltas()
{
    Y_ENSURE(PartView, "Cannot apply deltas to a missing part");
    Y_ENSURE(PartView.Slices, "Missing slices in deltas stage");

    for (const TString& rawDelta : Deltas) {
        TOverlay overlay{ std::move(PartView.Screen), std::move(PartView.Slices) };

        overlay.ApplyDelta(rawDelta);

        PartView.Screen = std::move(overlay.Screen);
        PartView.Slices = std::move(overlay.Slices);
    }
}

TLoader::TFetch TLoader::StagePreloadData()
{
    auto partStore = PartView.As<TPartStore>();

    // V2 preload: one index format per part, so walk the current and historic B-trees for room 0 pages.
    if (partStore->IndexPages.HasBTree() && !BTreeGroupIndexes.empty() && BTreeGroupIndexes[0].HasRootV2())
    {
        if (PreloadBTreeWalkers.empty()) {
            auto addWalkers = [&](const auto& indexes, bool historic) {
                for (ui32 groupIndex : xrange(indexes.size())) {
                    if (indexes[groupIndex].HasRootV2()) {
                        auto& item = PreloadBTreeWalkers.emplace_back();
                        item.Walker = MakeHolder<TBTreePartWalker>();
                        item.Walker->Start(indexes[groupIndex]);
                        item.GroupId = NPage::TGroupId(groupIndex, historic);
                        item.SkipDataPages = groupIndex != 0;
                    }
                }
            };

            addWalkers(BTreeGroupIndexes, false);
            addWalkers(BTreeHistoricIndexes, true);
        }

        bool anyMissed = false;
        for (auto& item : PreloadBTreeWalkers) {
            if (!item.Walker) {
                continue;
            }

            if (item.Walker->Step(
                    PartView.Part.Get(), LoaderEnv.Get(), item.GroupId, item.SkipDataPages))
            {
                item.Walker.Reset();
            } else {
                anyMissed = true;
            }
        }

        if (anyMissed) {
            return LoaderEnv->GetFetch();
        }

        // All groups walked.
        PreloadBTreeWalkers.clear();
        return {};
    }

    // V1 path: all pages of the page collections with data and index pages of the main group
    TSet<ui32> slots{ partStore->DataSlot({}), partStore->IndexSlot({}) };
    for (ui32 slot : slots) {
        auto pageCollection = partStore->GetPageCollectionRef(slot);
        auto total = pageCollection->MetaPages();

        for (TPageId pageId : xrange(total)) {
            if (NPageCollection::IsDeadPage(*pageCollection, pageId)) {
                continue;
            }
            LoaderEnv->TryGetSlotPage(slot, pageCollection->GetLocation(pageId));
        }
    }

    return LoaderEnv->GetFetch();
}

void TLoader::Save(NActors::IEventBase& event)
{
    if (Stage == EStage::PartView || Stage == EStage::Slice || Stage == EStage::PreloadData) {
        LoaderEnv->Save(event);
    } else {
        Y_TABLET_ERROR("Unexpected pages save on stage " << int(Stage));
    }
}

bool TLoader::TLoaderEnv::BeginMetadata(
    TArrayRef<TPageCollectionComponents> components, const TMetadataLoadContext& context, TFunctionRef<bool()>)
{
    if (MetadataState != EMetadataState::Idle) {
        return false;
    }
    MetadataState = EMetadataState::Loading;
    if (GetCollectionCount() < components.size()) {
        ResizeCollections(components.size());
    }
    for (ui32 slot = 0; slot < components.size(); ++slot) {
        MetadataRemaining += !HasCollection(slot) && !components[slot].RawMeta;
    }
    for (ui32 slot = 0; slot < components.size(); ++slot) {
        if (!HasCollection(slot) && !components[slot].RawMeta) {
            context.LoadRaw(slot, components[slot].LargeGlobId);
        }
    }
    return true;
}

EMetadataLoadResult TLoader::TLoaderEnv::SaveMetadata(
    TArrayRef<TPageCollectionComponents> components, ui32 slot, TSharedData data)
{
    if (MetadataState != EMetadataState::Loading || MetadataRemaining == 0) {
        return EMetadataLoadResult::Ignored;
    }
    Y_ENSURE(slot < components.size(), "Metadata slot is out of range");
    if (HasCollection(slot) || components[slot].RawMeta) {
        return EMetadataLoadResult::Ignored;
    }
    if (!data) {
        return EMetadataLoadResult::Error;
    }
    components[slot].RawMeta = std::move(data);
    --MetadataRemaining;
    return MetadataRemaining ? EMetadataLoadResult::Pending : EMetadataLoadResult::Ready;
}

void TLoader::TLoaderEnv::PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer)
{
    if (outer) {
        Y_ENSURE(components.RawMeta, "Outer blob slot has no raw meta data");
    } else {
        Y_ENSURE(components.RawMeta, "Slot " << slot << " has no raw meta data");
    }
    auto collection = MakeIntrusive<TPageCollection>(components.MakePageCollection(outer));
    for (auto& page : components.StickyPages) {
        collection->AddStickyPage(
            page.Location.Offset, page.Location.Size, NSharedCache::TSharedPageRef::MakePrivate(std::move(page.Data)));
    }
    for (auto& page : components.RegularPages) {
        collection->AddPage(
            page.Location.Offset, page.Location.Size, NSharedCache::TSharedPageRef::MakePrivate(std::move(page.Data)));
    }
    PageCollections[slot] = std::move(collection);
}

void TLoader::TLoaderEnv::Initialize(TPartSlots slots)
{
    Slots = slots;
    Y_ENSURE(PageCollections.size() >= Slots.SlotsCount());
    RetainedCollections = PageCollections;
    SavedPages.resize(Slots.SlotsCount());
    NeedPages.resize(Slots.SlotsCount());
}

TPartStore* TLoader::TLoaderEnv::CreatePart(TPart::TParams params, TPart::TStat stat)
{
    auto* partStore = new TPrivatePartStore(Collections().front()->PageCollection->Label(), std::move(params), stat);
    partStore->PageCollections = std::move(PageCollections);
    if (partStore->Blobs) {
        Y_ENSURE(partStore->Large, "Cannot use blobs without frames");
        partStore->Pseudo = new TPageCollection(partStore->Blobs);
    }
    return partStore;
}

void TLoader::TLoaderEnv::Save(NActors::IEventBase& event)
{
    auto& result = *CheckedCast<NSharedCache::TEvResult*>(&event);
    for (auto& page : result.Pages) {
        Save(std::move(page));
    }
}

} // namespace NTable
} // namespace NKikimr

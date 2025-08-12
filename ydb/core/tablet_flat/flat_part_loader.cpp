#include "flat_part_loader.h"
#include "flat_abi_check.h"
#include "flat_part_overlay.h"
#include "flat_part_keys.h"
#include "util_fmt_abort.h"
#include "ydb/core/base/appdata_fwd.h"
#include "ydb/core/base/feature_flags.h"

namespace NKikimr {
namespace NTable {

TLoader::TLoader(TVector<TIntrusivePtr<TPageCollection>> pageCollections,
        TString legacy,
        TString opaque,
        TVector<TString> deltas,
        TEpoch epoch)
    : PageCollections(std::move(pageCollections))
    , Legacy(std::move(legacy))
    , Opaque(std::move(opaque))
    , Deltas(std::move(deltas))
    , Epoch(epoch)
{
    if (PageCollections.size() < 1) {
        Y_TABLET_ERROR("Cannot load TPart from " << PageCollections.size() << " page collections");
    }
    LoaderEnv = MakeHolder<TLoaderEnv>(PageCollections[0]);
}

TLoader::~TLoader() { }

void TLoader::StageParseMeta()
{
    auto* metaPacket = dynamic_cast<const NPageCollection::TPageCollection*>(PageCollections.at(0)->PageCollection.Get());
    if (!metaPacket) {
        Y_TABLET_ERROR("Unexpected IPageCollection type " << TypeName(*PageCollections.at(0)->PageCollection));
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
        GarbageStatsId = layout.HasGarbageStats() ? layout.GetGarbageStats() : GarbageStatsId;
        TxIdStatsId = layout.HasTxIdStats() ? layout.GetTxIdStats() : TxIdStatsId;

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

        BTreeGroupIndexes.clear();
        BTreeHistoricIndexes.clear();
        if (layout.HasBTreeIndexesFormatVersion() && layout.GetBTreeIndexesFormatVersion() == NPage::TBtreeIndexNode::FormatVersion) {
            for (bool history : {false, true}) {
                for (const auto &meta : history ? layout.GetBTreeHistoricIndexes() : layout.GetBTreeGroupIndexes()) {
                    NPage::TBtreeIndexMeta converted{{
                        meta.GetRootPageId(), 
                        meta.GetRowCount(), 
                        meta.GetDataSize(),
                        meta.GetGroupDataSize(),
                        meta.GetErasedRowCount()}, 
                        meta.GetLevelCount(), 
                        meta.GetIndexSize()};
                    (history ? BTreeHistoricIndexes : BTreeGroupIndexes).push_back(converted);
                }
            }
        }

        if (!AppData()->FeatureFlags.GetEnableLocalDBBtreeIndex() && FlatGroupIndexes) {
            BTreeGroupIndexes.clear();
            BTreeHistoricIndexes.clear();
        }
        if (!AppData()->FeatureFlags.GetEnableLocalDBFlatIndex() && BTreeGroupIndexes) {
            FlatGroupIndexes.clear();
            FlatHistoricIndexes.clear();
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

    if (!HasBasics() || (Rooted && SchemeId != meta.TotalPages() - 1)
        || (LargeId == Max<TPageId>()) != (GlobsId == Max<TPageId>())
        || (Max(BTreeGroupIndexes.size(), FlatGroupIndexes.size()) + (SmallId == Max<TPageId>() ? 0 : 1)) != PageCollections.size())
    {
        Y_TABLET_ERROR("Part " << PageCollections[0]->PageCollection->Label() << " has"
            << " invalid layout : " << (Rooted ? "rooted" : "legacy")
            << " " << PageCollections.size() << "s " << meta.TotalPages() << "pg"
            << ", Scheme " << SchemeId 
            << ", FlatIndex " << (FlatGroupIndexes.size() ? FlatGroupIndexes[0] : Max<TPageId>())
            << ", BTreeIndex " << (BTreeGroupIndexes.size() ? BTreeGroupIndexes[0].GetPageId() : Max<TPageId>())
            << ", Blobs " << GlobsId << ", Small " << SmallId
            << ", Large " << LargeId << ", ByKey " << ByKeyId
            << ", Garbage " << GarbageStatsId
            << ", TxIdStats " << TxIdStatsId);
    }
}

TLoader::TFetch TLoader::StageCreatePartView(bool preloadIndex)
{
    Y_ENSURE(!PartView, "PartView already initialized in CreatePartView stage");
    Y_ENSURE(PageCollections && PageCollections.front());

    auto getPage = [&](TPageId pageId) {
        return pageId == Max<TPageId>() 
            ? nullptr 
            : LoaderEnv->TryGetPage(nullptr, pageId, {});
    };

    if (BTreeGroupIndexes) {
        if (preloadIndex) {
            // Note: preload root nodes only because we don't want to have multiple restarts here
            for (const auto& meta : BTreeGroupIndexes) {
                if (meta.LevelCount) getPage(meta.GetPageId());
            }
            for (const auto& meta : BTreeHistoricIndexes) {
                if (meta.LevelCount) getPage(meta.GetPageId());
            }
        }
    } else if (FlatGroupIndexes) {
        for (auto indexPageId : FlatGroupIndexes) {
            getPage(indexPageId);
        }
        for (auto indexPageId : FlatHistoricIndexes) {
            getPage(indexPageId);
        }
    }

    for (auto pageId: { SchemeId, GlobsId, SmallId, LargeId, ByKeyId, GarbageStatsId, TxIdStatsId }) {
        Y_DEBUG_ABORT_UNLESS(pageId == Max<TPageId>() || NeedIn(PageCollections[0]->GetPageType(pageId)));
        getPage(pageId);
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
    } else if (small && PageCollections.size() != (1 + Max(BTreeGroupIndexes.size(), FlatGroupIndexes.size()))) {
        Y_TABLET_ERROR("TPart has small blobs, " << PageCollections.size() << " page collections");
    }

    const auto extra = BlobsLabelFor(PageCollections[0]->PageCollection->Label());

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
        for (auto indexPage : FlatGroupIndexes) {
            indexesRawSize += PageCollections[0]->GetPageSize(indexPage);
        }
        for (auto indexPage : FlatHistoricIndexes) {
            indexesRawSize += PageCollections[0]->GetPageSize(indexPage);
        }
    }

    auto *partStore = new TPartStore(
        PageCollections.front()->PageCollection->Label(),
        {
            epoch,
            TPartScheme::Parse(*scheme, Rooted),
            { FlatGroupIndexes, FlatHistoricIndexes, BTreeGroupIndexes, BTreeHistoricIndexes },
            blobs ? new NPage::TExtBlobs(*blobs, extra) : nullptr,
            byKey ? new NPage::TBloom(*byKey) : nullptr,
            large ? new NPage::TFrames(*large) : nullptr,
            small ? new NPage::TFrames(*small) : nullptr,
            indexesRawSize,
            MinRowVersion,
            MaxRowVersion,
            garbageStats ? new NPage::TGarbageStats(*garbageStats) : nullptr,
            txIdStats ? new NPage::TTxIdStatsPage(*txIdStats) : nullptr,
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

    partStore->PageCollections = std::move(PageCollections);

    if (partStore->Blobs) {
        Y_ENSURE(partStore->Large, "Cannot use blobs without frames");

        partStore->Pseudo = new TPageCollection(partStore->Blobs);
    }

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

    LoaderEnv->EnsureNoNeedPages();

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

    // Note: preload works only for main group pages    
    auto total = partStore->PageCollections[0]->PageCollection->Total();

    TVector<TPageId> toLoad(::Reserve(total));
    for (TPageId pageId : xrange(total)) {
        LoaderEnv->TryGetPage(PartView.Part.Get(), pageId, {});
    }

    return LoaderEnv->GetFetch();
}

void TLoader::Save(TVector<NSharedCache::TEvResult::TLoaded>&& pages)
{
    if (Stage == EStage::PartView || Stage == EStage::Slice || Stage == EStage::PreloadData) {
        for (auto& page : pages) {
            LoaderEnv->Save(std::move(page));
        }
    } else {
        Y_TABLET_ERROR("Unexpected pages save on stage " << int(Stage));
    }
}

}
}

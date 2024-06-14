#include "flat_part_loader.h"
#include "flat_abi_check.h"
#include "flat_part_overlay.h"
#include "flat_part_keys.h"
#include "util_fmt_abort.h"
#include "ydb/core/base/appdata_fwd.h"
#include "ydb/core/base/feature_flags.h"

namespace NKikimr {
namespace NTable {

TLoader::TLoader(TVector<TIntrusivePtr<TCache>> pageCollections,
        TString legacy,
        TString opaque,
        TVector<TString> deltas,
        TEpoch epoch)
    : Packs(std::move(pageCollections))
    , Legacy(std::move(legacy))
    , Opaque(std::move(opaque))
    , Deltas(std::move(deltas))
    , Epoch(epoch)
{
    if (Packs.size() < 1) {
        Y_Fail("Cannot load TPart from " << Packs.size() << " page collections");
    }
}

TLoader::~TLoader() { }

void TLoader::StageParseMeta() noexcept
{
    auto* metaPacket = dynamic_cast<const NPageCollection::TPageCollection*>(Packs.at(0)->PageCollection.Get());
    if (!metaPacket) {
        Y_Fail("Unexpected IPageCollection type " << TypeName(*Packs.at(0)->PageCollection));
    }

    auto &meta = metaPacket->Meta;

    TPageId pageId = meta.TotalPages();

    Y_ABORT_UNLESS(pageId > 0, "Got page collection without pages");

    if (EPage(meta.Page(pageId - 1).Type) == EPage::Schem2) {
        /* New styled page collection with layout meta. Later root meta will
            be placed in page collection metablob, now it is placed as inplace
            data for EPage::Schem2, have to be the last page.
            */

        Rooted = true, SchemeId = pageId - 1;

        ParseMeta(meta.GetPageInplaceData(SchemeId));

        Y_ABORT_UNLESS(Root.HasLayout(), "Rooted page collection has no layout");

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
        || (Max(BTreeGroupIndexes.size(), FlatGroupIndexes.size()) + (SmallId == Max<TPageId>() ? 0 : 1)) != Packs.size())
    {
        Y_Fail("Part " << Packs[0]->PageCollection->Label() << " has"
            << " invalid layout : " << (Rooted ? "rooted" : "legacy")
            << " " << Packs.size() << "s " << meta.TotalPages() << "pg"
            << ", Scheme " << SchemeId 
            << ", FlatIndex " << (FlatGroupIndexes.size() ? FlatGroupIndexes[0] : Max<TPageId>())
            << ", BTreeIndex " << (BTreeGroupIndexes.size() ? BTreeGroupIndexes[0].PageId : Max<TPageId>())
            << ", Blobs " << GlobsId << ", Small " << SmallId
            << ", Large " << LargeId << ", ByKey " << ByKeyId
            << ", Garbage " << GarbageStatsId
            << ", TxIdStats " << TxIdStatsId);
    }
}

TAutoPtr<NPageCollection::TFetch> TLoader::StageCreatePartView() noexcept
{
    Y_ABORT_UNLESS(!PartView, "PartView already initialized in CreatePartView stage");
    Y_ABORT_UNLESS(Packs && Packs.front());

    TVector<TPageId> load;
    for (auto page: { SchemeId, GlobsId,
                        SmallId, LargeId, ByKeyId,
                        GarbageStatsId, TxIdStatsId }) {
        if (page != Max<TPageId>() && !Packs[0]->Lookup(page))
            load.push_back(page);
    }

    if (load) {
        return new NPageCollection::TFetch{ 0, Packs[0]->PageCollection, std::move(load) };
    }

    auto *scheme = GetPage(SchemeId);
    auto *large = GetPage(LargeId);
    auto *small = GetPage(SmallId);
    auto *blobs = GetPage(GlobsId);
    auto *byKey = GetPage(ByKeyId);
    auto *garbageStats = GetPage(GarbageStatsId);
    auto *txIdStats = GetPage(TxIdStatsId);

    if (scheme == nullptr) {
        Y_ABORT("Scheme page is not loaded");
    } else if (ByKeyId != Max<TPageId>() && !byKey) {
        Y_ABORT("Filter page must be loaded if it exists");
    } else if (small && Packs.size() != (1 + Max(BTreeGroupIndexes.size(), FlatGroupIndexes.size()))) {
        Y_Fail("TPart has small blobs, " << Packs.size() << " page collections");
    }

    const auto extra = BlobsLabelFor(Packs[0]->PageCollection->Label());

    auto *stat = Root.HasStat() ? &Root.GetStat() : nullptr;

    // Use epoch from metadata unless it has been provided to loader externally
    TEpoch epoch = Epoch != TEpoch::Max() ? Epoch : TEpoch(Root.GetEpoch());

    // TODO: put index size to stat?
    // TODO: include history indexes bytes
    size_t indexesRawSize = 0;
    if (BTreeGroupIndexes) {
        for (const auto &meta : BTreeGroupIndexes) {
            indexesRawSize += meta.IndexSize;
        }
        // Note: although we also have flat index, it shouldn't be loaded; so let's not count it here
    } else {
        for (auto indexPage : FlatGroupIndexes) {
            indexesRawSize += GetPageSize(indexPage);
        }
    }

    auto *partStore = new TPartStore(
        Packs.front()->PageCollection->Label(),
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

    partStore->PageCollections = std::move(Packs);

    if (partStore->Blobs) {
        Y_ABORT_UNLESS(partStore->Large, "Cannot use blobs without frames");

        partStore->Pseudo = new TCache(partStore->Blobs);
    }

    auto overlay = TOverlay::Decode(Legacy, Opaque);

    PartView = { partStore, std::move(overlay.Screen), std::move(overlay.Slices) };

    KeysEnv = new TKeysEnv(PartView.Part.Get(), TPartStore::Storages(PartView).at(0));

    return nullptr;
}

TAutoPtr<NPageCollection::TFetch> TLoader::StageSliceBounds() noexcept
{
    Y_ABORT_UNLESS(PartView, "Cannot generate bounds for a missing part");

    if (PartView.Slices) {
        TOverlay{ PartView.Screen, PartView.Slices }.Validate();
        return nullptr;
    }

    KeysEnv->Check(false); /* ensure there is no pending pages to load */

    TKeysLoader loader(PartView.Part.Get(), KeysEnv.Get());

    if (auto run = loader.Do(PartView.Screen)) {
        KeysEnv->Check(false); /* On success there shouldn't be left loads */
        PartView.Slices = std::move(run);
        TOverlay{ PartView.Screen, PartView.Slices }.Validate();

        return nullptr;
    } else if (auto fetches = KeysEnv->GetFetches()) {
        return fetches;
    } else {
        Y_ABORT("Screen keys loader stalled without result");
    }
}

void TLoader::StageDeltas() noexcept
{
    Y_ABORT_UNLESS(PartView, "Cannot apply deltas to a missing part");
    Y_ABORT_UNLESS(PartView.Slices, "Missing slices in deltas stage");

    for (const TString& rawDelta : Deltas) {
        TOverlay overlay{ std::move(PartView.Screen), std::move(PartView.Slices) };

        overlay.ApplyDelta(rawDelta);

        PartView.Screen = std::move(overlay.Screen);
        PartView.Slices = std::move(overlay.Slices);
    }
}

void TLoader::Save(ui64 cookie, TArrayRef<NSharedCache::TEvResult::TLoaded> blocks) noexcept
{
    Y_ABORT_UNLESS(cookie == 0, "Only the leader pack is used on load");

    if (Stage == EStage::PartView) {
        for (auto& loaded : blocks) {
            Packs[0]->Fill(std::move(loaded), true);
        }
    } else if (Stage == EStage::Slice) {
        for (auto& loaded : blocks) {
            KeysEnv->Save(cookie, std::move(loaded));
        }
    } else {
        Y_Fail("Unexpected pages save on stage " << int(Stage));
    }
}

}
}

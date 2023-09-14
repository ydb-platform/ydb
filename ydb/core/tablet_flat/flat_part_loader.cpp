#include "flat_part_loader.h"
#include "flat_abi_check.h"
#include "flat_part_overlay.h"
#include "flat_part_keys.h"
#include "util_fmt_abort.h"

#include <typeinfo>

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

    Y_VERIFY(pageId > 0, "Got page collection without pages");

    if (EPage(meta.Page(pageId - 1).Type) == EPage::Schem2) {
        /* New styled page collection with layout meta. Later root meta will
            be placed in page collection metablob, now it is placed as inplace
            data for EPage::Schem2, have to be the last page.
            */

        Rooted = true, SchemeId = pageId - 1;

        ParseMeta(meta.GetPageInplaceData(SchemeId));

        Y_VERIFY(Root.HasLayout(), "Rooted page collection has no layout");

        if (auto *abi = Root.HasEvol() ? &Root.GetEvol() : nullptr)
            TAbi().Check(abi->GetTail(), abi->GetHead(), "part");

        const auto &layout = Root.GetLayout();

        SchemeId = layout.HasScheme() ? layout.GetScheme() : SchemeId;
        IndexId = layout.HasIndex() ? layout.GetIndex() : IndexId;
        GlobsId = layout.HasGlobs() ? layout.GetGlobs() : GlobsId;
        LargeId = layout.HasLarge() ? layout.GetLarge() : LargeId;
        SmallId = layout.HasSmall() ? layout.GetSmall() : SmallId;
        ByKeyId = layout.HasByKey() ? layout.GetByKey() : ByKeyId;
        GarbageStatsId = layout.HasGarbageStats() ? layout.GetGarbageStats() : GarbageStatsId;
        TxIdStatsId = layout.HasTxIdStats() ? layout.GetTxIdStats() : TxIdStatsId;

        GroupIndexesIds.clear();
        for (ui32 id : layout.GetGroupIndexes()) {
            GroupIndexesIds.push_back(id);
        }

        HistoricIndexesIds.clear();
        for (ui32 id : layout.GetHistoricIndexes()) {
            HistoricIndexesIds.push_back(id);
        }

    } else { /* legacy page collection w/o layout data, (Evolution < 14) */
        do {
            pageId--;
            auto type = EPage(meta.Page(pageId).Type);

            switch (type) {
            case EPage::Scheme: SchemeId = pageId; break;
            case EPage::Index: IndexId = pageId; break;
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
        || (1 + GroupIndexesIds.size() + (SmallId == Max<TPageId>() ? 0 : 1)) != Packs.size())
    {
        Y_Fail("Part " << Packs[0]->PageCollection->Label() << " has"
            << " invalid layout : " << (Rooted ? "rooted" : "legacy")
            << " " << Packs.size() << "s " << meta.TotalPages() << "pg"
            << ", Scheme " << SchemeId << ", Index " << IndexId
            << ", Blobs " << GlobsId << ", Small " << SmallId
            << ", Large " << LargeId << ", ByKey " << ByKeyId
            << ", Garbage " << GarbageStatsId
            << ", TxIdStats " << TxIdStatsId);
    }
}

TAutoPtr<NPageCollection::TFetch> TLoader::StageCreatePartView() noexcept
{
    Y_VERIFY(!PartView, "PartView already initialized in CreatePartView stage");
    Y_VERIFY(Packs && Packs.front());

    TVector<TPageId> load;
    for (auto page: { SchemeId, IndexId, GlobsId,
                        SmallId, LargeId, ByKeyId,
                        GarbageStatsId, TxIdStatsId }) {
        if (page != Max<TPageId>() && !Packs[0]->Lookup(page))
            load.push_back(page);
    }
    for (auto page : GroupIndexesIds) {
        if (!Packs[0]->Lookup(page)) {
            load.push_back(page);
        }
    }
    for (auto page : HistoricIndexesIds) {
        if (!Packs[0]->Lookup(page)) {
            load.push_back(page);
        }
    }

    if (load) {
        return new NPageCollection::TFetch{ 0, Packs[0]->PageCollection, std::move(load) };
    }

    auto *scheme = GetPage(SchemeId);
    auto *index = GetPage(IndexId);
    auto *large = GetPage(LargeId);
    auto *small = GetPage(SmallId);
    auto *blobs = GetPage(GlobsId);
    auto *byKey = GetPage(ByKeyId);
    auto *garbageStats = GetPage(GarbageStatsId);
    auto *txIdStats = GetPage(TxIdStatsId);

    if (scheme == nullptr || index == nullptr) {
        Y_FAIL("One of scheme or index pages is not loaded");
    } else if (ByKeyId != Max<TPageId>() && !byKey) {
        Y_FAIL("Filter page must be loaded if it exists");
    } else if (small && Packs.size() != (1 + GroupIndexesIds.size() + 1)) {
        Y_Fail("TPart has small blobs, " << Packs.size() << " page collections");
    }

    TVector<TSharedData> groupIndexes;
    groupIndexes.reserve(GroupIndexesIds.size());
    for (auto pageId : GroupIndexesIds) {
        auto* page = GetPage(pageId);
        if (!page) {
            Y_Fail("Missing group index page " << pageId);
        }
        groupIndexes.emplace_back(*page);
    }

    TVector<TSharedData> historicIndexes(Reserve(HistoricIndexesIds.size()));
    for (auto pageId : HistoricIndexesIds) {
        auto* page = GetPage(pageId);
        if (!page) {
            Y_Fail("Missing historic index page " << pageId);
        }
        historicIndexes.emplace_back(*page);
    }

    const auto extra = BlobsLabelFor(Packs[0]->PageCollection->Label());

    auto *stat = Root.HasStat() ? &Root.GetStat() : nullptr;

    // Use epoch from metadata unless it has been provided to loader externally
    TEpoch epoch = Epoch != TEpoch::Max() ? Epoch : TEpoch(Root.GetEpoch());

    TVector<TPageId> groupIndexesIds(Reserve(GroupIndexesIds.size() + 1));
    groupIndexesIds.push_back(IndexId);
    for (auto pageId : GroupIndexesIds) {
        groupIndexesIds.push_back(pageId);
    }

    auto *partStore = new TPartStore(
        Packs.front()->PageCollection->Label(),
        {
            epoch,
            TPartScheme::Parse(*scheme, Rooted),
            { groupIndexesIds, HistoricIndexesIds },
            *index,
            blobs ? new NPage::TExtBlobs(*blobs, extra) : nullptr,
            byKey ? new NPage::TBloom(*byKey) : nullptr,
            large ? new NPage::TFrames(*large) : nullptr,
            small ? new NPage::TFrames(*small) : nullptr,
            std::move(groupIndexes),
            std::move(historicIndexes),
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
        Y_VERIFY(partStore->Large, "Cannot use blobs without frames");

        partStore->Pseudo = new TCache(partStore->Blobs);
    }

    auto overlay = TOverlay::Decode(Legacy, Opaque);

    PartView = { partStore, std::move(overlay.Screen), std::move(overlay.Slices) };

    KeysEnv = new TKeysEnv(PartView.Part.Get(), TPartStore::Storages(PartView).at(0));

    return nullptr;
}

TAutoPtr<NPageCollection::TFetch> TLoader::StageSliceBounds() noexcept
{
    Y_VERIFY(PartView, "Cannot generate bounds for a missing part");

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
        Y_FAIL("Screen keys loader stalled withoud result");
    }
}

void TLoader::StageDeltas() noexcept
{
    Y_VERIFY(PartView, "Cannot apply deltas to a missing part");
    Y_VERIFY(PartView.Slices, "Missing slices in deltas stage");

    for (const TString& rawDelta : Deltas) {
        TOverlay overlay{ std::move(PartView.Screen), std::move(PartView.Slices) };

        overlay.ApplyDelta(rawDelta);

        PartView.Screen = std::move(overlay.Screen);
        PartView.Slices = std::move(overlay.Slices);
    }
}

void TLoader::Save(ui64 cookie, TArrayRef<NSharedCache::TEvResult::TLoaded> blocks) noexcept
{
    Y_VERIFY(cookie == 0, "Only the leader pack is used on load");

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

#include "flat_store_hotdog.h"
#include "flat_store_solid.h"
#include "flat_part_store.h"
#include "flat_part_overlay.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

void TPageCollectionProtoHelper::Snap(NKikimrExecutorFlat::TLogTableSnap *snap, const TPartView &partView, ui32 table, ui32 level)
{
    snap->SetTable(table);
    snap->SetCompactionLevel(level);

    TPageCollectionProtoHelper(false).Do(snap->AddBundles(), partView);
}

void TPageCollectionProtoHelper::Snap(NKikimrExecutorFlat::TLogTableSnap *snap, const TIntrusiveConstPtr<TColdPart> &part, ui32 table, ui32 level)
{
    snap->SetTable(table);
    snap->SetCompactionLevel(level);

    TPageCollectionProtoHelper(false).Do(snap->AddBundles(), part);
}

void TPageCollectionProtoHelper::Snap(NKikimrExecutorFlat::TLogTableSnap *snap, const TPartComponents &pc, ui32 table, ui32 level)
{
    snap->SetTable(table);
    snap->SetCompactionLevel(level);

    TPageCollectionProtoHelper(false).Do(snap->AddBundles(), pc);
}

void TPageCollectionProtoHelper::Do(TBundle *bundle, const TPartComponents &pc)
{
    bundle->MutablePageCollections()->Reserve(pc.PageCollectionComponents.size());

    for (auto &one : pc.PageCollectionComponents)
        Bundle(bundle->AddPageCollections(), one.LargeGlobId, one.PageCollection.Get());

    if (auto &legacy = pc.Legacy)
        bundle->SetLegacy(legacy);

    if (auto &opaque = pc.Opaque)
        bundle->SetOpaque(opaque);

    bundle->SetEpoch(pc.GetEpoch().ToProto());
}

void TPageCollectionProtoHelper::Do(TBundle *bundle, const NTable::TPartView &partView)
{
    Y_ENSURE(partView, "Cannot make bundle dump from empty NTable::TPartView");

    auto *part = partView.As<NTable::TPartStore>();

    Y_ENSURE(part, "Cannot cast TPart to page collection backed up part");
    Y_ENSURE(part->Label == part->PageCollections[0]->PageCollection->Label());

    bundle->MutablePageCollections()->Reserve(part->PageCollections.size());

    for (auto &cache : part->PageCollections)
        Bundle(bundle->AddPageCollections(), *cache);

    if (auto legacy = NTable::TOverlay{ partView.Screen, nullptr }.Encode()) {
        bundle->SetLegacy(std::move(legacy));
    }

    if (auto opaque = NTable::TOverlay{ nullptr, partView.Slices }.Encode()) {
        bundle->SetOpaque(std::move(opaque));
    }

    bundle->SetEpoch(part->Epoch.ToProto());
}

void TPageCollectionProtoHelper::Do(TBundle *bundle, const TIntrusiveConstPtr<NTable::TColdPart> &part)
{
    Y_ENSURE(part, "Cannot make bundle dump from empty NTable::TColdPart");

    auto *partStore = dynamic_cast<const NTable::TColdPartStore*>(part.Get());

    Y_ENSURE(partStore, "Cannot cast TColdPart to page collection backed up part");
    Y_ENSURE(partStore->Label == partStore->LargeGlobIds[0].Lead);

    bundle->MutablePageCollections()->Reserve(partStore->LargeGlobIds.size());

    for (const auto &largeGlobId : partStore->LargeGlobIds) {
        Bundle(bundle->AddPageCollections(), largeGlobId, nullptr);
    }

    if (partStore->Legacy) {
        bundle->SetLegacy(partStore->Legacy);
    }

    if (partStore->Opaque) {
        bundle->SetOpaque(partStore->Opaque);
    }

    bundle->SetEpoch(partStore->Epoch.ToProto());
}

void TPageCollectionProtoHelper::Bundle(NKikimrExecutorFlat::TPageCollection *pageCollectionProto, const TPrivatePageCache::TPageCollection &pageCollection_)
{
    auto *pageCollection = CheckedCast<const NPageCollection::TPageCollection*>(pageCollection_.PageCollection.Get());

    return Bundle(pageCollectionProto, pageCollection->LargeGlobId, pageCollection);
}


void TPageCollectionProtoHelper::Bundle(
        NKikimrExecutorFlat::TPageCollection *pageCollectionProto,
        const TLargeGlobId &largeGlobId,
        const NPageCollection::TPageCollection *pack)
{
    TLargeGlobIdProto::Put(*pageCollectionProto->MutableLargeGlobId(), largeGlobId);

    if (PutMeta && pack) {
        pageCollectionProto->SetMeta(pack->Meta.Raw.ToString());
    }
}


NTable::TPartComponents TPageCollectionProtoHelper::MakePageCollectionComponents(const TBundle &proto, bool unsplit)
{
    TVector<NTable::TPageCollectionComponents> components;

    for (auto &pageCollection: proto.GetPageCollections()) {
        Y_ENSURE(pageCollection.HasLargeGlobId(), "Got page collection without TLargeGlobId");

        auto& item = components.emplace_back();
        item.LargeGlobId = TLargeGlobIdProto::Get(pageCollection.GetLargeGlobId());
        if (pageCollection.HasMeta()) {
            item.ParsePageCollection(TSharedData::Copy(pageCollection.GetMeta()));
        }
    }

    TString opaque = proto.HasLegacy() ? proto.GetLegacy() : TString{ };
    TString opaqueExt = proto.HasOpaque() ? proto.GetOpaque() : TString{ };
    NTable::TEpoch epoch = proto.HasEpoch() ? NTable::TEpoch(proto.GetEpoch()) : NTable::TEpoch::Max();

    if (unsplit && !opaqueExt.empty()) {
        TString modified = NTable::TOverlay::MaybeUnsplitSlices(opaqueExt);
        if (!modified.empty()) {
            opaqueExt = std::move(modified);
        }
    }

    return NTable::TPartComponents{ std::move(components), std::move(opaque), std::move(opaqueExt), epoch };
}

}
}

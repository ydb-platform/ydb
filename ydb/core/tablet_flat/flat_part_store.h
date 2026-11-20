#pragma once

#include "flat_part_iface.h"
#include "flat_part_outset.h"
#include "flat_part_laid.h"
#include "flat_table_part.h"
#include "flat_store_bundle.h"
#include "flat_sausage_packet.h"
#include "flat_sausagecache.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTable {

class TColdPartStore : public TColdPart {
public:
    TColdPartStore(
            TVector<NPageCollection::TLargeGlobId> largeGlobIds,
            TString legacy,
            TString opaque,
            TEpoch epoch)
        : TColdPart(ExtractLabel(largeGlobIds), epoch)
        , LargeGlobIds(std::move(largeGlobIds))
        , Legacy(std::move(legacy))
        , Opaque(std::move(opaque))
    { }

private:
    static TLogoBlobID ExtractLabel(const TVector<NPageCollection::TLargeGlobId>& largeGlobIds) {
        Y_ENSURE(!largeGlobIds.empty());
        return largeGlobIds[0].Lead;
    }

public:
    TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
    TString Legacy;
    TString Opaque;
};

class TPartStore : public TPart, public IBundle {
protected:
    static bool IsExternalPageRef(ELargeObj lob, ui64 ref) {
        if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
            Y_TABLET_ERROR("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
        }
        return lob == ELargeObj::Extern;
    }

    static ui8 GetExternalPageChannel(const NPageCollection::IPageCollection& collection, ui64 ref) {
        auto bounds = collection.Bounds(ref);
        auto glob = collection.Glob(bounds.Lo.Blob);
        return glob.Logo.Channel();
    }

    TPartStore(const TPartStore& src, TEpoch epoch)
        : TPart(src, epoch)
    {}

public:
    TPartStore(const TLogoBlobID &label, TPart::TParams egg, TStat stat)
        : TPart(label, egg, stat)
    {

    }

    const TLogoBlobID& BundleId() const noexcept override {
        return GetPageCollection(0)->Label();
    }

    ui64 BackingSize() const noexcept override
    {
        ui64 size = 0;
        for (ui32 slot : xrange(GetPageCollectionCount())) {
            size += GetPageCollection(slot)->BackingSize();
        }
        return size;
    }

    ui64 DataSize() const noexcept override
    {
        return BackingSize() - IndexesRawSize;
    }

    virtual ui32 GetPageCollectionCount() const noexcept = 0;
    virtual const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetPageCollectionRef(ui32 slot) const = 0;
    virtual const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetPseudoPageCollectionRef() const noexcept = 0;

    const NPageCollection::IPageCollection* GetPageCollection(ui32 slot) const override {
        return GetPageCollectionRef(slot).Get();
    }

    const NPageCollection::IPageCollection* GetPageCollection(ELargeObj lob, ui64 ref) const {
        return IsExternalPageRef(lob, ref) ? GetPseudoPageCollection() : GetPageCollection(OuterSlot());
    }

    const NPageCollection::IPageCollection* GetPseudoPageCollection() const noexcept {
        return GetPseudoPageCollectionRef().Get();
    }

    const NPageCollection::TPageCollection* Packet(ui32 room) const noexcept override {
        auto* pageCollection = room < GetPageCollectionCount() ? GetPageCollection(room) : nullptr;

        return dynamic_cast<const NPageCollection::TPageCollection*>(pageCollection);
    }

    /**
     * Returns all live pages of the page collection, except pages superseded by the V2 b-tree of the group,
     * which the shared cache walks by itself
     */
    TVector<TPageLocation> GetPages(ui32 slot, NPage::TGroupId groupId) const
    {
        Y_ENSURE(slot < GetPageCollectionCount());

        auto& pageCollection = *GetPageCollection(slot);
        const auto& metas = groupId.IsHistoric() ? IndexPages.BTreeHistoric : IndexPages.BTreeGroups;
        auto meta = groupId.Index < metas.size() ? &IndexPages.GetBTree(groupId) : nullptr;
        bool supersededByV2Tree = meta && meta->HasRootV2();
        auto total = pageCollection.MetaPages();
        TVector<TPageLocation> pages(Reserve(supersededByV2Tree ? 8 : total));
        for (ui32 i = 0; i < total; ++i) {
            auto type = pageCollection.Page(i).Type;
            if (type == ui32(EPage::Skip) ||
                (supersededByV2Tree && (type == ui32(EPage::BTreeIndex) || type == ui32(EPage::BTreeIndexV2) ||
                               type == ui32(EPage::DataPage)))) {
                continue;
            }
            pages.push_back(pageCollection.GetLocation(i));
        }

        return pages;
    }
};

class TPrivatePartStore : public TPartStore {
protected:
    TPrivatePartStore(const TPrivatePartStore& src, TEpoch epoch)
        : TPartStore(src, epoch)
        , PageCollections(src.PageCollections)
        , Pseudo(src.Pseudo)
    {}

public:
    using TPageCollection = NTabletFlatExecutor::TPrivatePageCache::TPageCollection;

    using TPartStore::TPartStore;

    using TPart::GetPageSize;

    ui64 GetPageSize(ELargeObj lob, ui64 ref) const override
    {
        auto* cache = Locate(lob, ref);

        return cache->PageCollection->Page(ref).Size;
    }

    ui32 GetPageCollectionCount() const noexcept override {
        return PageCollections.size();
    }

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetPageCollectionRef(ui32 slot) const override {
        return PageCollections.at(slot)->PageCollection;
    }

    const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetPseudoPageCollectionRef() const noexcept override {
        static const TIntrusiveConstPtr<NPageCollection::IPageCollection> empty;
        return Pseudo ? Pseudo->PageCollection : empty;
    }

    const NPageCollection::IPageCollection* GetPseudoPageCollection() const noexcept {
        return Pseudo ? Pseudo->PageCollection.Get() : nullptr;
    }

    using TPartStore::GetPageCollection;

    const NPageCollection::IPageCollection* GetPageCollection(ui32 slot) const override
    {
        Y_ENSURE(slot < PageCollections.size());
        const auto& collection = PageCollections[slot];
        return collection ? collection->PageCollection.Get() : nullptr;
    }

    ui64 GetSlotPageSize(ui32 slot, NPage::TPageId pageId) const override
    {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->GetPageSize(pageId);
    }

    NPage::EPage GetSlotPageType(ui32 slot, NPage::TPageId pageId) const override
    {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->GetPageType(pageId);
    }

    ui8 GetSlotChannel(ui32 slot) const override
    {
        Y_ENSURE(slot < PageCollections.size());
        return PageCollections[slot]->Id.Channel();
    }

    ui8 GetPageChannel(ELargeObj lob, ui64 ref) const override
    {
        if (IsExternalPageRef(lob, ref)) {
            return GetExternalPageChannel(*Pseudo->PageCollection, ref);
        } else {
            return PageCollections.at(OuterSlot()).Get()->Id.Channel();
        }
    }

    TIntrusiveConstPtr<TPart> CloneWithEpoch(TEpoch epoch) const override
    {
        return new TPrivatePartStore(*this, epoch);
    }

    TPageCollection* Locate(ELargeObj lob, ui64 ref) const
    {
        return (IsExternalPageRef(lob, ref) ? Pseudo : PageCollections.at(OuterSlot())).Get();
    }

    static TArrayRef<const TIntrusivePtr<TPageCollection>> Storages(const TPartView &partView)
    {
        auto *part = partView.As<TPrivatePartStore>();

        Y_ENSURE(!partView || part, "Got an unexpected type of TPart part");

        return part ? part->PageCollections : TArrayRef<const TIntrusivePtr<TPageCollection>> { };
    }

    TVector<TIntrusivePtr<TPageCollection>> PageCollections;
    TIntrusivePtr<TPageCollection> Pseudo;    /* Cache for NPage::TBlobs */
};

class TTxStatusPartStore : public TTxStatusPart, public IBorrowBundle {
public:
    TTxStatusPartStore(const NPageCollection::TLargeGlobId& dataId, TEpoch epoch, TSharedData data)
        : TTxStatusPart(dataId.Lead, epoch, new NPage::TTxStatusPage(data))
        , DataId(dataId)
    { }

    const NPageCollection::TLargeGlobId& GetDataId() const noexcept {
        return DataId;
    }

    const TLogoBlobID& BundleId() const noexcept override {
        return DataId.Lead;
    }

    ui64 BackingSize() const noexcept override {
        return DataId.Bytes;
    }

    void SaveAllBlobIdsTo(TVector<TLogoBlobID>& vec) const override {
        for (const auto& blobId : DataId.Blobs()) {
            vec.emplace_back(blobId);
        }
    }

private:
    const NPageCollection::TLargeGlobId DataId;
};

}
}

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
        Y_VERIFY(!largeGlobIds.empty());
        return largeGlobIds[0].Lead;
    }

public:
    TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
    TString Legacy;
    TString Opaque;
};

class TPartStore : public TPart, public IBundle {
protected:
    TPartStore(const TPartStore& src, TEpoch epoch)
        : TPart(src, epoch)
        , PageCollections(src.PageCollections)
        , Pseudo(src.Pseudo)
    { }

public:
    using TCache = NTabletFlatExecutor::TPrivatePageCache::TInfo;

    TPartStore(const TLogoBlobID &label, TPart::TParams egg, TStat stat)
        : TPart(label, egg, stat)
    {

    }

    const TLogoBlobID& BundleId() const override
    {
        return PageCollections[0]->PageCollection->Label();
    }

    ui64 BackingSize() const override
    {
        ui64 size = 0;
        for (const auto &cache : PageCollections) {
            size += cache->PageCollection->BackingSize();
        }
        return size;
    }

    ui64 DataSize() const override
    {
        return BackingSize() - IndexesRawSize;
    }

    ui64 GetPageSize(NPage::TPageId id, NPage::TGroupId groupId) const override
    {
        Y_VERIFY(groupId.Index < PageCollections.size());
        return PageCollections[groupId.Index]->PageCollection->Page(id).Size;
    }

    ui8 GetPageChannel(NPage::TPageId id, NPage::TGroupId groupId) const override
    {
        Y_UNUSED(id);
        Y_VERIFY(groupId.Index < PageCollections.size());
        return PageCollections[groupId.Index]->Id.Channel();
    }

    ui8 GetPageChannel(ELargeObj lob, ui64 ref) const override
    {
        if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
            Y_Fail("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
        }

        if (lob == ELargeObj::Extern) {
            auto bounds = Pseudo.Get()->PageCollection->Bounds(ref);
            auto glob = Pseudo.Get()->PageCollection->Glob(bounds.Lo.Blob);
            return glob.Logo.Channel();
        } else {
            return PageCollections.at(Groups).Get()->Id.Channel();
        }
    }

    TIntrusiveConstPtr<TPart> CloneWithEpoch(TEpoch epoch) const override
    {
        return new TPartStore(*this, epoch);
    }

    const NPageCollection::TPageCollection* Packet(ui32 room) const noexcept override
    {
        auto *pageCollection = room < PageCollections.size() ? PageCollections[room]->PageCollection.Get() : nullptr;

        return dynamic_cast<const NPageCollection::TPageCollection*>(pageCollection);
    }

    TCache* Locate(ELargeObj lob, ui64 ref) const noexcept
    {
        if ((lob != ELargeObj::Extern && lob != ELargeObj::Outer) || (ref >> 32)) {
            Y_Fail("Invalid ref ELargeObj{" << int(lob) << ", " << ref << "}");
        }

        return (lob == ELargeObj::Extern ? Pseudo : PageCollections.at(Groups)).Get();
    }

    TAutoPtr<NPageCollection::TFetch> DataPages() const noexcept
    {
        TVector<TPageId> pages;

        pages.reserve(Index->End() - Index->Begin());

        auto it = Index.LookupKey({ }, Scheme->Groups[0], ESeek::Lower, nullptr);

        for (; it; ++it) pages.emplace_back(it->GetPageId());

        return new NPageCollection::TFetch{ 0, PageCollections[0]->PageCollection , std::move(pages) };
    }

    static TVector<TIntrusivePtr<TCache>> Construct(TVector<TPageCollectionComponents> components) noexcept
    {
        TVector<TIntrusivePtr<TCache>> caches;

        for (auto &one: components) {
            caches.emplace_back(new TCache(std::move(one.Packet)));

            for (auto &page: one.Sticky)
                caches.back()->Fill(page, true);
        }

        return caches;
    }

    static TArrayRef<const TIntrusivePtr<TCache>> Storages(const TPartView &partView) noexcept
    {
        auto *part = partView.As<TPartStore>();

        Y_VERIFY(!partView || part, "Got an unexpected type of TPart part");

        return part ? part->PageCollections : TArrayRef<const TIntrusivePtr<TCache>> { };
    }

    TVector<TIntrusivePtr<TCache>> PageCollections;
    TIntrusivePtr<TCache> Pseudo;    /* Cache for NPage::TBlobs */
};

class TTxStatusPartStore : public TTxStatusPart, public IBorrowBundle {
public:
    TTxStatusPartStore(const NPageCollection::TLargeGlobId& dataId, TEpoch epoch, TSharedData data)
        : TTxStatusPart(dataId.Lead, epoch, new NPage::TTxStatusPage(data))
        , DataId(dataId)
    { }

    const NPageCollection::TLargeGlobId& GetDataId() const {
        return DataId;
    }

    const TLogoBlobID& BundleId() const override {
        return DataId.Lead;
    }

    ui64 BackingSize() const override {
        return DataId.Bytes;
    }

    void SaveAllBlobIdsTo(TVector<TLogoBlobID>& vec) const override {
        for (auto blobId : DataId.Blobs()) {
            vec.emplace_back(blobId);
        }
    }

private:
    const NPageCollection::TLargeGlobId DataId;
};

}
}

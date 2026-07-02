#pragma once

#include "ydb/core/tablet_flat/flat_table_part.h"
#include <ydb/core/tablet_flat/flat_page_label.h>
#include <ydb/core/tablet_flat/flat_part_iface.h>
#include <ydb/core/tablet_flat/flat_sausage_misc.h>
#include <ydb/core/tablet_flat/flat_util_binary.h>
#include <ydb/core/tablet_flat/util_deref.h>
#include <ydb/core/tablet_flat/util_fmt_abort.h>

#include <util/generic/xrange.h>
#include <util/generic/hash.h>
#include <array>
#include <util/system/backtrace.h>

namespace NKikimr {
namespace NTable {
namespace NTest {

    class TStore : public TAtomicRefCount<TStore> {
        enum : ui32 {
            MainPageCollection = 0,
        };

    public:
        static bool IsByteOffsetType(NPage::EPage type) noexcept {
            return type == NPage::EPage::DataPage || type == NPage::EPage::BTreeIndex;
        }

        using TData = const TSharedData;

        struct TEggs {
            bool Rooted;
            TVector<TPageId> FlatGroupIndexes;
            TVector<TPageId> FlatHistoricIndexes;
            TVector<NPage::TBtreeIndexMeta> BTreeGroupIndexes;
            TVector<NPage::TBtreeIndexMeta> BTreeHistoricIndexes;
            TData *Scheme;
            TData *Blobs;
            TData *ByKey;
            TData *Large;
            TData *Small;
            TData *GarbageStats;
            TData *TxIdStats;
        };

        ui32 GetGroupCount() const noexcept {
            return Groups;
        }

        ui32 GetRoomCount() const noexcept {
            return PageCollections.size();
        }

        ui32 GetOuterRoom() const noexcept {
            return Groups;
        }

        ui32 GetExternRoom() const noexcept {
            return Groups + 1;
        }

        const TSharedData* GetPage(ui32 room, ui32 page) const
        {
            Y_ENSURE(room < PageCollections.size(), "Room is out of bounds");

            if (page == Max<TPageId>()) return nullptr;

            if (page >= PageCollections.at(room).size()) {
                Cerr << "GetPage OOB: room=" << room << " page=" << page
                     << " size=" << PageCollections.at(room).size() << Endl;
                PrintBackTrace();
            }
            return &PageCollections.at(room).at(page);
        }

        const TSharedData* GetPage(ui32 room, NPage::TPageOffset offset) const
        {
            Y_ENSURE(room < PageCollections.size(), "Room is out of bounds");
            TPageId pageId;
            if (offset.IsByteOffset()) {
                auto it = ByteOffsetToPageId[room].find(offset.AsByteOffset());
                Y_ENSURE(it != ByteOffsetToPageId[room].end(),
                    "Byte offset " << offset.AsByteOffset() << " not found in room " << room);
                pageId = it->second;
            } else {
                pageId = offset.AsPageIndex();
            }
            if (pageId >= PageCollections.at(room).size()) {
                Cerr << "GetPage(offset) OOB: room=" << room << " pageId=" << pageId
                     << " size=" << PageCollections.at(room).size()
                     << " offset=" << offset.AsPageIndex()
                     << " isByteOffset=" << offset.IsByteOffset() << Endl;
                PrintBackTrace();
            }
            return &PageCollections.at(room).at(pageId);
        }

        size_t GetPageSize(ui32 room, ui32 page) const
        {
            Y_ENSURE(room < PageCollections.size(), "Room is out of bounds");
            Y_ENSURE(page < PageCollections.at(room).size(),
                "GetPageSize index " << page << " out of range for room " << room
                << " (size=" << PageCollections.at(room).size() << ")");

            return PageCollections.at(room).at(page).size();
        }

        NPage::EPage GetPageType(ui32 room, ui32 page) const
        {
            Y_ENSURE(room < PageCollections.size(), "Room is out of bounds");
            Y_ENSURE(page < PageTypes.at(room).size(),
                "GetPageType index " << page << " out of range for room " << room
                << " (size=" << PageTypes.at(room).size() << ")");

            return PageTypes.at(room).at(page);
        }

        ui32 GetPageChecksum(ui32 room, ui32 page) const
        {
            Y_ENSURE(room < PageCrc32.size(), "Room is out of bounds");
            Y_ENSURE(page < PageCrc32.at(room).size(),
                "GetPageChecksum index " << page << " out of range for room " << room
                << " (size=" << PageCrc32.at(room).size() << ")");

            return PageCrc32.at(room).at(page);
        }

        TArrayRef<const TSharedData> PageCollectionArray(ui32 room) const
        {
            Y_ENSURE(room < PageCollections.size(), "Only regular rooms can be used as arr");

            return PageCollections[room];
        }

        NPageCollection::TGlobId GlobForBlob(ui64 ref) const
        {
            const auto& blob = PageCollections[GetExternRoom()].at(ref);

            return { TLogoBlobID(1, 2, 3, 7, blob.size(), GlobOffset + ref), /* fake group */ 123 };
        }

        ui32 PageCollectionPagesCount(ui32 room) const
        {
            return PageCollections.at(room).size();
        }

        ui64 PageCollectionBytes(ui32 room) const
        {
            auto &pages = PageCollections.at(room);

            return
                std::accumulate(pages.begin(), pages.end(), ui64(0),
                    [](ui64 bytes, const TSharedData &page) {
                        return bytes + page.size();
                    });
        }

        ui64 GetDataBytes(ui32 room) const noexcept
        {
            return DataBytes[room];
        }

        TData* GetMeta() const noexcept
        {
            return Meta ? &Meta : nullptr;
        }

        /**
         * Used for legacy part from a binary file
         */
        TEggs LegacyEggs() const
        {
            if (PageCollectionPagesCount(MainPageCollection) == 0) {
                Y_TABLET_ERROR("Cannot construct an empty part");
            }

            Y_ENSURE(!Rooted, "Legacy store must not be rooted");
            Y_ENSURE(Groups == 1, "Legacy store must have a single main group");
            Y_ENSURE(Indexes.size() == 1, "Legacy store must have a single index");
            Y_ENSURE(Scheme != Max<TPageId>(), "Legacy store is missing a scheme page");

            return {
                Rooted,
                { Indexes.back() }, 
                { },
                { },
                { },
                GetPage(MainPageCollection, Scheme),
                GetPage(MainPageCollection, Globs),
                GetPage(MainPageCollection, ByKey),
                GetPage(MainPageCollection, Large),
                nullptr,
                nullptr,
                nullptr,
            };
        }

        void Dump(IOutputStream &stream) const
        {
            NUtil::NBin::TOut out(stream);

            if (Groups > 1) {
                Y_TABLET_ERROR("Cannot dump TStore with multiple column groups");
            } else if (!PageCollections[MainPageCollection]) {
                Y_TABLET_ERROR("Cannot dump TStore with empty leader page collection");
            } else if (PageCollections[GetOuterRoom()] || PageCollections[GetExternRoom()]) {
                Y_TABLET_ERROR("TStore has auxillary rooms, cannot be dumped");
            }

            /* Dump pages as is, without any special markup as it already
                has generic labels with sufficient data to restore page collection */

            const auto& pages = PageCollections.at(MainPageCollection);

            for (auto it: xrange(pages.size())) {
                auto got = NPage::TLabelWrapper().Read(pages[it], EPage::Undef);

                Y_ENSURE(got.Page.end() == pages[it].end());

                out.Put(pages[it]);
            }
        }

        static TIntrusivePtr<TStore> Restore(IInputStream &in)
        {
            TIntrusivePtr<TStore> storage(new TStore(1));
            NPage::TLabel label;

            while (auto got = in.Load(&label, sizeof(label))) {
                Y_ENSURE(got == sizeof(label), "Invalid pages stream");

                TSharedData to = TSharedData::Uninitialized(label.Size);

                WriteUnaligned<NPage::TLabel>(to.mutable_begin(), label);

                auto *begin = to.mutable_begin() + sizeof(NPage::TLabel);

                got = in.Load(begin,  to.mutable_end() - begin);

                if (got + sizeof(NPage::TLabel) != label.Size) {
                    Y_TABLET_ERROR("Stausage loading stalled in middle of page");
                } else if (label.Type == EPage::Scheme) {
                    /* Required for Read(Evolution < 16), hack for old style
                        scheme pages without leading label. It was ecoded in
                        sample blobs with artificial label.
                     */
                    to = TSharedData::Copy(to.Slice(sizeof(NPage::TLabel)));
                }

                storage->Write(std::move(to), label.Type, 0);
            }

            storage->Finish();

            return storage;
        }

        TPageId WriteOuter(TSharedData page)
        {
            Y_ENSURE(!Finished, "This store is already finished");

            auto room = GetOuterRoom();
            TPageId pageId = PageCollections[room].size();
            auto crc32 = NPageCollection::Checksum(page);

            PageCollections[room].emplace_back(std::move(page));
            PageTypes[room].push_back(EPage::Opaque);
            PageCrc32[room].push_back(crc32);

            return pageId;
        }

        TPageLocation Write(TSharedData page, EPage type, ui32 group)
        {
            Y_ENSURE(group < PageCollections.size() - 1, "Invalid column group");
            Y_ENSURE(!Finished, "This store is already finished");
            auto crc32 = NPageCollection::Checksum(page); /* also catches uninitialized values */

            if (type == EPage::DataPage) {
                DataBytes[group] += page.size();
            }
            TPageId pageId = PageCollections[group].size();
            ui64 byteOffset = CumulativeOffset[group]; // byte offset in cumulative stream

            CumulativeOffset[group] += page.size();
            PageOffset[group].push_back(byteOffset);

            PageCollections[group].emplace_back(std::move(page));
            PageTypes[group].push_back(type);
            PageCrc32[group].push_back(crc32);

            if (IsByteOffsetType(type)) {
                ByteOffsetToPageId[group][byteOffset] = pageId;
            }

            if (group == 0) {
                switch (type) {
                    case EPage::FlatIndex:
                        Indexes.push_back(pageId);
                        break;
                    case EPage::Frames:
                        Large = pageId;
                        break;
                    case EPage::Globs:
                        Globs = pageId;
                        break;
                    case EPage::Scheme:
                    case EPage::Schem2:
                        Scheme = pageId;
                        Rooted = (type == EPage::Schem2);
                        break;
                    case EPage::Bloom:
                        ByKey = pageId;
                        break;
                    default:
                        break;
                }
            }

            auto size = PageCollections[group][pageId].size();
            if (IsByteOffsetType(type)) {
                return TPageLocation::FromByteOffset(byteOffset, size, type, crc32);
            } else {
                return TPageLocation::FromPageIndex(pageId, size, type, crc32);
            }
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body)
        {
            Y_ENSURE(page == Scheme);

            Meta = TSharedData::Copy(body.data(), body.size());
        }

        ui32 GetWrittenPageId(ui32 group) const
        {
            return PageCollections[group].size() - 1;
        }

        NPageCollection::TGlobId WriteLarge(TSharedData data)
        {
            Y_ENSURE(!Finished, "This store is already finished");

            auto room = GetExternRoom();
            TPageId pageId = PageCollections[room].size();
            auto crc32 = NPageCollection::Checksum(data);

            PageCollections[room].emplace_back(std::move(data));
            PageTypes[room].push_back(EPage::Opaque);
            PageCrc32[room].push_back(crc32);

            return GlobForBlob(pageId);
        }

        void Finish()
        {
            Y_ENSURE(!Finished, "Cannot finish test store more than once");
            Finished = true;
        }

        TPageId ResolveByteOffset(ui32 room, ui64 offset) const
        {
            auto it = ByteOffsetToPageId[room].find(offset);
            Y_ENSURE(it != ByteOffsetToPageId[room].end(),
                "Byte offset " << offset << " not found in room " << room);
            return it->second;
        }

        NTable::NPage::TPageLocation GetPageLocation(ui32 room, TPageId pageId) const
        {
            auto type = GetPageType(room, pageId);
            auto size = GetPageSize(room, pageId);
            auto crc32 = GetPageChecksum(room, pageId);
            if (IsByteOffsetType(type)) {
                auto byteOffset = PageOffset.at(room).at(pageId);
                return NTable::NPage::TPageLocation::FromByteOffset(byteOffset, size, type, crc32);
            }
            return NTable::NPage::TPageLocation::FromPageIndex(pageId, size, type, crc32);
        }

        explicit TStore(size_t groups, ui32 globOffset = 0)
            : Groups(groups)
            , GlobOffset(globOffset)
            , PageCollections(groups + 2)
            , PageTypes(groups + 2)
            , PageCrc32(groups + 2)
            , PageOffset(groups + 2)
            , DataBytes(groups + 2)
            , CumulativeOffset(groups + 2, 0)
            , ByteOffsetToPageId(groups + 2)
        { }

        ui32 NextGlobOffset() const {
            auto& pages = PageCollections[GetExternRoom()];
            return GlobOffset + pages.size();
        }

    private:
        const size_t Groups;
        const ui32 GlobOffset;
        TVector<TVector<TSharedData>> PageCollections;
        TVector<TVector<EPage>> PageTypes;
        TVector<TVector<ui32>> PageCrc32;
        TVector<TVector<ui64>> PageOffset;
        TVector<ui64> DataBytes;
        TVector<ui64> CumulativeOffset;
        mutable TVector<THashMap<ui64, TPageId>> ByteOffsetToPageId;

        /*_ Sometimes will be replaced just with one root TPageId */

        TVector<TPageId> Indexes;
        TPageId Scheme = Max<TPageId>();
        TPageId Large = Max<TPageId>();
        TPageId Globs = Max<TPageId>();
        TPageId ByKey = Max<TPageId>();
        TSharedData Meta;
        bool Rooted = false;
        bool Finished = false;
    };

    class TStorePageCollection : public NPageCollection::IPageCollection {
        TIntrusiveConstPtr<TStore> Store;
        ui32 Room;
    public:
        TStorePageCollection(TIntrusiveConstPtr<TStore> store, ui32 room)
            : Store(std::move(store)), Room(room)
        {}

        const TLogoBlobID& Label() const noexcept override {
            static TLogoBlobID dummy(0, 0, 0, 0, 0, 0);
            return dummy;
        }

        ui32 Total() const noexcept override {
            return Store->PageCollectionPagesCount(Room);
        }

        NPageCollection::TInfo Page(ui32 page) const override {
            return {Store->GetPageSize(Room, page), 0};
        }

        NPageCollection::TBorder Bounds(ui32 page) const override {
            ui32 size = Store->GetPageSize(Room, page);
            return { size, { page, 0 }, { page, size } };
        }

        NPageCollection::TBorder Bounds(TPageLocation location) const override {
            TPageId pageId;
            if (location.Offset.IsByteOffset()) {
                pageId = Store->ResolveByteOffset(Room, location.Offset.AsByteOffset());
            } else {
                pageId = location.Offset.AsPageIndex();
            }
            ui32 size = Store->GetPageSize(Room, pageId);
            return { size, { pageId, 0 }, { pageId, size } };
        }

        NPageCollection::TGlobId Glob(ui32) const override {
            Y_TABLET_ERROR("Not implemented");
        }

        bool Verify(ui32, TArrayRef<const char>) const override {
            return true;
        }

        bool Verify(TPageLocation location, TArrayRef<const char> data) const override {
            return data.size() == location.Size;
        }

        size_t BackingSize() const noexcept override {
            return Store->PageCollectionBytes(Room);
        }

        NTable::NPage::TPageLocation GetLocation(ui32 pageId) const override {
            Y_ENSURE(pageId < Store->PageCollectionPagesCount(Room),
                "GetLocation OOB: pageId=" << pageId << " room=" << Room
                << " total=" << Store->PageCollectionPagesCount(Room));
            auto type = Store->GetPageType(Room, pageId);
            auto size = Store->GetPageSize(Room, pageId);
            auto crc32 = Store->GetPageChecksum(Room, pageId);
            return NTable::NPage::TPageLocation::FromPageIndex(pageId, size, type, crc32);
        }
    };

}
}
}

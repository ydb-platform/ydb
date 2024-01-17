#pragma once

#include <ydb/core/base/logoblob.h>
#include <ydb/library/actors/util/shared_data.h>

namespace NKikimr {
namespace NPageCollection {

    struct TLargeGlobId {
        /* ... is a piece of some data up to 4GiB placed on a continous
            series of TLogoBlobs which IDs are differs only in cookie and
            have the single upper chunk bytes limit. All blobs of span have
            the same BS storage group.
        */

        constexpr static ui32 InvalidGroup = (Max<ui32>() >> 1);

        TLargeGlobId() { };

        TLargeGlobId(ui32 group, TLogoBlobID single)
            : TLargeGlobId(group, single, single.BlobSize())
        {

        }

        TLargeGlobId(ui32 group, const TLogoBlobID &lead, ui64 bytes)
            : Group(group)
            , Bytes(bytes)
            , Lead(lead)
        {
            //Y_ABORT_UNLESS(Group != InvalidGroup, "Invalid TLargeGlobId storage group");
            Y_ABORT_UNLESS(Lead && Lead.BlobSize() && Lead.BlobSize() <= Bytes);
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "TLargeGlobId{" << Lead << " ~" << Bytes
                << "b, grp " << Group << "}";
        }

        explicit operator bool() const noexcept
        {
            return Bytes > 0;
        }

        bool operator==(const TLargeGlobId &so) const noexcept
        {
            return Lead == so.Lead && Group == so.Group && Bytes == so.Bytes;
        }

        class TBlobsEndIterator {
        public:
            TBlobsEndIterator() = default;
        };

        class TBlobsIterator {
        public:
            TBlobsIterator(const TLogoBlobID& lead, ui32 bytes)
                : Current(lead)
                , Left(bytes - lead.BlobSize())
            { }

            const TLogoBlobID& operator*() const {
                return Current;
            }

            const TLogoBlobID* operator->() const {
                return &Current;
            }

            TBlobsIterator& operator++() {
                ui32 chunk = Min(Left, Current.BlobSize());
                Current = TLogoBlobID(
                    Current.TabletID(), Current.Generation(), Current.Step(),
                    Current.Channel(), chunk, Current.Cookie() + 1);
                Left -= chunk;
                return *this;
            }

            TBlobsIterator operator++(int) {
                TBlobsIterator copy(*this);
                ++*this;
                return copy;
            }

            bool operator==(const TBlobsEndIterator&) const {
                return Current.BlobSize() == 0;
            }

            bool operator!=(const TBlobsEndIterator&) const {
                return Current.BlobSize() > 0;
            }

        private:
            TLogoBlobID Current;
            ui32 Left;
        };

        class TBlobsRange {
        public:
            TBlobsRange(const TLogoBlobID& lead, ui32 bytes)
                : Lead(lead)
                , Bytes(bytes)
            { }

            TBlobsIterator begin() const {
                return TBlobsIterator(Lead, Bytes);
            }

            TBlobsEndIterator end() const {
                return TBlobsEndIterator();
            }

        private:
            TLogoBlobID Lead;
            ui32 Bytes;
        };

        TBlobsRange Blobs() const noexcept
        {
            return TBlobsRange(Lead, Bytes);
        }

        ui32 BlobCount() const noexcept
        {
            const auto limit = Lead.BlobSize();

            return Bytes / limit + (Bytes % limit > 0);
        }

        template<typename TOut>
        void MaterializeTo(TOut &out) const noexcept
        {
            for (auto blobId : Blobs()) {
                out.emplace_back(blobId);
            }
        }

        ui32 Group = InvalidGroup;
        ui32 Bytes = 0;     /* Total bytes in sequence  */
        TLogoBlobID Lead;   /* First blob in sequence   */
    };

    struct TGlobId { /* Just single reference to TLogoBlob in group */
        TGlobId() { };

        TGlobId(const TLogoBlobID &logo, ui32 group)
            : Logo(logo)
            , Group(group)
        {

        }

        explicit operator bool() const noexcept
        {
            return bool(Logo);
        }

        bool operator!=(const TGlobId &glob) const noexcept
        {
            return Group != glob.Group || Logo != glob.Logo;
        }

        bool operator==(const TGlobId &glob) const noexcept
        {
            return Group == glob.Group && Logo == glob.Logo;
        }

        ui32 Bytes() const noexcept
        {
            return Logo.BlobSize();
        }

        TLogoBlobID Logo;
        ui32 Group = TLargeGlobId::InvalidGroup;
        ui32 Pad0_ = 0; /* Padding to 32 byte boundary, unused */
    };

    /**
     * Large blob that is written to blobstorage
     *
     * Data is stored in TString so there are less copies and conversions.
     */
    struct TGlob {
        TGlob(const TGlobId& id, TString data)
            : GId(id)
            , Data(std::move(data))
        {

        }

        ui32 Bytes() const noexcept
        {
            return GId.Logo.BlobSize();
        }

        TGlobId GId;
        TString Data;
    };

    /**
     * Large blob that is attached to memtable
     *
     * Even though it is initially written to blobstorage as TString it is
     * stored in a page-friendly format so it may be directly referenced
     * until the first compaction properly moves it to a shared cache.
     */
    struct TMemGlob {
        TMemGlob(const TGlobId& id, TSharedData data)
            : GId(id)
            , Data(std::move(data))
        {

        }

        ui32 Bytes() const noexcept
        {
            return GId.Logo.BlobSize();
        }

        TGlobId GId;
        TSharedData Data;
    };

    class TLargeGlobIdRestoreState {
        using TBlobs = TVector<TLogoBlobID>;
        using TBodies = TVector<TString>;

    public:
        TLargeGlobIdRestoreState(const TLargeGlobId& largeGlobId) {
            Blobs.reserve(largeGlobId.BlobCount());
            largeGlobId.MaterializeTo(Blobs);
            Bodies.resize(Blobs.size());
        }

        bool Apply(const TLogoBlobID& id, TString body) noexcept {
            for (size_t idx = 0, end = Blobs.size(); idx < end; ++idx) {
                if (Blobs[idx] != id) {
                    continue;
                }
                Y_ABORT_UNLESS(!Bodies[idx],
                    "Apply blob %s multiple times", id.ToString().c_str());
                Y_ABORT_UNLESS(id.BlobSize() == body.size(),
                    "Apply blob %s and body size mismatch", id.ToString().c_str());
                // N.B. we store individual bodies to minimize upfront memory requirements
                BytesLoaded += body.size();
                Bodies[idx] = std::move(body);
                return ++BlobsLoaded == Blobs.size();
            }

            Y_ABORT("Apply unknown blob %s", id.ToString().c_str());
        }

        explicit operator bool() const {
            return BlobsLoaded == Blobs.size();
        }

        const TBlobs& GetBlobs() const {
            return Blobs;
        }

        TString ExtractString() {
            Y_DEBUG_ABORT_UNLESS(BlobsLoaded == Bodies.size());

            TString data;

            if (Bodies.size() == 1) {
                data = std::move(Bodies[0]);
            } else {
                data.reserve(BytesLoaded);
                for (const TString& body : Bodies) {
                    data.append(body);
                }
            }
            Y_DEBUG_ABORT_UNLESS(data.size() == BytesLoaded);

            TBodies().swap(Bodies);

            return data;
        }

        TSharedData ExtractSharedData() {
            Y_DEBUG_ABORT_UNLESS(BlobsLoaded == Bodies.size());

            TSharedData data = TSharedData::Uninitialized(BytesLoaded);

            char* ptr = data.mutable_begin();
            for (const TString& body : Bodies) {
                ::memcpy(ptr, body.data(), body.size());
                ptr += body.size();
            }
            Y_DEBUG_ABORT_UNLESS(ptr == data.mutable_end());

            TBodies().swap(Bodies);

            return data;
        }

    private:
        TBlobs Blobs;
        TBodies Bodies;
        size_t BlobsLoaded = 0;
        size_t BytesLoaded = 0;
    };

}
}

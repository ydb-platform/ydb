#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress_matrix.h>
#include <ydb/core/base/blobstorage_grouptype.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TDiskBlob -- works with a blob loaded from disk
    ////////////////////////////////////////////////////////////////////////////
    // TODO:
    // 1. We can remove LocalParts, it is used for asserts only
    // 2. cthulhu@ knows how to reduce space for FullDataSize to 2 bytes (we would
    //    store diff between FullDataSize and PartSize)
    // Format ::= [FullDataSize=4b] [LocalParts=1b] PartData+
    //
    //
    //                        PartSize
    //                        <------->
    // Format ::= ________ __ _________ ... __________
    //            ^        ^  ^             ^
    //            |        |  |             |
    //   FullDataSize=4b   |  Part          Part
    //                     |
    //               LocalParts=1b
    //
    class TDiskBlob {
        const TRope *Rope = nullptr; // the origin rope from which this blob comes; may be null for merger case
        NMatrix::TVectorType Parts; // a set of parts in this blob
        std::array<TRope, MaxTotalPartCount> PartData; // array of part data
        std::array<ui32, MaxTotalPartCount + 1> PartOffs; // array of part offsets
        ui32 FullDataSize = 0; // size of the whole blob

    public:
        static const size_t HeaderSize = sizeof(ui32) + sizeof(ui8);
        static const size_t HugeBlobOverhead = HeaderSize;

        TDiskBlob() = default;

        TDiskBlob(const TRope *rope, NMatrix::TVectorType parts, TBlobStorageGroupType gtype, const TLogoBlobID& fullId)
            : Rope(rope)
            , Parts(parts)
        {
            // ensure the blob format is correct
            Y_VERIFY(Rope->GetSize() >= HeaderSize);
            Y_VERIFY(parts.GetSize() <= MaxTotalPartCount);
            //Y_VERIFY(parts.GetSize() == gtype.TotalPartCount()); // TODO(alexvru): fit UTs

            auto iter = Rope->Begin();

            // obtain full data size from the header
            iter.ExtractPlainDataAndAdvance(&FullDataSize, sizeof(FullDataSize));

            // then check the parts; we have `parts' argument to validate actual blob content
            ui8 partsMask;
            iter.ExtractPlainDataAndAdvance(&partsMask, sizeof(partsMask));
            Y_VERIFY(parts.Raw() == partsMask);

            // calculate part layout in the binary
            ui32 offset = HeaderSize;
            for (ui8 i = 0; i <= parts.GetSize(); ++i) {
                PartOffs[i] = offset;
                if (i != parts.GetSize()) {
                    offset += parts.Get(i) ? gtype.PartSize(TLogoBlobID(fullId, i + 1)) : 0;
                }
            }
            Y_VERIFY(GetSize() == Rope->GetSize(), "%" PRIu32 " != %zu", GetSize(), Rope->GetSize());
        }

        bool Empty() const {
            return Parts.Empty();
        }

        bool ContainsMetadataPartsOnly() const {
            for (ui8 i = Parts.FirstPosition(); i != Parts.GetSize(); i = Parts.NextPosition(i)) {
                if (GetPartSize(i)) {
                    return false;
                }
            }
            return !Empty();
        }

        ui64 GetFullDataSize() const {
            return FullDataSize;
        }

        // in some cases GetPart may return reference without actually copying the rope, so we provide holder in this case
        const TRope& GetPart(ui8 part, ui32 offset, ui32 size, TRope *holder) const {
            Y_VERIFY(Parts.Get(part));
            const ui32 partSize = GetPartSize(part);
            Y_VERIFY(offset <= partSize && offset + size <= partSize && part < PartData.size());
            if (Rope) {
                auto iter = Rope->Position(PartOffs[part] + offset);
                return *holder = TRope(iter, iter + size);
            } else {
                return PartData[part];
            }
        }

        TRope GetPart(ui8 part, TRope *holder) const {
            return GetPart(part, 0, GetPartSize(part), holder);
        }

        ui32 GetPartSize(ui8 part) const {
            Y_VERIFY(part < PartData.size());
            return PartOffs[part + 1] - PartOffs[part];
        }

        NMatrix::TVectorType GetParts() const {
            return Parts;
        }

        ui32 GetSize() const {
            return PartOffs[Parts.GetSize()];
        }

        ////////////////// Iterator via all parts ///////////////////////////////////////
        class TPartIterator {
        public:
            TPartIterator(const TDiskBlob *blob, ui8 part)
                : Blob(blob)
                , Part(part)
            {
                if (blob->Rope) {
                    Iter = blob->Rope->Position(blob->PartOffs[part]);
                }
            }

            inline TPartIterator& operator++() noexcept {
                if (Blob->Rope) {
                    Iter += Blob->GetPartSize(Part);
                }
                Part = Blob->Parts.NextPosition(Part);
                return *this;
            }

            inline TPartIterator operator++(int) noexcept {
                TPartIterator res(*this);
                ++*this;
                return res;
            }

            ui8 GetPartId() const {
                return Part + 1;
            }

            const TRope& GetPart(ui32 offset, ui32 size, TRope *holder) const {
                if (Blob->Rope) {
                    return *holder = TRope(Iter + offset, Iter + (offset + size));
                } else if (offset == 0 && size == Blob->GetPartSize(Part)) {
                    Y_VERIFY(Part < Blob->PartData.size());
                    return Blob->PartData[Part];
                } else {
                    Y_VERIFY(Part < Blob->PartData.size());
                    const TRope& data = Blob->PartData[Part];
                    return *holder = TRope(data.Position(offset), data.Position(offset + size));
                }
            }

            TRope GetPart() const {
                if (Blob->Rope) {
                    return TRope(Iter, Iter + Blob->GetPartSize(Part));
                } else {
                    Y_VERIFY(Part < Blob->PartData.size());
                    return Blob->PartData[Part];
                }
            }

            bool operator == (const TPartIterator &i) {
                Y_VERIFY_DEBUG(Blob == i.Blob);
                return Part == i.Part;
            }

            bool operator != (const TPartIterator &i) {
                return !operator==(i);
            }

        private:
            const TDiskBlob *Blob; // the blob we are walking over
            ui8 Part; // part index, actually partId - 1
            TRope::TConstIterator Iter; // iterator to rope, if set
        };

        TPartIterator begin() const {
            return {this, Parts.FirstPosition()};
        }

        TPartIterator end() const {
            return {this, Parts.GetSize()};
        }

        ////////////////// Iterator via all parts ///////////////////////////////////////

        friend bool operator ==(const TDiskBlob& x, const TDiskBlob& y) {
            return x.FullDataSize == y.FullDataSize
                && x.Parts == y.Parts
                && ((x.Rope && y.Rope) ? *x.Rope == *y.Rope : x.Rope == y.Rope)
                && x.PartData == y.PartData
                && x.PartOffs == y.PartOffs;
        }

    public:
        template<typename TPartIt>
        static TRope CreateFromDistinctParts(TPartIt first, TPartIt last, NMatrix::TVectorType parts, ui64 fullDataSize, TRopeArena& arena) {
            // ensure that we have correct number of set parts
            Y_VERIFY(parts.CountBits() == std::distance(first, last));
            Y_VERIFY(first != last);

            TRope rope;

            // fill in header
            char header[HeaderSize];
            Y_VERIFY(fullDataSize <= Max<ui32>());
            *reinterpret_cast<ui32*>(header) = fullDataSize;
            *reinterpret_cast<ui8*>(header + sizeof(ui32)) = parts.Raw();
            rope.Insert(rope.End(), arena.CreateRope(header, HeaderSize));

            // then copy parts' contents to the rope
            while (first != last) {
                rope.Insert(rope.End(), std::move(*first++));
            }

            return rope;
        }

        static inline TRope Create(ui64 fullDataSize, ui8 partId, ui8 total, TRope&& data, TRopeArena& arena) {
            Y_VERIFY(partId > 0 && partId <= 8);
            return CreateFromDistinctParts(&data, &data + 1, NMatrix::TVectorType::MakeOneHot(partId - 1, total),
                fullDataSize, arena);
        }

        static inline TRope Create(ui64 fullDataSize, NMatrix::TVectorType parts, TRope&& data, TRopeArena& arena) {
            return CreateFromDistinctParts(&data, &data + 1, parts, fullDataSize, arena);
        }

        // static function for calculating size of a blob being created ('Create' function creates blob of this size)
        static inline ui32 CalculateBlobSize(TBlobStorageGroupType gtype, const TLogoBlobID& fullId, NMatrix::TVectorType parts) {
            ui32 res = HeaderSize;
            for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
                res += gtype.PartSize(TLogoBlobID(fullId, i + 1));
            }
            return res;
        }

    private:
        friend class TDiskBlobMerger;

        // used by blob merger
        void MergePart(const TDiskBlob& source, TPartIterator iter) {
            const ui8 part = iter.GetPartId() - 1;
            Y_VERIFY(!Rope); // ensure that this blob is used inside merger
            Y_VERIFY(FullDataSize == 0 || FullDataSize == source.FullDataSize, "FullDataSize# %" PRIu32 " source.FullDataSize# %" PRIu32,
                FullDataSize, source.FullDataSize);

            if (Parts.Empty()) {
                Parts = NMatrix::TVectorType(0, source.Parts.GetSize());
                PartOffs.fill(HeaderSize);
            } else {
                Y_VERIFY(Parts.GetSize() == source.Parts.GetSize());
            }

            if (!Parts.Get(part)) {
                Parts.Set(part);
                TRope partData = iter.GetPart();
                for (ui8 i = part + 1; i <= Parts.GetSize(); ++i) {
                    PartOffs[i] += partData.GetSize();
                }
                Y_VERIFY(part < PartData.size());
                PartData[part] = std::move(partData);
                FullDataSize = source.FullDataSize;
            }
        }

        TRope CreateDiskBlob(TRopeArena& arena) const {
            Y_VERIFY(!Empty());

            char header[HeaderSize];
            *reinterpret_cast<ui32*>(header) = FullDataSize;
            *reinterpret_cast<ui8*>(header + sizeof(ui32)) = Parts.Raw();

            TRope rope(arena.CreateRope(header, sizeof(header)));
            for (auto it = begin(); it != end(); ++it) {
                rope.Insert(rope.End(), it.GetPart());
            }

            return rope;
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TDiskBlobMerger -- merges several blobs (parts) into one
    ////////////////////////////////////////////////////////////////////////////
    class TDiskBlobMerger {
    public:
        TDiskBlobMerger()
        {}

        void Clear() {
            Blob = {};
        }

        void Add(const TDiskBlob &addBlob) {
            Y_VERIFY(!addBlob.GetParts().Empty());
            AddImpl(addBlob, addBlob.GetParts());
        }

        void AddPart(const TDiskBlob& source, const TDiskBlob::TPartIterator& it) {
            Blob.MergePart(source, it);
        }

        bool Empty() const {
            return Blob.Empty();
        }

        TRope CreateDiskBlob(TRopeArena& arena) const {
            return Blob.CreateDiskBlob(arena);
        }

        const TDiskBlob& GetDiskBlob() const {
            return Blob;
        }

        void Swap(TDiskBlobMerger &m) {
            std::swap(Blob, m.Blob);
        }

        TString ToString() const {
            TStringStream str;
            str << "{FullDataSize# " << Blob.GetFullDataSize() << " Parts# " << Blob.GetParts().ToString() << "}";
            return str.Str();
        }

        friend bool operator ==(const TDiskBlobMerger& x, const TDiskBlobMerger& y) {
            return x.Blob == y.Blob;
        }

    protected:
        void AddImpl(const TDiskBlob &addBlob, NMatrix::TVectorType addParts) {
            for (auto it = addBlob.begin(); it != addBlob.end(); ++it) {
                const ui8 part = it.GetPartId() - 1;
                if (addParts.Get(part)) {
                    AddPart(addBlob, it);
                }
            }
        }

    private:
        TDiskBlob Blob;
    };

    class TDiskBlobMergerWithMask : public TDiskBlobMerger {
    public:
        TDiskBlobMergerWithMask() = default;
        TDiskBlobMergerWithMask(const TDiskBlobMergerWithMask&) = default;
        TDiskBlobMergerWithMask(TDiskBlobMergerWithMask&&) = default;

        TDiskBlobMergerWithMask(const TDiskBlobMerger& base, NMatrix::TVectorType mask)
            : AddFilterMask(mask)
        {
            // TODO(alexvru): check for saneness; maybe we shall not provide blobs not in mask?
            const TDiskBlob& blob = base.GetDiskBlob();
            for (auto it = blob.begin(); it != blob.end(); ++it) {
                AddPart(blob, it);
            }
        }

        void Clear() {
            TDiskBlobMerger::Clear();
            AddFilterMask.Clear();
        }

        void SetFilterMask(NMatrix::TVectorType mask) {
            Y_VERIFY(!AddFilterMask);
            AddFilterMask = mask;
        }

        void Add(const TDiskBlob &addBlob) {
            Y_VERIFY(AddFilterMask);
            NMatrix::TVectorType addParts = addBlob.GetParts() & *AddFilterMask;
            if (!addParts.Empty()) {
                TDiskBlobMerger::AddImpl(addBlob, addParts);
            }
        }

        void AddPart(const TDiskBlob& source, const TDiskBlob::TPartIterator& it) {
            Y_VERIFY(AddFilterMask);
            if (AddFilterMask->Get(it.GetPartId() - 1)) {
                TDiskBlobMerger::AddPart(source, it);
            }
        }

        void Swap(TDiskBlobMergerWithMask& m) {
            TDiskBlobMerger::Swap(m);
            DoSwap(AddFilterMask, m.AddFilterMask);
        }

    private:
        TMaybe<NMatrix::TVectorType> AddFilterMask;
    };

} // NKikimr

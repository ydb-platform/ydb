#include "hullds_sst_packed.h"

#include <util/generic/algorithm.h>

#include <bit>

namespace NKikimr {

    namespace {

        using E = TPackedLogoBlobSstIndex;

        // returns the field's own type, which PackFrame checks
        template <E::EColumn C>
        auto ValueOf(const E::TRec& rec) {
            if constexpr (C == E::Step || C == E::PartCrc || C == E::BlobSize || C == E::Cookie) {
                const TLogoBlobID id = rec.GetKey().LogoBlobID();
                if constexpr (C == E::Step) {
                    return id.Step();
                } else if constexpr (C == E::PartCrc) {
                    return id.PartId() | id.CrcMode() << 4;
                } else if constexpr (C == E::BlobSize) {
                    return id.BlobSize();
                } else {
                    return id.Cookie();
                }
            } else {
                const TMemRecLogoBlob memRec = rec.GetMemRec();
                E::TMemRecFields fields;
                memcpy(&fields, &memRec, sizeof(fields));
                if constexpr (C == E::Ingress) {
                    return fields.Ingress;
                } else if constexpr (C == E::TypeId) {
                    return fields.TypeId;
                } else if constexpr (C == E::Offset) {
                    return fields.Offset;
                } else {
                    return fields.Size;
                }
            }
        }

    } // anonymous

    TPackedLogoBlobSstIndex::TPackedLogoBlobSstIndex(const TMemoryConsumer& consumer,
            const TTrackableVector<TRec>& linearIndex)
        : Records(linearIndex.size())
        , IndexHigh(BuildLogoBlobSstIndexHigh(consumer, linearIndex))
        , Bits(TMemoryConsumer(consumer))
        , Dicts(TMemoryConsumer(consumer))
        , Frames(TMemoryConsumer(consumer))
    {
        TBitVector<ui64> stream;
        Pack<Step>(stream, linearIndex);
        Pack<PartCrc>(stream, linearIndex);
        Pack<BlobSize>(stream, linearIndex);
        Pack<Cookie>(stream, linearIndex);
        Pack<Ingress>(stream, linearIndex);
        Pack<TypeId>(stream, linearIndex);
        Pack<Offset>(stream, linearIndex);
        Pack<Size>(stream, linearIndex);

        // Bit positions are ui32. Overflow takes ~23M records, while an SST has ~3M per 128 MB chunk and takes
        // HullSstSizeInChunksFresh/Level chunks, 1 by default. Crash rather than read truncated positions.
        Y_ABORT_UNLESS(stream.Size() <= Max<ui32>(), "Packed SST index exceeds 2^32 bits");

        stream.Resize(stream.Size() + ReadBitsPadding);
        Bits.assign(stream.Data(), stream.Data() + stream.Words());
        Dicts.shrink_to_fit();
        Frames.shrink_to_fit();
    }

    template <TPackedLogoBlobSstIndex::EColumn C>
    void TPackedLogoBlobSstIndex::Pack(TBitVector<ui64>& stream, const TTrackableVector<TRec>& linearIndex) {
        if constexpr (Kinds[C] == EKind::Dict) {
            PackDict<C>(stream, linearIndex);
        } else {
            PackFrame<C>(stream, linearIndex);
        }
    }

    template <TPackedLogoBlobSstIndex::EColumn C>
    void TPackedLogoBlobSstIndex::PackDict(TBitVector<ui64>& stream, const TTrackableVector<TRec>& linearIndex) {
        TVector<ui64> dict(Reserve(Records));
        for (const TRec& rec : linearIndex) {
            dict.push_back(ValueOf<C>(rec));
        }
        SortUnique(dict);

        TColumn& col = Columns[C];
        col.Base = Dicts.size();
        col.BitBase = stream.Size();
        col.Width = std::bit_width(dict.size() - 1);
        Dicts.insert(Dicts.end(), dict.begin(), dict.end());
        for (const TRec& rec : linearIndex) {
            stream.Append(LowerBound(dict.begin(), dict.end(), ValueOf<C>(rec)) - dict.begin(), col.Width);
        }
    }

    template <TPackedLogoBlobSstIndex::EColumn C>
    void TPackedLogoBlobSstIndex::PackFrame(TBitVector<ui64>& stream, const TTrackableVector<TRec>& linearIndex) {
        static_assert(std::is_same_v<decltype(ValueOf<C>(linearIndex[0])), ui32>, "TFrame::ValueBase is ui32");
        Columns[C].Base = Frames.size();
        for (size_t begin = 0; begin < Records; begin += FrameSize) {
            const size_t end = Min<size_t>(Records, begin + FrameSize);
            ui32 min = Max<ui32>();
            ui32 max = 0;
            for (size_t i = begin; i < end; ++i) {
                const ui32 value = ValueOf<C>(linearIndex[i]);
                min = Min(min, value);
                max = Max(max, value);
            }
            const ui8 width = std::bit_width(max - min);
            Frames.push_back(TFrame{.BitBase = static_cast<ui32>(stream.Size()), .ValueBase = min, .Width = width});
            for (size_t i = begin; i < end; ++i) {
                stream.Append(ValueOf<C>(linearIndex[i]) - min, width);
            }
        }
    }

    void TPackedLogoBlobSstIndex::Save(TTrackableVector<TRec> *linearIndex) const {
        linearIndex->clear();
        linearIndex->reserve(Records);
        ui64 i = 0;
        for (const TRecHigh& high : IndexHigh) {
            for (; i < high.LowRangeEndIndex; ++i) {
                linearIndex->emplace_back(KeyAt(high, i), MemRecAt(i));
            }
        }
    }

} // NKikimr

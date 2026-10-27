#pragma once

#include "defs.h"
#include "hullds_sst_indexhigh.h"

#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_rec.h>

#include <library/cpp/containers/bitseq/bitvector.h>

#include <util/system/unaligned_mem.h>

#include <bit>

namespace NKikimr {

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TPackedLogoBlobSstIndex - the two-level LogoBlob SST index with the low level stored as a fixed-width bit
    // column per field, so that any record decodes directly from its index.
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    class TPackedLogoBlobSstIndex {
    public:
        using TRec = TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>;

        using TRecHigh = TLogoBlobSstRecHigh;

        static constexpr ui32 FrameSize = 32;

        static constexpr ui32 PartIdBits = std::bit_width(TLogoBlobID::MaxPartId);

        enum EColumn : ui32 {
            Step,
            PartCrc,        // PartId | CrcMode << PartIdBits
            BlobSize,
            Cookie,
            Ingress,
            TypeId,         // Type | Id << TMemRecLogoBlob::TypeBits
            Offset,
            Size,
            ColumnCount
        };

        enum class EKind : ui8 {
            Dict,           // index into the SST's sorted distinct values
            Frame,          // offset from the frame's minimum, with a width per frame
        };

        // Chosen on production snapshots: Step and Cookie change little between neighbours, the Dict columns have
        // few distinct values per SST, and a frame bounds the cost of Offset, which grows through the SST.
        static constexpr std::array<EKind, ColumnCount> Kinds = {
            EKind::Frame,   // Step
            EKind::Dict,    // PartCrc
            EKind::Dict,    // BlobSize
            EKind::Frame,   // Cookie
            EKind::Dict,    // Ingress
            EKind::Dict,    // TypeId
            EKind::Frame,   // Offset
            EKind::Dict,    // Size
        };

        TPackedLogoBlobSstIndex(const TMemoryConsumer& consumer, const TTrackableVector<TRec>& linearIndex);

        void Save(TTrackableVector<TRec> *linearIndex) const;

        ui64 Elements() const {
            return Records;
        }

        TKeyLogoBlob FirstKey() const {
            return KeyAt(IndexHigh.front(), 0);
        }

        TKeyLogoBlob LastKey() const {
            return KeyAt(IndexHigh.back(), Records - 1);
        }

        TKeyLogoBlob KeyAt(const TRecHigh& high, ui64 index) const {
            const ui64 step = Get<Step>(index);
            return TKeyLogoBlob(TLogoBlobID(high.TabletId, high.Raw1(step), Raw2At(index, step)));
        }

        TMemRecLogoBlob MemRecAt(ui64 index) const {
            const ui64 typeId = Get<TypeId>(index);
            const auto type = TBlobType::EType(typeId & TMemRecLogoBlob::TypeMask);
            return TMemRecLogoBlob(TIngress(Get<Ingress>(index)), type, typeId >> TMemRecLogoBlob::TypeBits,
                Get<Offset>(index), Get<Size>(index));
        }

        // the key must be in the record's group
        bool LowKeyLess(ui64 index, ui64 step, ui64 raw2) const {
            const ui64 recStep = Get<Step>(index);
            return recStep != step ? recStep < step : Raw2At(index, recStep) < raw2;
        }

        class TIterator;

    private:
        struct TFrame {
            ui32 BitBase;
            ui32 ValueBase;
            ui8 Width;
        } Y_PACKED;

        static_assert(sizeof(TFrame) == 9, "expect sizeof(TFrame) == 9");

        static_assert(std::endian::native == std::endian::little, "ReadBits loads TBitVector's words as bytes");

        // TBitVector<ui64> stores bits LSB first, so that a field of up to 32 bits is one unaligned 64-bit load at its
        // first byte: the shift within the byte is at most 7, and 7 + 32 <= 64.
        static constexpr ui32 MaxReadBits = 32;

        static ui64 ReadBits(const ui64 *words, ui64 pos, ui32 bits) {
            Y_DEBUG_ABORT_UNLESS(bits <= MaxReadBits);
            const char *bytes = reinterpret_cast<const char*>(words);
            return ReadUnaligned<ui64>(bytes + pos / 8) >> (pos % 8) & ((ui64(1) << bits) - 1);
        }

        struct TColumn {
            ui32 Base = 0;      // Dict: the first entry in Dicts; Frame: the first frame in Frames
            ui32 BitBase = 0;   // Dict only: a Frame column keeps it per frame
            ui8 Width = 0;      // Dict only
        };

        const ui64 Records;
        const TTrackableVector<TRecHigh> IndexHigh;
        TTrackableVector<ui64> Bits;
        TTrackableVector<ui64> Dicts;
        TTrackableVector<TFrame> Frames;
        std::array<TColumn, ColumnCount> Columns;

        template <EColumn C>
        void Pack(TBitVector<ui64>& stream, const TTrackableVector<TRec>& linearIndex);

        template <EColumn C>
        void PackDict(TBitVector<ui64>& stream, const TTrackableVector<TRec>& linearIndex);

        template <EColumn C>
        void PackFrame(TBitVector<ui64>& stream, const TTrackableVector<TRec>& linearIndex);

        template <EColumn C>
        ui64 Get(ui64 index) const {
            const TColumn& col = Columns[C];
            if constexpr (Kinds[C] == EKind::Dict) {
                return Dicts[col.Base + ReadBits(Bits.data(), col.BitBase + index * col.Width, col.Width)];
            } else {
                const TFrame& frame = Frames[col.Base + index / FrameSize];
                return frame.ValueBase
                    + ReadBits(Bits.data(), frame.BitBase + index % FrameSize * frame.Width, frame.Width);
            }
        }

        // takes the Step its callers have already decoded
        ui64 Raw2At(ui64 index, ui64 step) const {
            const ui64 partCrc = Get<PartCrc>(index);
            return step << TLogoBlobID::StepLowShift // the high Step bits shift out
                | Get<Cookie>(index) << TLogoBlobID::CookieShift
                | (partCrc >> PartIdBits) << TLogoBlobID::CrcModeShift
                | Get<BlobSize>(index) << TLogoBlobID::BlobSizeShift
                | (partCrc & TLogoBlobID::PartIdMask);
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TPackedLogoBlobSstIndex::TIterator
    //
    // The two-level iterator with a record index in place of the low pointer.
    // Past-the-end is Pos == Records at IndexHigh.end(); before-begin is Pos == Max<ui64>().
    ////////////////////////////////////////////////////////////////////////////
    class TPackedLogoBlobSstIndex::TIterator {
        const TPackedLogoBlobSstIndex* Index = nullptr;

        const TRecHigh* High = nullptr;
        ui64 Pos = 0;
        ui64 LowRangeBegin = 0;

    public:
        TIterator() = default;

        TIterator(const TPackedLogoBlobSstIndex* index)
            : Index(index)
        {}

        bool Valid() const {
            return Index && High && Pos < Index->Records;
        }

        void Next() {
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++Pos;
            if (Pos == High->LowRangeEndIndex) {
                ++High;
                LowRangeBegin = Pos;
            }
        }

        void Prev() {
            Y_DEBUG_ABORT_UNLESS(Index && High && Pos <= Index->Records);
            if (Pos == LowRangeBegin) {
                --High;
                LowRangeBegin = High <= Index->IndexHigh.begin() ? 0 : (High - 1)->LowRangeEndIndex;
            }
            --Pos;
        }

        TKeyLogoBlob GetCurKey() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Index->KeyAt(*High, Pos);
        }

        TMemRecLogoBlob GetMemRec() const {
            Y_DEBUG_ABORT_UNLESS(Valid());
            return Index->MemRecAt(Pos);
        }

        void SeekToFirst() {
            High = Index->IndexHigh.begin();
            Pos = LowRangeBegin = 0;
        }

        void SeekToLast() {
            High = Index->IndexHigh.end();
            Pos = LowRangeBegin = Index->Records;
            Prev();
        }

        void Seek(const TKeyLogoBlob& key) {
            const TLogoBlobID id = key.LogoBlobID();
            const TRecHigh keyHigh(id);

            High = std::lower_bound(Index->IndexHigh.begin(), Index->IndexHigh.end(), keyHigh);

            if (High == Index->IndexHigh.end()) {
                Pos = LowRangeBegin = Index->Records;
                return;
            }

            const ui64 rangeBegin = High == Index->IndexHigh.begin() ? 0 : (High - 1)->LowRangeEndIndex;

            if (!High->SameKey(keyHigh)) {
                Pos = LowRangeBegin = rangeBegin;
                return;
            }

            ui64 begin = rangeBegin;
            ui64 end = High->LowRangeEndIndex;
            while (begin < end) {
                const ui64 middle = begin + (end - begin) / 2;
                if (Index->LowKeyLess(middle, id.Step(), id.GetRaw()[2])) {
                    begin = middle + 1;
                } else {
                    end = middle;
                }
            }
            Pos = begin;

            if (Pos == High->LowRangeEndIndex) {
                LowRangeBegin = Pos;
                ++High;
            } else {
                LowRangeBegin = rangeBegin;
            }
        }

        bool operator == (const TIterator& it) const {
            return High == it.High && Pos == it.Pos;
        }
    };

} // NKikimr

#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    static constexpr ui32 BaseDataChannel = 2;

    struct TChannelKind {
        std::array<ui8, 256> ChannelToIndex;
        std::vector<std::pair<ui8, ui32>> ChannelGroups;
    };

#pragma pack(push, 1)
    struct TVirtualGroupBlobFooter {
        TLogoBlobID StoredBlobId;
    };
#pragma pack(pop)

    static constexpr ui32 MaxBlobSize = 10 << 20; // 10 MB BlobStorage hard limit

    enum class EBlobType : ui32 {
        VG_COMPOSITE_BLOB = 0, // data + footer
        VG_DATA_BLOB = 1, // just data, footer aside
        VG_FOOTER_BLOB = 2, // footer only
        VG_GC_BLOB = 3, // garbage collection command
    };

    struct TBlobSeqId {
        static constexpr ui32 IndexBits = 20;
        static constexpr ui32 MaxIndex = (1 << IndexBits) - 1;

        ui32 Channel = 0;
        ui32 Generation = 0;
        ui32 Step = 0;
        ui32 Index = 0;
        
        auto AsTuple() const { return std::make_tuple(Channel, Generation, Step, Index); }

        friend bool operator ==(const TBlobSeqId& x, const TBlobSeqId& y) { return x.AsTuple() == y.AsTuple(); }
        friend bool operator !=(const TBlobSeqId& x, const TBlobSeqId& y) { return x.AsTuple() != y.AsTuple(); }
        friend bool operator < (const TBlobSeqId& x, const TBlobSeqId& y) { return x.AsTuple() <  y.AsTuple(); }
        friend bool operator <=(const TBlobSeqId& x, const TBlobSeqId& y) { return x.AsTuple() <= y.AsTuple(); }
        friend bool operator > (const TBlobSeqId& x, const TBlobSeqId& y) { return x.AsTuple() >  y.AsTuple(); }
        friend bool operator >=(const TBlobSeqId& x, const TBlobSeqId& y) { return x.AsTuple() >= y.AsTuple(); }

        TString ToString() const {
            return TStringBuilder() << "{" << Channel << ":" << Generation << ":" << Step << ":" << Index << "}";
        }

        explicit operator bool() const {
            return *this != TBlobSeqId();
        }

        ui64 ToSequentialNumber() const {
            return ui64(Step) << IndexBits | Index;
        }

        static TBlobSeqId FromSequentalNumber(ui32 channel, ui32 generation, ui64 value) {
            return {channel, generation, ui32(value >> IndexBits), ui32(value & MaxIndex)};
        }

        static TBlobSeqId FromProto(const NKikimrBlobDepot::TBlobSeqId& proto) {
            return TBlobSeqId{
                proto.GetChannel(),
                proto.GetGeneration(),
                proto.GetStep(),
                proto.GetIndex()
            };
        }

        void ToProto(NKikimrBlobDepot::TBlobSeqId *proto) const {
            proto->SetChannel(Channel);
            proto->SetGeneration(Generation);
            proto->SetStep(Step);
            proto->SetIndex(Index);
        }

        TLogoBlobID MakeBlobId(ui64 tabletId, EBlobType type, ui32 part, ui32 size) const {
            return TLogoBlobID(tabletId, Generation, Step, Channel, size, MakeCookie(type, part));
        }

        ui32 MakeCookie(EBlobType type, ui32 part) const {
            switch (type) {
                case EBlobType::VG_COMPOSITE_BLOB:
                case EBlobType::VG_DATA_BLOB:
                case EBlobType::VG_FOOTER_BLOB:
                case EBlobType::VG_GC_BLOB:
                    static constexpr ui32 typeBits = 24 - IndexBits;
                    Y_VERIFY(static_cast<ui32>(type) < (1 << typeBits));
                    Y_VERIFY(!part);
                    return Index << typeBits | static_cast<ui32>(type);
            }

            Y_FAIL();
        }
    };

    class TGivenIdRange {
        struct TRange {
            ui64 Begin;
            ui64 End;
            ui32 NumSetBits = 0;
            TDynBitMap Bits;

            TRange(ui64 begin, ui64 end)
                : Begin(begin)
                , End(end)
                , NumSetBits(end - begin)
            {
                Bits.Set(0, end - begin);
            }

            static constexpr struct TZero {} Zero{};

            TRange(ui64 begin, ui64 end, TZero)
                : Begin(begin)
                , End(end)
                , NumSetBits(0)
            {
                Bits.Reset(0, end - begin);
            }

            struct TCompare {
                bool operator ()(const TRange& x, const TRange& y) const { return x.Begin < y.Begin; }
                bool operator ()(const TRange& x, ui64 y) const { return x.Begin < y; }
                bool operator ()(ui64 x, const TRange& y) const { return x < y.Begin; }
                using is_transparent = void;
            };
        };

        using TRanges = std::set<TRange, TRange::TCompare>; // FIXME: deque?
        TRanges Ranges;
        ui32 NumAvailableItems = 0;

    public:
        void IssueNewRange(ui64 begin, ui64 end);
        void AddPoint(ui64 value);
        void RemovePoint(ui64 value, bool *wasLeast);
        bool GetPoint(ui64 value) const;

        bool IsEmpty() const;
        ui32 GetNumAvailableItems() const;
        ui64 GetMinimumValue() const;
        ui64 Allocate();

        void Subtract(const TGivenIdRange& other);
        TGivenIdRange Trim(ui64 trimUpTo);

        void Output(IOutputStream& s) const;
        TString ToString() const;

        std::vector<bool> ToDebugArray(size_t numItems) const;
        void CheckConsistency() const;

    private:
        void Pop(TRanges::iterator it, ui64 value);
    };

    using TValueChain = NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TValueChain>;
    using TResolvedValueChain = NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TResolvedValueChain>;

    template<typename TCallback>
    void EnumerateBlobsForValueChain(const TValueChain& valueChain, ui64 tabletId, TCallback&& callback) {
        for (const auto& item : valueChain) {
            const auto& locator = item.GetLocator();
            const auto& blobSeqId = TBlobSeqId::FromProto(locator.GetBlobSeqId());
            if (locator.GetTotalDataLen() + locator.GetFooterLen() > MaxBlobSize) {
                callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen()), 0, locator.GetTotalDataLen());
                callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_FOOTER_BLOB, 0, locator.GetFooterLen()), 0, 0);
            } else {
                callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_COMPOSITE_BLOB, 0, locator.GetTotalDataLen() +
                    locator.GetFooterLen()), 0, locator.GetTotalDataLen());
            }
        }
    }

    class TGenStep {
        ui64 Value = 0;

    public:
        TGenStep() = default;
        TGenStep(const TGenStep&) = default;
        TGenStep &operator=(const TGenStep& other) = default;

        explicit TGenStep(ui64 value)
            : Value(value)
        {}

        TGenStep(ui32 gen, ui32 step)
            : Value(ui64(gen) << 32 | step)
        {}

        explicit TGenStep(const TLogoBlobID& id)
            : TGenStep(id.Generation(), id.Step())
        {}
        
        explicit TGenStep(const TBlobSeqId& id)
            : TGenStep(id.Generation, id.Step)
        {}

        explicit operator ui64() const {
            return Value;
        }

        ui32 Generation() const {
            return Value >> 32;
        }

        ui32 Step() const {
            return Value;
        }

        void Output(IOutputStream& s) const {
            s << Generation() << ":" << Step();
        }

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }

        friend bool operator ==(const TGenStep& x, const TGenStep& y) { return x.Value == y.Value; }
        friend bool operator !=(const TGenStep& x, const TGenStep& y) { return x.Value != y.Value; }
        friend bool operator < (const TGenStep& x, const TGenStep& y) { return x.Value <  y.Value; }
        friend bool operator <=(const TGenStep& x, const TGenStep& y) { return x.Value <= y.Value; }
        friend bool operator > (const TGenStep& x, const TGenStep& y) { return x.Value >  y.Value; }
        friend bool operator >=(const TGenStep& x, const TGenStep& y) { return x.Value >= y.Value; }
    };

} // NKikimr::NBlobDepot

#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    static constexpr ui32 BaseDataChannel = 2;

    struct TChannelKind {
        std::array<ui8, 256> ChannelToIndex;
        std::vector<std::tuple<ui8, ui32>> ChannelGroups;
    };

#pragma pack(push, 1)
    struct TVirtualGroupBlobFooter {
        TLogoBlobID StoredBlobId;
    };
#pragma pack(pop)

    static constexpr ui32 MaxBlobSize = 10 << 20; // 10 MB BlobStorage hard limit

    enum class EBlobType : ui32 {
        VG_COMPOSITE_BLOB = 0, // data + footer
        VG_DATA_BLOB = 1, // just data, footer aside (optional)
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

        void Output(IOutputStream& s) const {
            s << "{" << Channel << ":" << Generation << ":" << Step << ":" << Index << "}";
        }

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
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

        static TBlobSeqId FromLogoBlobId(TLogoBlobID id) {
            return TBlobSeqId{
                id.Channel(),
                id.Generation(),
                id.Step(),
                IndexFromCookie(id.Cookie())
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
                    Y_ABORT_UNLESS(static_cast<ui32>(type) < (1 << typeBits));
                    Y_ABORT_UNLESS(!part);
                    return Index << typeBits | static_cast<ui32>(type);
            }

            Y_ABORT();
        }

        static ui32 IndexFromCookie(ui32 cookie) {
            static constexpr ui32 typeBits = 24 - IndexBits;
            const auto type = static_cast<EBlobType>(cookie & ((1 << typeBits) - 1));
            Y_ABORT_UNLESS(type == EBlobType::VG_COMPOSITE_BLOB || type == EBlobType::VG_DATA_BLOB ||
                type == EBlobType::VG_FOOTER_BLOB || type == EBlobType::VG_GC_BLOB);
            return cookie >> typeBits;
        }

        explicit operator TGenStep() const {
            return {Generation, Step};
        }
    };

    struct TS3Locator {
        ui32 Len = 0;
        ui32 Generation = 0;
        ui64 KeyId = 0;

        static TS3Locator FromProto(const NKikimrBlobDepot::TS3Locator& locator) {
            return {
                .Len = locator.GetLen(),
                .Generation = locator.GetGeneration(),
                .KeyId = locator.GetKeyId(),
            };
        }

        void ToProto(NKikimrBlobDepot::TS3Locator *locator) const {
            locator->SetLen(Len);
            locator->SetGeneration(Generation);
            locator->SetKeyId(KeyId);
        }

        void Output(IOutputStream& s) const {
            s << '{' << Len << '@' << Generation << '.' << KeyId << '}';
        }

        TString ToString() const {
            TStringStream s;
            Output(s);
            return s.Str();
        }

        TString MakeObjectName(const TString& basePath) const {
            const size_t hash = MultiHash(Generation, KeyId);
            const size_t a = hash % 36;
            const size_t b = hash / 36 % 36;
            static const char vec[] = "0123456789abcdefghijklmnopqrstuvwxyz";
            return TStringBuilder() << basePath
                << '/' << Generation
                << '/' << vec[a]
                << '/' << vec[b]
                << '/' << KeyId;
        }

        static std::optional<TS3Locator> FromObjectName(const TString& name, ui64 len, TString *error) {
            try {
                if (len > Max<ui32>()) {
                    *error = "value is too long";
                    return std::nullopt;
                }
                ui32 generation;
                ui64 keyId;
                TString a, b;
                Split(name, '/', generation, a, b, keyId);
                TS3Locator res{
                    .Len = static_cast<ui32>(len),
                    .Generation = generation,
                    .KeyId = keyId,
                };
                if (res.MakeObjectName(TString()) != '/' + name) {
                    *error = "object name does not match";
                    return std::nullopt;
                }
                return res;
            } catch (const std::exception& ex) {
                *error = ex.what();
                return std::nullopt;
            }
        }

        struct THash {
            size_t operator ()(const TS3Locator& x) const {
                return MultiHash(x.Len, x.Generation, x.KeyId);
            }
        };

        friend std::strong_ordering operator <=>(const TS3Locator&, const TS3Locator&) = default;
    };

    class TGivenIdRange {
        static constexpr size_t BitsPerChunk = 256;
        using TChunk = TBitMap<BitsPerChunk, ui64>;

        std::map<ui64, TChunk> Ranges;
        ui32 NumAvailableItems = 0;

    public:
        void IssueNewRange(ui64 begin, ui64 end);
        void AddPoint(ui64 value);
        void RemovePoint(ui64 value);
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

        template<typename T>
        void ForEach(T&& callback) const {
            for (const auto& [index, chunk] : Ranges) {
                Y_FOR_EACH_BIT(offset, chunk) {
                    callback(index * BitsPerChunk + offset);
                }
            }
        }
    };

    using TValueChain = NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TValueChain>;

    template<typename TCallback>
    void EnumerateBlobsForValueChain(const TValueChain& valueChain, ui64 tabletId, TCallback&& callback) {
        for (const auto& item : valueChain) {
            if (item.HasBlobLocator()) {
                const auto& locator = item.GetBlobLocator();
                const auto& blobSeqId = TBlobSeqId::FromProto(locator.GetBlobSeqId());
                if (locator.GetFooterLen() == 0) {
                    callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen()), 0, locator.GetTotalDataLen());
                } else if (locator.GetTotalDataLen() + locator.GetFooterLen() > MaxBlobSize) {
                    callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen()), 0, locator.GetTotalDataLen());
                    callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_FOOTER_BLOB, 0, locator.GetFooterLen()), 0, 0);
                } else {
                    callback(blobSeqId.MakeBlobId(tabletId, EBlobType::VG_COMPOSITE_BLOB, 0, locator.GetTotalDataLen() +
                        locator.GetFooterLen()), 0, locator.GetTotalDataLen());
                }
            }
            if (item.HasS3Locator()) {
                callback(TS3Locator::FromProto(item.GetS3Locator()));
            }
        }
    }

    inline bool IsSameValueChain(const TValueChain& x, const TValueChain& y) {
        if (x.size() != y.size()) {
            return false;
        }
        for (int i = 0; i < x.size(); ++i) {
            TString a;
            bool success = x[i].SerializeToString(&a);
            Y_ABORT_UNLESS(success);

            TString b;
            success = y[i].SerializeToString(&b);
            Y_ABORT_UNLESS(success);

            if (a != b) {
                return false;
            }
        }
        return true;
    }

#define BDEV(MARKER, TEXT, ...) \
    do { \
        auto& ctx = *TlsActivationContext; \
        const auto priority = NLog::PRI_TRACE; \
        const auto component = NKikimrServices::BLOB_DEPOT_EVENTS; \
        if (IS_LOG_PRIORITY_ENABLED(priority, component)) { \
            struct MARKER {}; \
            TStringStream __stream; \
            { \
                NJson::TJsonWriter __json(&__stream, false); \
                ::NKikimr::NStLog::TMessage<MARKER>("", 0, #MARKER)STLOG_PARAMS(__VA_ARGS__).WriteToJson(__json) << TEXT; \
            } \
            ::NActors::MemLogAdapter(ctx, priority, component, __FILE_NAME__, __LINE__, __stream.Str()); \
        }; \
    } while (false)

} // NKikimr::NBlobDepot

template<> struct THash<NKikimr::NBlobDepot::TS3Locator> : NKikimr::NBlobDepot::TS3Locator::THash {};
template<> struct std::hash<NKikimr::NBlobDepot::TS3Locator> : THash<NKikimr::NBlobDepot::TS3Locator> {};

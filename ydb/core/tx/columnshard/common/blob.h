#pragma once

#include <ydb/core/base/logoblob.h>
#include <ydb/library/conclusion/result.h>

#include <util/generic/string.h>

namespace NKikimrColumnShardProto {
class TBlobRange;
class TBlobRangeLink16;
class TUnifiedBlobId;
}

namespace NKikimr::NOlap {

class IBlobGroupSelector {
protected:
    virtual ~IBlobGroupSelector() = default;

public:
    virtual ui32 GetGroup(const TLogoBlobID& blobId) const = 0;
};

class TUnifiedBlobId;

class TUnifiedBlobId {
    // Id of a blob in YDB distributed storage
    struct TDsBlobId {
        TLogoBlobID BlobId;
        ui32 DsGroup;

        bool operator == (const TDsBlobId& other) const {
             return BlobId == other.BlobId && DsGroup == other.DsGroup;
        }

        TString ToStringNew() const {
            return Sprintf( "DS:%" PRIu32 ":%s", DsGroup, BlobId.ToString().c_str());
        }

        TString ToStringLegacy() const {
            return BlobId.ToString();
        }

        ui64 Hash() const {
            return CombineHashes<ui64>(BlobId.Hash(), IntHash(DsGroup));
        }
    };

    TDsBlobId Id;

public:
    TUnifiedBlobId() = default;

    // Initialize as DS blob Id
    TUnifiedBlobId(ui32 dsGroup, const TLogoBlobID& logoBlobId)
        : Id(TDsBlobId{logoBlobId, dsGroup})
    {}

    // Initialize as Small blob Id
    TUnifiedBlobId(ui64 tabletId, ui32 gen, ui32 step, ui32 cookie, ui32 channel, const ui32 groupId, ui32 size)
        : Id(TDsBlobId{TLogoBlobID(tabletId, gen, step, channel, size, cookie), groupId})
    {}

    TUnifiedBlobId(const TUnifiedBlobId& other) = default;
    TUnifiedBlobId& operator = (const TUnifiedBlobId& logoBlobId) = default;
    TUnifiedBlobId(TUnifiedBlobId&& other) = default;
    TUnifiedBlobId& operator = (TUnifiedBlobId&& logoBlobId) = default;

    static TUnifiedBlobId BuildRaw(const ui32 groupId, const ui64 tabletId, const ui64 r1, const ui64 r2) {
        return TUnifiedBlobId(groupId, TLogoBlobID(tabletId, r1, r2));
    }

    NKikimrColumnShardProto::TUnifiedBlobId SerializeToProto() const;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardProto::TUnifiedBlobId& proto);

    static TConclusion<TUnifiedBlobId> BuildFromProto(const NKikimrColumnShardProto::TUnifiedBlobId& proto);

    static TConclusion<TUnifiedBlobId> BuildFromString(const TString& id, const IBlobGroupSelector* dsGroupSelector) {
        TString error;
        TUnifiedBlobId result = ParseFromString(id, dsGroupSelector, error);
        if (!result.IsValid()) {
            return TConclusionStatus::Fail(error);
        }
        return result;
    }

    static TUnifiedBlobId ParseFromString(const TString& str,
        const IBlobGroupSelector* dsGroupSelector, TString& error);

    bool operator == (const TUnifiedBlobId& other) const {
        return Id == other.Id;
    }

    bool IsValid() const {
        return Id.BlobId.IsValid();
    }

    size_t BlobSize() const {
        return Id.BlobId.BlobSize();
    }

    TLogoBlobID GetLogoBlobId() const {
        return Id.BlobId;
    }

    ui32 GetDsGroup() const {
        return Id.DsGroup;
    }

    ui64 GetTabletId() const {
        return Id.BlobId.TabletID();
    }

    ui64 Hash() const noexcept {
        return Id.Hash();
    }

    // This is only implemented for DS for backward compatibility with persisted data.
    // All new functionality should rahter use string blob id representation
    TString SerializeBinary() const {
        return TString((const char*)GetLogoBlobId().GetRaw(), sizeof(TLogoBlobID));
    }

    TString ToStringLegacy() const {
        return Id.ToStringLegacy();
    }

    TString ToStringNew() const {
        return Id.ToStringNew();
    }
};


// Describes a range of bytes in a blob. It is used for read requests and for caching.
struct TBlobRange;
class TBlobRangeLink16 {
public:
    using TLinkId = ui16;

    std::optional<ui16> BlobIdx;
    ui32 Offset;
    ui32 Size;

    TBlobRangeLink16() = default;

    ui32 GetSize() const {
        return Size;
    }

    ui32 GetOffset() const {
        return Offset;
    }

    explicit TBlobRangeLink16(ui32 offset, ui32 size)
        : Offset(offset)
        , Size(size) {
    }

    explicit TBlobRangeLink16(const ui16 blobIdx, ui32 offset, ui32 size)
        : BlobIdx(blobIdx)
        , Offset(offset)
        , Size(size) {
    }

    ui16 GetBlobIdxVerified() const;

    bool IsValid() const {
        return !!BlobIdx;
    }

    NKikimrColumnShardProto::TBlobRangeLink16 SerializeToProto() const;
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardProto::TBlobRangeLink16& proto);
    static TConclusion<TBlobRangeLink16> BuildFromProto(const NKikimrColumnShardProto::TBlobRangeLink16& proto);
    TString ToString() const {
        TStringBuilder result;
        result << "[";
        if (BlobIdx) {
            result << *BlobIdx;
        } else {
            result << "NO_BLOB";
        }
        return result << ":" << Offset << ":" << Size << "]";
    }

    TBlobRange RestoreRange(const TUnifiedBlobId& blobId) const;
};

struct TBlobRange {
    TUnifiedBlobId BlobId;
    ui32 Offset;
    ui32 Size;

    bool operator<(const TBlobRange& br) const {
        if (BlobId != br.BlobId) {
            return BlobId.GetLogoBlobId().Compare(br.BlobId.GetLogoBlobId()) < 0;
        } else if (Offset != br.Offset) {
            return Offset < br.Offset;
        } else {
            return Size < br.Size;
        }
    }

    const TUnifiedBlobId& GetBlobId() const {
        return BlobId;
    }

    bool IsNextRangeFor(const TBlobRange& br) const {
        return BlobId == br.BlobId && br.Offset + br.Size == Offset;
    }

    bool TryGlueSameBlob(const TBlobRange& br, const ui64 limit) {
        if (GetBlobId() != br.GetBlobId()) {
            return false;
        }
        const ui32 right = std::max<ui32>(Offset + Size, br.Offset + br.Size);
        const ui32 offset = std::min<ui32>(Offset, br.Offset);
        const ui32 size = right - offset;
        if (size > limit) {
            return false;
        }
        Size = size;
        Offset = offset;
        return true;
    }

    bool TryGlueWithNext(const TBlobRange& br) {
        if (!br.IsNextRangeFor(*this)) {
            return false;
        }
        Size += br.Size;
        return true;
    }

    TBlobRangeLink16 BuildLink(const TBlobRangeLink16::TLinkId idx) const {
        return TBlobRangeLink16(idx, Offset, Size);
    }

    TBlobRangeLink16 IncorrectLink() const {
        return TBlobRangeLink16(Offset, Size);
    }

    bool IsValid() const {
        return BlobId.IsValid() && Size && Offset + Size <= BlobId.BlobSize();
    }

    ui32 GetBlobSize() const {
        return Size;
    }

    bool IsFullBlob() const {
        return Size == BlobId.BlobSize();
    }

    explicit TBlobRange(const TUnifiedBlobId& blobId = TUnifiedBlobId(), ui32 offset = 0, ui32 size = 0)
        : BlobId(blobId)
        , Offset(offset)
        , Size(size)
    {
        if (Size > 0) {
            Y_ABORT_UNLESS(Offset < BlobId.BlobSize());
            Y_ABORT_UNLESS(Offset + Size <= BlobId.BlobSize());
        }
    }

    static TBlobRange FromBlobId(const TUnifiedBlobId& blobId) {
        return TBlobRange(blobId, 0, blobId.BlobSize());
    }

    bool operator == (const TBlobRange& other) const {
        return
            BlobId == other.BlobId &&
            Offset == other.Offset &&
            Size == other.Size;
    }

    ui64 Hash() const noexcept {
        ui64 hash = BlobId.Hash();
        hash = CombineHashes<ui64>(hash, IntHash(Offset));
        hash = CombineHashes<ui64>(hash, IntHash(Size));
        return hash;
    }

    TString ToString() const {
        return Sprintf("{ Blob: %s Offset: %" PRIu32 " Size: %" PRIu32 " }",
                       BlobId.ToStringNew().c_str(), Offset, Size);
    }

    NKikimrColumnShardProto::TBlobRange SerializeToProto() const;

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardProto::TBlobRange& proto);
    static TConclusion<TBlobRange> BuildFromProto(const NKikimrColumnShardProto::TBlobRange& proto);
};

class IBlobInUseTracker {
private:
    virtual bool DoFreeBlob(const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual bool DoUseBlob(const NOlap::TUnifiedBlobId& blobId) = 0;
public:
    virtual ~IBlobInUseTracker() = default;

    bool FreeBlob(const NOlap::TUnifiedBlobId& blobId) {
        return DoFreeBlob(blobId);
    }
    bool UseBlob(const NOlap::TUnifiedBlobId& blobId) {
        return DoUseBlob(blobId);
    }

    virtual bool IsBlobInUsage(const NOlap::TUnifiedBlobId& blobId) const = 0;
};

// Expected blob lifecycle: EVICTING -> SELF_CACHED -> EXTERN <-> CACHED
enum class EEvictState : ui8 {
    UNKNOWN = 0,
    EVICTING = 1,       // source, extern, cached blobs: 1--
    SELF_CACHED = 2,    // source, extern, cached blobs: 11-
    EXTERN = 3,         // source, extern, cached blobs: -1-
    CACHED = 4,         // source, extern, cached blobs: -11
    ERASING = 5,        // source, extern, cached blobs: -??
    //ERASED = 6,       // source, extern, cached blobs: ---
};

inline bool IsExported(EEvictState state) {
    return state == EEvictState::SELF_CACHED ||
        state == EEvictState::EXTERN ||
        state == EEvictState::CACHED;
}

inline bool CouldBeExported(EEvictState state) {
    return state == EEvictState::SELF_CACHED ||
        state == EEvictState::EXTERN ||
        state == EEvictState::CACHED ||
        state == EEvictState::ERASING;
}

inline bool IsDeleted(EEvictState state) {
    return ui8(state) >= ui8(EEvictState::EXTERN); // !EVICTING and !SELF_CACHED
}

struct TEvictedBlob {
    EEvictState State = EEvictState::UNKNOWN;
    TUnifiedBlobId Blob;
    TUnifiedBlobId ExternBlob;
    TUnifiedBlobId CachedBlob;

    bool operator == (const TEvictedBlob& other) const {
        return Blob == other.Blob;
    }

    ui64 Hash() const noexcept {
        return Blob.Hash();
    }

    bool IsEvicting() const {
        return State == EEvictState::EVICTING;
    }

    bool IsExternal() const {
        if (State == EEvictState::EXTERN) {
            Y_ABORT_UNLESS(ExternBlob.IsValid());
            return true;
        }
        return false;
    }

    TString ToString() const {
        return TStringBuilder() << "state: " << (ui32)State
            << " blob: " << Blob.ToStringNew()
            << " extern: " << ExternBlob.ToStringNew()
            << " cached: " << CachedBlob.ToStringNew();
    }
};

}

inline
IOutputStream& operator <<(IOutputStream& out, const NKikimr::NOlap::TUnifiedBlobId& blobId) {
    return out << blobId.ToStringNew();
}

inline
IOutputStream& operator <<(IOutputStream& out, const NKikimr::NOlap::TBlobRange& blobRange) {
    return out << blobRange.ToString();
}

template<>
struct ::THash<NKikimr::NOlap::TUnifiedBlobId> {
    inline ui64 operator()(const NKikimr::NOlap::TUnifiedBlobId& a) const {
        return a.Hash();
    }
};

template <>
struct THash<NKikimr::NOlap::TBlobRange> {
    inline size_t operator() (const NKikimr::NOlap::TBlobRange& key) const {
        return key.Hash();
    }
};

template <>
struct THash<NKikimr::NOlap::TEvictedBlob> {
    inline size_t operator() (const NKikimr::NOlap::TEvictedBlob& key) const {
        return key.Hash();
    }
};

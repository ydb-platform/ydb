#pragma once

#include <ydb/core/base/logoblob.h>

#include <util/generic/string.h>

namespace NKikimr::NOlap {

class IBlobGroupSelector;
class TUnifiedBlobId;

TString DsIdToS3Key(const TUnifiedBlobId& dsid, const ui64 pathId);
TUnifiedBlobId S3KeyToDsId(const TString& s, TString& error, ui64& pathId);

// Encapsulates different types of blob ids to simplify dealing with blobs for the
// components that do not need to know where the blob is stored
// Blob id formats:
//  * Old DS blob id:  just "logoBlobId" e.g. "[72075186224038245:51:31595:2:0:11952:0]"
//  * DS blob id:      "DS:dsGroup:logoBlobId" e.g. "DS:2181038103:[72075186224038245:51:31595:2:0:11952:0]"
//  * Small blob id:   "SM[tabletId:generation:step:cookie:size]"  e.g. "SM[72075186224038245:51:31184:0:2528]"
class TUnifiedBlobId {
    struct TInvalid {
        bool operator == (const TInvalid&) const { return true; }
    };

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

    // Id of a blob that is stored in Tablet local DB table
    struct TSmallBlobId {
        static constexpr ui8 FAKE_CHANNEL = 255;    // Small blob id can be represented as
                                                    // a fake TLogoBlobID with channel = FAKE_CHANNEL

        ui64 TabletId;
        ui32 Gen;
        ui32 Step;
        ui32 Cookie;
        ui32 Size;

        bool operator == (const TSmallBlobId& other) const {
            return TabletId == other.TabletId &&
                Gen == other.Gen &&
                Step == other.Step &&
                Cookie == other.Cookie &&
                Size == other.Size;
        }

        TString ToStringNew() const {
            return Sprintf( "SM[%" PRIu64 ":%" PRIu32 ":%" PRIu32 ":%" PRIu32 ":%" PRIu32 "]",
                TabletId, Gen, Step, Cookie, Size);
        }

        TString ToStringLegacy() const {
            // For compatibility with preproduction version small blobs can also be
            // addressed by fake TlogoBlobID with channel = 255
            return TLogoBlobID(TabletId, Gen, Step, FAKE_CHANNEL, Size, Cookie).ToString();
        }

        ui64 Hash() const {
            ui64 hash = IntHash(TabletId);
            hash = CombineHashes<ui64>(hash, IntHash(Gen));
            hash = CombineHashes<ui64>(hash, IntHash(Step));
            hash = CombineHashes<ui64>(hash, IntHash(Cookie));
            hash = CombineHashes<ui64>(hash, IntHash(Size));
            return hash;
        }
    };

    struct TS3BlobId {
        TDsBlobId DsBlobId;
        TString Key;

        TS3BlobId() = default;

        TS3BlobId(const TUnifiedBlobId& dsBlob, const ui64 pathId)
        {
            Y_VERIFY(dsBlob.IsDsBlob());
            DsBlobId = std::get<TDsBlobId>(dsBlob.Id);
            Key = DsIdToS3Key(dsBlob, pathId);
        }

        bool operator == (const TS3BlobId& other) const {
            return Key == other.Key;
        }

        TString ToStringNew() const {
            return Sprintf("%s", Key.c_str());
        }

        ui64 Hash() const {
            return IntHash(THash<TString>()(Key));
        }
    };

    std::variant<
        TInvalid,
        TDsBlobId,
        TSmallBlobId,
        TS3BlobId
    > Id;

public:
    enum EBlobType {
        INVALID = 0,
        DS_BLOB = 1,
        TABLET_SMALL_BLOB = 2,
        S3_BLOB = 3,
    };

    TUnifiedBlobId()
        : Id(TInvalid())
    {}

    // Initialize as DS blob Id
    TUnifiedBlobId(ui32 dsGroup, const TLogoBlobID& logoBlobId)
        : Id(TDsBlobId{logoBlobId, dsGroup})
    {}

    // Initialize as Small blob Id
    TUnifiedBlobId(ui64 tabletId, ui32 gen, ui32 step, ui32 cookie, ui32 size)
        : Id(TSmallBlobId{tabletId, gen, step, cookie, size})
    {}

    // Make S3 blob Id from DS one
    TUnifiedBlobId(const TUnifiedBlobId& blob, EBlobType type, const ui64 pathId)
        : Id(TS3BlobId(blob, pathId))
    {
        Y_VERIFY(type == S3_BLOB);
    }

    TUnifiedBlobId(const TUnifiedBlobId& other) = default;
    TUnifiedBlobId& operator = (const TUnifiedBlobId& logoBlobId) = default;
    TUnifiedBlobId(TUnifiedBlobId&& other) = default;
    TUnifiedBlobId& operator = (TUnifiedBlobId&& logoBlobId) = default;

    TUnifiedBlobId MakeS3BlobId(ui64 pathId) const {
        Y_VERIFY(IsDsBlob());
        return TUnifiedBlobId(*this, TUnifiedBlobId::S3_BLOB, pathId);
    }

    static TUnifiedBlobId ParseFromString(const TString& str,
        const IBlobGroupSelector* dsGroupSelector, TString& error);

    bool operator == (const TUnifiedBlobId& other) const {
        return Id == other.Id;
    }

    EBlobType GetType() const {
        return (EBlobType)Id.index();
    }

    bool IsValid() const {
        return Id.index() != INVALID;
    }

    size_t BlobSize() const {
        switch (Id.index()) {
        case DS_BLOB:
            return std::get<TDsBlobId>(Id).BlobId.BlobSize();
        case TABLET_SMALL_BLOB:
            return std::get<TSmallBlobId>(Id).Size;
        case S3_BLOB:
            return std::get<TS3BlobId>(Id).DsBlobId.BlobId.BlobSize();
        case INVALID:
            Y_FAIL("Invalid blob id");
        }
        Y_FAIL();
    }

    bool IsSmallBlob() const {
        return GetType() == TABLET_SMALL_BLOB;
    }

    bool IsDsBlob() const {
        return GetType() == DS_BLOB;
    }

    bool IsS3Blob() const {
        return GetType() == S3_BLOB;
    }

    TLogoBlobID GetLogoBlobId() const {
        Y_VERIFY(IsDsBlob());
        return std::get<TDsBlobId>(Id).BlobId;
    }

    ui32 GetDsGroup() const {
        Y_VERIFY(IsDsBlob());
        return std::get<TDsBlobId>(Id).DsGroup;
    }

    TString GetS3Key() const {
        Y_VERIFY(IsS3Blob());
        return std::get<TS3BlobId>(Id).Key;
    }

    ui64 GetTabletId() const {
        switch (Id.index()) {
        case DS_BLOB:
            return std::get<TDsBlobId>(Id).BlobId.TabletID();
        case TABLET_SMALL_BLOB:
            return std::get<TSmallBlobId>(Id).TabletId;
        case S3_BLOB:
            return std::get<TS3BlobId>(Id).DsBlobId.BlobId.TabletID();
        case INVALID:
            Y_FAIL("Invalid blob id");
        }
        Y_FAIL();
    }

    ui64 Hash() const noexcept {
        switch (Id.index()) {
        case INVALID:
            return 0;
        case DS_BLOB:
            return std::get<TDsBlobId>(Id).Hash();
        case TABLET_SMALL_BLOB:
            return std::get<TSmallBlobId>(Id).Hash();
        case S3_BLOB:
            return std::get<TS3BlobId>(Id).Hash();
        }
        Y_FAIL();
    }

    // This is only implemented for DS for backward compatibility with persisted data.
    // All new functionality should rahter use string blob id representation
    TString SerializeBinary() const {
        Y_VERIFY(IsDsBlob());
        return TString((const char*)GetLogoBlobId().GetRaw(), sizeof(TLogoBlobID));
    }

    TString ToStringLegacy() const {
        switch (Id.index()) {
        case DS_BLOB:
            return std::get<TDsBlobId>(Id).ToStringLegacy();
        case TABLET_SMALL_BLOB:
            return std::get<TSmallBlobId>(Id).ToStringLegacy();
        case S3_BLOB:
            Y_FAIL("Not implemented");
        case INVALID:
            return "<Invalid blob id>";
        }
        Y_FAIL();
    }

    TString ToStringNew() const {
        switch (Id.index()) {
        case DS_BLOB:
            return std::get<TDsBlobId>(Id).ToStringNew();
        case TABLET_SMALL_BLOB:
            return std::get<TSmallBlobId>(Id).ToStringNew();
        case S3_BLOB:
            return std::get<TS3BlobId>(Id).ToStringNew();
        case INVALID:
            return "<Invalid blob id>";
        }
        Y_FAIL();
    }
};


// Describes a range of bytes in a blob. It is used for read requests and for caching.
struct TBlobRange {
    TUnifiedBlobId BlobId;
    ui32 Offset;
    ui32 Size;

    explicit TBlobRange(const TUnifiedBlobId& blobId = TUnifiedBlobId(), ui32 offset = 0, ui32 size = 0)
        : BlobId(blobId)
        , Offset(offset)
        , Size(size)
    {}

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
            Y_VERIFY(ExternBlob.IsValid());
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

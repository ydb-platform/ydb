#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/protos/blobstorage.pb.h>

#include "blobstorage_pdisk_signature.h"

#include <ydb/core/util/text.h>

namespace NKikimr {

    namespace NPDisk {
        struct TPrintable_ui8 {
            ui8 Val;

            TPrintable_ui8(ui8 val = 0)
                : Val(val)
            {}

            // To make possible usage of class instances in array indexing
            operator ui64() const {
                return Val;
            }

            TPrintable_ui8& operator--() {
                Val--;
                return *this;
            }

            TPrintable_ui8& operator++() {
                Val++;
                return *this;
            }
        };

        enum class EPDiskMetadataOutcome {
            OK, // metadata was successfully read/written
            ERROR, // I/O, locking or some other kind of error has occured
            NO_METADATA, // no metadata record available
        };

        typedef TPrintable_ui8 TOwner;
        typedef ui64 TOwnerRound;
        typedef ui32 TStatusFlags;
        typedef ui64 TKey;
        typedef ui64 THash;

        struct TMainKey {
            TStackVec<TKey, 2> Keys;
            TString ErrorReason = "";
            bool IsInitialized = false;

            operator bool() const {
                return !Keys.empty();
            }

            void Initialize() {
                if (!IsInitialized && Keys.empty()) {
                    Keys = { NPDisk::YdbDefaultPDiskSequence };
                }
                IsInitialized = true;
            }
        };

        struct TOwnerToken {
            TOwner Owner = 0;
            TOwnerRound OwnerRound = 0;

            TOwnerToken(TOwner owner, TOwnerRound ownerRound)
                : Owner(owner)
                , OwnerRound(ownerRound)
            {}
        };

        // using TLogPosition = std::pair<TChunkIdx, ui32>;
        struct TLogPosition {
            TChunkIdx ChunkIdx = 0;
            ui32 OffsetInChunk = 0;

            constexpr static TLogPosition Invalid() {
                return {Max<TChunkIdx>(), Max<ui32>()};
            }
        };

        inline bool operator==(const TLogPosition& x, const TLogPosition& y) {
            return x.ChunkIdx == y.ChunkIdx && x.OffsetInChunk == y.OffsetInChunk;
        }

        inline bool operator!=(const TLogPosition& x, const TLogPosition& y) {
            return !(x == y);
        }

        // Flag values for TStatusFlags ydb/core/protos/blobstorage.proto EStatusFlags
        inline TString StatusFlagsToString(TStatusFlags flags) {
            TStringStream str;
            bool isFirst = true;
            isFirst = NText::OutFlag(isFirst, flags == 0, "None", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusIsValid), "IsValid", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceCyan), "DiskSpaceCyan", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove),
                    "DiskSpaceLightYellowMove", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop),
                    "DiskSpaceYellowStop", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceLightOrange), "DiskSpaceLightOrange", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpacePreOrange), "DiskSpacePreOrange", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceOrange), "DiskSpaceOrange", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceRed), "DiskSpaceRed", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusDiskSpaceBlack), "DiskSpaceBlack", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusNewOwner), "NewOwner", str);
            isFirst = NText::OutFlag(isFirst, flags & ui32(NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation), "NotEnoughDiskSpaceForOperation", str);
            NText::OutFlag(isFirst, isFirst, "Unknown", str);
            return str.Str();
        }
    } // NPDisk
} // NKikimr

template<>
inline void Out<NKikimr::NPDisk::TLogPosition>(IOutputStream& os, const NKikimr::NPDisk::TLogPosition& pos) {
    os << "{";
    os << " ChunkIdx# " << pos.ChunkIdx;
    os << " OffsetInChunk# " << pos.OffsetInChunk;
    os << "}";
}

template<>
inline void Out<NKikimr::NPDisk::TOwner>(IOutputStream& os, const NKikimr::NPDisk::TPrintable_ui8& x) {
    os << static_cast<ui64>(x);
}

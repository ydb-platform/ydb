#pragma once

#include <ydb/core/blobstorage/defs.h>

#define INCRHUGE_HARDENED 1

namespace NKikimr {
    namespace NIncrHuge {
        using TIncrHugeBlobId = ui64;

#pragma pack(push,1)
        // Huge blob metadata -- universal nontransparent structure that is stored along with blob's data and returned
        // on read/init requests.
        union TBlobMetadata {
            ui8  Raw[32];
            ui64 RawU64[4];
        };

        class TChunkSerNum {
            ui64 Value;

        public:
            TChunkSerNum()
                : Value(-1)
            {}

            explicit TChunkSerNum(ui64 value)
                : Value(value)
            {}

            void Advance(ui64 shift) {
                Value += shift;
            }

            TChunkSerNum Add(ui64 shift) const {
                return TChunkSerNum(Value + shift);
            }

            TString ToString() const {
                return Sprintf("%" PRIu64, Value);
            }

            explicit operator ui64() const {
                return Value;
            }

            friend bool operator ==(const TChunkSerNum& x, const TChunkSerNum& y) { return x.Value == y.Value; }
            friend bool operator !=(const TChunkSerNum& x, const TChunkSerNum& y) { return x.Value != y.Value; }
            friend bool operator <(const TChunkSerNum& x, const TChunkSerNum& y) { return x.Value < y.Value; }
        };
#pragma pack(pop)

    } // NIncrHuge
} // NKikimr

template<>
struct THash<NKikimr::NIncrHuge::TChunkSerNum> : THash<ui64> {
    size_t operator ()(const NKikimr::NIncrHuge::TChunkSerNum& chunkSerNum) const noexcept {
        return THash<ui64>::operator ()(static_cast<ui64>(chunkSerNum));
    }
};

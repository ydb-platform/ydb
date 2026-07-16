#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore {

constexpr ui64 WasmBlobChunkSize = 8ull * 1024 * 1024;

TVector<TString> SplitBlob(TStringBuf data, ui64 chunkSize = WasmBlobChunkSize);
TString JoinBlobs(const TVector<TString>& chunks);

inline const TString& BlobKindWasmData() {
    static const TString value = "wasm_data";
    return value;
}

inline const TString& BlobKindObjectCode() {
    static const TString value = "object_code";
    return value;
}

struct TSourceChunkSchema {
    static inline const TString OwnerKeyColName = "owner_key";
    static inline const TString ChunkIdxColName = "chunk_idx";
    static inline const TString DataColName = "data";

    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();
};

struct TArtifactChunkSchema {
    static inline const TString IdColName = "id";
    static inline const TString KindColName = "kind";
    static inline const TString BlobKindColName = "blob_kind";
    static inline const TString ChunkIdxColName = "chunk_idx";
    static inline const TString DataColName = "data";

    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();
};

} // namespace NKikimr::NUdfStore

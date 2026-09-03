#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore {

constexpr ui64 WasmBlobChunkSize = 8ull * 1024 * 1024;

TVector<TString> SplitBlob(TStringBuf data, ui64 chunkSize = WasmBlobChunkSize);
TString JoinBlobs(const TVector<TString>& chunks);

//! Joins `chunks` back into a body and checks it against the metadata the
//! uploader recorded: the chunk count, the size and the md5 of the original
//! file. This is the only thing md5 is for — a module is identified by its
//! name, so a hash mismatch means the upload landed corrupted, not that we
//! fetched the wrong module. Writes the body to `body` and returns true on
//! success; on failure `error` explains which check failed.
//! An empty `expectedMd5` or a zero `expectedSize` skips that check, for rows
//! written before the field was populated.
bool JoinAndVerifyBlobs(
    const TVector<TString>& chunks,
    ui64 expectedChunkCount,
    ui64 expectedSize,
    TStringBuf expectedMd5,
    TString& body,
    TString& error);

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

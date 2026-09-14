#include "blob_chunks.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NUdfStore {

TVector<TString> SplitBlob(TStringBuf data, ui64 chunkSize) {
    Y_ABORT_UNLESS(chunkSize > 0);
    TVector<TString> result;
    if (data.empty()) {
        return result;
    }
    result.reserve((data.size() + chunkSize - 1) / chunkSize);
    for (ui64 offset = 0; offset < data.size(); offset += chunkSize) {
        const ui64 size = Min<ui64>(chunkSize, data.size() - offset);
        result.emplace_back(data.SubString(offset, size));
    }
    return result;
}

TString JoinBlobs(const TVector<TString>& chunks) {
    ui64 total = 0;
    for (const auto& chunk : chunks) {
        total += chunk.size();
    }
    TString result;
    result.reserve(total);
    for (const auto& chunk : chunks) {
        result.append(chunk);
    }
    return result;
}

namespace {

NKikimrSchemeOp::TColumnDescription MakeCol(const TString& name, const char* type) {
    NKikimrSchemeOp::TColumnDescription col;
    col.SetName(name);
    col.SetType(type);
    return col;
}

} // namespace

TVector<NKikimrSchemeOp::TColumnDescription> TSourceChunkSchema::GetColumnDescription() {
    return {
        MakeCol(OwnerKeyColName, "Utf8"),
        MakeCol(ChunkIdxColName, "Uint64"),
        MakeCol(DataColName, "String"),
    };
}

TVector<TString> TSourceChunkSchema::GetPk() {
    return {OwnerKeyColName, ChunkIdxColName};
}

TVector<NKikimrSchemeOp::TColumnDescription> TArtifactChunkSchema::GetColumnDescription() {
    return {
        MakeCol(IdColName, "Utf8"),
        MakeCol(KindColName, "Utf8"),
        MakeCol(BlobKindColName, "Utf8"),
        MakeCol(ChunkIdxColName, "Uint64"),
        MakeCol(DataColName, "String"),
    };
}

TVector<TString> TArtifactChunkSchema::GetPk() {
    return {IdColName, KindColName, BlobKindColName, ChunkIdxColName};
}

} // namespace NKikimr::NUdfStore

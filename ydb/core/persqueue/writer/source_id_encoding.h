#pragma once

#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <util/generic/fwd.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

enum class ESourceIdTableGeneration {
    SrcIdMeta2,
    PartitionMapping
};

TString GetSelectSourceIdQuery(const TString& root, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2);
TString GetUpdateSourceIdQuery(const TString& root, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2);
TString GetUpdateAccessTimeQuery(const TString& root, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2);

TString GetSelectSourceIdQueryFromPath(const TString& path, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2);
TString GetUpdateSourceIdQueryFromPath(const TString& path, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2);
TString GetUpdateAccessTimeQueryFromPath(const TString& path, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2);

namespace NSourceIdEncoding {

TString EncodeSimple(const TString& sourceId);
TString Encode(const TString& sourceId);
TString Decode(const TString& encodedSourceId);
bool IsValidEncoded(const TString& encodedSourceId);

struct TEncodedSourceId {
    TString OriginalSourceId;
    TString EscapedSourceId;
    ui32 Hash = 0;
    ui64 KeysHash = 0;
    ESourceIdTableGeneration Generation;
};

void SetHashToTxParams(NClient::TParameters& parameters, const TEncodedSourceId& encodedSrcId);

void SetHashToTParamsBuilder(NYdb::TParamsBuilder& builder, const TEncodedSourceId& encodedSrcId);

TEncodedSourceId EncodeSrcId(const TString& topic, const TString& userSourceId, ESourceIdTableGeneration generation);

} // NSourceIdEncoding

} // NKikimr::NPQ

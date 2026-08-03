#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>

#include <util/generic/fwd.h>
#include <util/generic/string.h>

namespace NKikimr::NClient {
    class TParameters;
}

namespace NKikimr::NPQ {

enum class ESourceIdTableGeneration {
    SrcIdMeta2,
    PartitionMapping
};

// When withFallback is true, the query additionally reads/writes a second (legacy) Topic value
TString GetSelectSourceIdQuery(const TString& root, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2, bool withFallback = false);
TString GetUpdateSourceIdQuery(const TString& root, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2, bool withFallback = false);
TString GetUpdateAccessTimeQuery(const TString& root, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2, bool withFallback = false);

TString GetSelectSourceIdQueryFromPath(const TString& path, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2, bool withFallback = false);
TString GetUpdateSourceIdQueryFromPath(const TString& path, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2, bool withFallback = false);
TString GetUpdateAccessTimeQueryFromPath(const TString& path, ESourceIdTableGeneration = ESourceIdTableGeneration::SrcIdMeta2, bool withFallback = false);

// Builds the PathId-encoded Topic column value.
TString EncodeTopicWithPathId(ui64 pathId, const TString& topicName);

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

#include "metadata_initializers.h"
#include "source_id_encoding.h"

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/digest/city.h>
#include <util/digest/murmur.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/hex.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NKikimr::NPQ {

TString GetSelectSourceIdQueryFromPath(const TString& path, ESourceIdTableGeneration generation) {
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return TStringBuilder() << "--!syntax_v1\n"
                   "DECLARE $Hash AS Uint32; "
                   "DECLARE $Topic AS Utf8; "
                   "DECLARE $SourceId AS Utf8;\n"
                   "SELECT Partition, CreateTime, AccessTime, SeqNo FROM `" << path << "` "
                   "WHERE Hash == $Hash AND Topic == $Topic AND SourceId == $SourceId;";
        case ESourceIdTableGeneration::PartitionMapping:
            return TStringBuilder() << "--!syntax_v1\n"
                   "DECLARE $Hash AS Uint64; "
                   "DECLARE $Topic AS Utf8; "
                   "DECLARE $SourceId AS Utf8;\n"
                   "SELECT Partition, CreateTime, AccessTime, SeqNo FROM `"
                   << NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath()
                   << "` WHERE Hash == $Hash AND Topic == $Topic AND ProducerId == $SourceId;";
        default:
            Y_ABORT();
    }
}

TString GetSelectSourceIdQuery(const TString& root, ESourceIdTableGeneration generation) {
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return GetSelectSourceIdQueryFromPath(root + "/SourceIdMeta2", generation);
        case ESourceIdTableGeneration::PartitionMapping:
            return GetSelectSourceIdQueryFromPath(
                    NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath(),
                    generation
            );
        default:
            Y_ABORT();
    }
}

TString GetUpdateSourceIdQueryFromPath(const TString& path, ESourceIdTableGeneration generation) {
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return TStringBuilder() << "--!syntax_v1\n"
                   "DECLARE $SourceId AS Utf8; "
                   "DECLARE $Topic AS Utf8; "
                   "DECLARE $Hash AS Uint32; "
                   "DECLARE $Partition AS Uint32; "
                   "DECLARE $CreateTime AS Uint64; "
                   "DECLARE $AccessTime AS Uint64;"
                   "DECLARE $SeqNo AS Uint64;\n"
                   "UPSERT INTO `" << path << "` (Hash, Topic, SourceId, CreateTime, AccessTime, Partition, SeqNo) VALUES "
                                              "($Hash, $Topic, $SourceId, $CreateTime, $AccessTime, $Partition, $SeqNo);";
        case ESourceIdTableGeneration::PartitionMapping:
            return TStringBuilder() << "--!syntax_v1\n"
                   "DECLARE $SourceId AS Utf8; "
                   "DECLARE $Topic AS Utf8; "
                   "DECLARE $Hash AS Uint64; "
                   "DECLARE $Partition AS Uint32; "
                   "DECLARE $CreateTime AS Uint64; "
                   "DECLARE $AccessTime AS Uint64; "
                   "DECLARE $SeqNo AS Uint64;\n"
                   "UPSERT INTO `" << NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath()
                    << "` (Hash, Topic, ProducerId, CreateTime, AccessTime, Partition, SeqNo) VALUES "
                                              "($Hash, $Topic, $SourceId, $CreateTime, $AccessTime, $Partition, $SeqNo);";
        default:
            Y_ABORT();
    }
}

TString GetUpdateAccessTimeQueryFromPath(const TString& path, ESourceIdTableGeneration generation) {
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return TStringBuilder() << "--!syntax_v1\n"
                   "DECLARE $SourceId AS Utf8; "
                   "DECLARE $Topic AS Utf8; "
                   "DECLARE $Hash AS Uint32; "
                   "DECLARE $Partition AS Uint32; "
                   "DECLARE $CreateTime AS Uint64; "
                   "DECLARE $AccessTime AS Uint64;\n"
                   "UPDATE `" << path << "` "
                   "SET AccessTime = $AccessTime "
                   "WHERE Hash = $Hash AND Topic = $Topic AND SourceId = $SourceId AND Partition = $Partition;";
        case ESourceIdTableGeneration::PartitionMapping:
            return TStringBuilder() << "--!syntax_v1\n"
                   "DECLARE $SourceId AS Utf8; "
                   "DECLARE $Topic AS Utf8; "
                   "DECLARE $Hash AS Uint64; "
                   "DECLARE $Partition AS Uint32; "
                   "DECLARE $CreateTime AS Uint64; "
                   "DECLARE $AccessTime AS Uint64;\n"
                   "UPDATE `" << NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath() << "` "
                   "SET AccessTime = $AccessTime "
                   "WHERE Hash = $Hash AND Topic = $Topic AND ProducerId = $SourceId AND Partition = $Partition;";
        default:
            Y_ABORT();
    }
}

TString GetUpdateSourceIdQuery(const TString& root, ESourceIdTableGeneration generation) {
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return GetUpdateSourceIdQueryFromPath(root + "/SourceIdMeta2", generation);
        case ESourceIdTableGeneration::PartitionMapping:
            return GetUpdateSourceIdQueryFromPath(
                    NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath(),
                    generation
            );
        default:
            Y_ABORT();
    }
}

TString GetUpdateAccessTimeQuery(const TString& root, ESourceIdTableGeneration generation) {
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return GetUpdateAccessTimeQueryFromPath(root + "/SourceIdMeta2", generation);
        case ESourceIdTableGeneration::PartitionMapping:
            return GetUpdateAccessTimeQueryFromPath(
                    NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant()->GetStorageTablePath(),
                    generation
            );
        default:
            Y_ABORT();
    }
}

namespace NSourceIdEncoding {

static const ui32 MURMUR_ARRAY_SEED = 0x9747b28c;

struct TTags {
    static constexpr char Simple = 0;
    static constexpr char Base64 = 1;
};

static constexpr TStringBuf Base64Prefix = "base64:";

TString EncodeSimple(const TString& sourceId) {
    return TString(1, TTags::Simple) + sourceId;
}

TString DecodeSimple(const TString& sourceId) {
    Y_ABORT_UNLESS(!sourceId.empty() && sourceId[0] == TTags::Simple);
    return sourceId.substr(1);
}

TString EncodeBase64(const TString& sourceId) {
    const auto size = sourceId.size();
    const auto prefixSize = Base64Prefix.size();

    if (size == prefixSize || sourceId.find('=') != TString::npos) {
        ythrow yexception() << "started from \"" << Base64Prefix << "\" must be valid base64 encoded string, without padding";
    }

    const auto remainder = (size - prefixSize) % 4;
    const auto padding = remainder ? TString(4 - remainder, '=') : "";
    const auto effective = sourceId.substr(prefixSize) + padding;

    try {
        return TString(1, TTags::Base64) + Base64StrictDecode(effective);
    } catch (yexception& e) {
        ythrow yexception() << "started from \"" << Base64Prefix << "\" must be valid base64 encoded string: " << e.what();
    }
}

TString DecodeBase64(const TString& sourceId) {
    Y_ABORT_UNLESS(!sourceId.empty() && sourceId[0] == TTags::Base64);
    return Base64Prefix + StripStringRight(Base64EncodeUrl(sourceId.substr(1)), EqualsStripAdapter(','));
}

TString Encode(const TString& sourceId) {
    if (sourceId.StartsWith(Base64Prefix)) {
        return EncodeBase64(sourceId);
    } else {
        return EncodeSimple(sourceId);
    }
}

TString Decode(const TString& sourceId) {
    Y_ABORT_UNLESS(!sourceId.empty());

    switch (sourceId[0]) {
    case TTags::Simple:
        return DecodeSimple(sourceId);
    case TTags::Base64:
        return DecodeBase64(sourceId);
    default:
        return sourceId;
    }
}

bool IsValidEncoded(const TString& sourceId) {
    if (sourceId.empty()) {
        return false;
    }

    switch (sourceId[0]) {
    case TTags::Simple:
    case TTags::Base64:
        return true;
    default:
        return false;
    }
}

template <class... TArgs>
ui64 GetKeysHash(TArgs&&... args) {
    return CityHash64(Join("#", args...));
}

TEncodedSourceId EncodeSrcId(const TString& topic, const TString& userSourceId, ESourceIdTableGeneration generation) {
    TEncodedSourceId res;
    res.OriginalSourceId = userSourceId;
    TString encodedSourceId = Encode(userSourceId);
    res.EscapedSourceId = HexEncode(encodedSourceId);
    switch (generation) {
        case ESourceIdTableGeneration::SrcIdMeta2: {
            TString s = topic + encodedSourceId;
            res.Hash = MurmurHash<ui32>(s.c_str(), s.size(), MURMUR_ARRAY_SEED);
            break;
        }
        case ESourceIdTableGeneration::PartitionMapping:
            res.KeysHash = GetKeysHash(topic, encodedSourceId);
            break;
    }
    res.Generation = generation;
    return res;
}

void SetHashToTxParams(NClient::TParameters& parameters, const TEncodedSourceId& encodedSrcId) {
    switch (encodedSrcId.Generation) {
        case ESourceIdTableGeneration::PartitionMapping:
            parameters["$Hash"] = encodedSrcId.KeysHash;
            return;
        case ESourceIdTableGeneration::SrcIdMeta2:
            parameters["$Hash"] = encodedSrcId.Hash;
            return;

    }
}

void SetHashToTParamsBuilder(NYdb::TParamsBuilder& builder, const TEncodedSourceId& encodedSrcId) {
    switch (encodedSrcId.Generation) {
        case ESourceIdTableGeneration::PartitionMapping:
            builder.AddParam("$Hash").Uint64(encodedSrcId.KeysHash).Build();
            return;
        case ESourceIdTableGeneration::SrcIdMeta2:
            builder.AddParam("$Hash").Uint32(encodedSrcId.Hash).Build();
            return;
    }
}

} // NSourceIdEncoding

} // NKikimr::NPQ

#pragma once

#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/services/metadata/abstract/request_features.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <optional>
#include <map>

namespace NKikimr::NArrow {

class TCompressionDiff {
private:
    std::optional<arrow::Compression::type> Codec;
    std::optional<int> Level;
public:
    bool IsEmpty() const {
        return !Level && !Codec;
    }
    NKikimrSchemeOp::TCompressionOptions SerializeToProto() const;
    bool DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& proto);
    TConclusionStatus DeserializeFromRequestFeatures(NYql::TFeaturesExtractor& features);
    const std::optional<arrow::Compression::type>& GetCodec() const {
        return Codec;
    }
    const std::optional<int>& GetLevel() const {
        return Level;
    }
};
}

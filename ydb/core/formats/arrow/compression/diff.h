#pragma once

#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <optional>
#include <map>

namespace NKikimr::NArrow {

class TCompressionDiff {
private:
    YDB_READONLY_DEF(std::optional<arrow::Compression::type>, Codec);
    YDB_READONLY_DEF(std::optional<int>, Level);
public:
    bool IsEmpty() const {
        return !Level && !Codec;
    }
    NKikimrSchemeOp::TCompressionOptions SerializeToProto() const;
    bool DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& proto);
    TConclusionStatus DeserializeFromRequestFeatures(const std::map<TString, TString>& features);
};
}

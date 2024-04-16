#pragma once

#include <ydb/library/conclusion/result.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include "diff.h"

namespace NKikimr::NArrow {

class TCompression {
private:
    arrow::Compression::type Codec = arrow::Compression::LZ4_FRAME;
    std::optional<int> Level;
    TCompression() = default;

    TConclusionStatus Validate() const;
    friend class TCompressionDiff;
public:

    static std::unique_ptr<arrow::util::Codec> BuildDefaultCodec();

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& compression);

    NKikimrSchemeOp::TCompressionOptions SerializeToProto() const;

    static TConclusion<TCompression> BuildFromProto(const NKikimrSchemeOp::TCompressionOptions& compression);

    explicit TCompression(const arrow::Compression::type codec, std::optional<int> level = {})
        : Codec(codec)
        , Level(level)
    {

    }

    TString DebugString() const;
    std::unique_ptr<arrow::util::Codec> BuildArrowCodec() const;

};

}

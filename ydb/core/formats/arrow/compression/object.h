#pragma once

#include <ydb/library/conclusion/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include "diff.h"

namespace NKikimr::NArrow {

struct TCompression {
private:
    arrow::Compression::type Codec = arrow::Compression::LZ4_FRAME;
    std::optional<int> Level;
    TCompression() = default;

    TConclusionStatus Validate() const;

public:

    TConclusionStatus ApplyDiff(const TCompressionDiff& diff);

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& compression);

    NKikimrSchemeOp::TCompressionOptions SerializeToProto() const;

    static const TCompression& Default() {
        static TCompression result;
        return result;
    }

    explicit TCompression(const arrow::Compression::type codec, std::optional<int> level = {})
        : Codec(codec)
        , Level(level)
    {

    }

    TString DebugString() const;
    std::unique_ptr<arrow::util::Codec> BuildArrowCodec() const;

};

}

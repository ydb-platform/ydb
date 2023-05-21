#include "object.h"
#include "parsing.h"
#include <ydb/core/formats/arrow/common/validation.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow {

TConclusionStatus NKikimr::NArrow::TCompression::Validate() const {
    if (Level) {
        auto codec = TStatusValidator::GetValid(arrow::util::Codec::Create(Codec));
        const int levelMin = codec->minimum_compression_level();
        const int levelMax = codec->maximum_compression_level();
        if (Level && (*Level < levelMin || levelMax < *Level)) {
            TStringBuilder sb;
            sb << "incorrect level for codec. have to be: [" << levelMin << ":" << levelMax << "]";
            return TConclusionStatus::Fail(sb);
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus NKikimr::NArrow::TCompression::ApplyDiff(const TCompressionDiff& diff) {
    TCompression merged = *this;
    if (diff.GetCodec()) {
        merged.Codec = *diff.GetCodec();
    }
    if (diff.GetLevel()) {
        merged.Level = *diff.GetLevel();
    }
    auto validation = merged.Validate();
    if (!validation) {
        return validation;
    }
    std::swap(*this, merged);
    return TConclusionStatus::Success();
}

TConclusionStatus TCompression::DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& compression) {
    if (compression.HasCompressionCodec()) {
        auto codecOpt = NArrow::CompressionFromProto(compression.GetCompressionCodec());
        if (!codecOpt) {
            return TConclusionStatus::Fail("cannot parse codec type from proto");
        }
        Codec = *codecOpt;
    }
    if (compression.HasCompressionLevel()) {
        Level = compression.GetCompressionLevel();
    }
    return Validate();
}

NKikimrSchemeOp::TCompressionOptions TCompression::SerializeToProto() const {
    NKikimrSchemeOp::TCompressionOptions result;
    result.SetCompressionCodec(NArrow::CompressionToProto(Codec));
    if (Level) {
        result.SetCompressionLevel(*Level);
    }
    return result;
}

TString TCompression::DebugString() const {
    TStringBuilder sb;
    sb << arrow::util::Codec::GetCodecAsString(Codec) << ":" << Level.value_or(arrow::util::kUseDefaultCompressionLevel);
    return sb;
}

std::unique_ptr<arrow::util::Codec> TCompression::BuildArrowCodec() const {
    return NArrow::TStatusValidator::GetValid(
        arrow::util::Codec::Create(
            Codec, Level.value_or(arrow::util::kUseDefaultCompressionLevel)));
}

}

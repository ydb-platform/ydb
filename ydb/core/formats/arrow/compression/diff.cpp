#include "diff.h"
#include "object.h"
#include "parsing.h"
#include <util/string/cast.h>

namespace NKikimr::NArrow {

NKikimrSchemeOp::TCompressionOptions TCompressionDiff::SerializeToProto() const {
    NKikimrSchemeOp::TCompressionOptions result;
    if (Level) {
        result.SetCompressionLevel(*Level);
    }
    if (Codec) {
        result.SetCompressionCodec(CompressionToProto(*Codec));
    }
    return result;
}

TConclusionStatus TCompressionDiff::DeserializeFromRequestFeatures(NYql::TFeaturesExtractor& features) {
    {
        auto fValue = features.Extract("COMPRESSION.TYPE");
        if (fValue) {
            Codec = NArrow::CompressionFromString(*fValue);
            if (!Codec) {
                return TConclusionStatus::Fail("cannot parse COMPRESSION.TYPE as arrow::Compression");
            }
        }
    }
    {
        auto fValue = features.Extract("COMPRESSION.LEVEL");
        if (fValue) {
            ui32 level;
            if (!TryFromString<ui32>(*fValue, level)) {
                return TConclusionStatus::Fail("cannot parse COMPRESSION.LEVEL as ui32");
            }
            Level = level;
        }
    }
    return TConclusionStatus::Success();
}

bool TCompressionDiff::DeserializeFromProto(const NKikimrSchemeOp::TCompressionOptions& proto) {
    if (proto.HasCompressionLevel()) {
        Level = proto.GetCompressionLevel();
    }
    if (proto.HasCompressionCodec()) {
        Codec = CompressionFromProto(proto.GetCompressionCodec());
        if (!Codec) {
            return false;
        }
    }
    return true;
}

NKikimr::TConclusionStatus TCompressionDiff::Apply(std::optional<TCompression>& settings) const {
    if (IsEmpty()) {
        return TConclusionStatus::Success();
    }
    TCompression merged;
    if (!!settings) {
        merged = *settings;
    }
    if (Codec) {
        merged.Codec = *Codec;
    }
    if (Level) {
        merged.Level = *Level;
    }
    auto validation = merged.Validate();
    if (!validation) {
        return validation;
    }
    settings = merged;
    return TConclusionStatus::Success();
}

}

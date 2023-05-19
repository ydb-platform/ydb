#include "diff.h"
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

TConclusionStatus TCompressionDiff::DeserializeFromRequestFeatures(const std::map<TString, TString>& features) {
    {
        auto it = features.find("COMPRESSION.TYPE");
        if (it != features.end()) {
            Codec = NArrow::CompressionFromString(it->second);
            if (!Codec) {
                return TConclusionStatus::Fail("cannot parse COMPRESSION.TYPE as arrow::Compression");
            }
        }
    }
    {
        auto it = features.find("COMPRESSION.LEVEL");
        if (it != features.end()) {
            ui32 level;
            if (!TryFromString<ui32>(it->second, level)) {
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

}

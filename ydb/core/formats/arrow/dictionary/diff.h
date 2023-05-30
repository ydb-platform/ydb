#pragma once

#include <ydb/library/conclusion/status.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/services/metadata/abstract/request_features.h>
#include <optional>
#include <map>

namespace NKikimr::NArrow::NDictionary {

class TEncodingSettings;

class TEncodingDiff {
private:
    std::optional<bool> Enabled;
    bool IsEmpty() const {
        return !Enabled;
    }
public:
    NKikimrSchemeOp::TDictionaryEncodingSettings SerializeToProto() const;
    bool DeserializeFromProto(const NKikimrSchemeOp::TDictionaryEncodingSettings& proto);
    TConclusionStatus DeserializeFromRequestFeatures(NYql::TFeaturesExtractor& features);
    const std::optional<bool>& GetEnabled() const {
        return Enabled;
    }
    TConclusionStatus Apply(std::optional<TEncodingSettings>& settings) const;
};
}

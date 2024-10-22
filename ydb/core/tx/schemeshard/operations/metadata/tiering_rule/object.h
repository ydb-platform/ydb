#pragma once

#include <ydb/core/tx/schemeshard/operations/metadata/abstract/info.h>
#include <ydb/core/tx/tiering/rule/info.h>

namespace NKikimr::NSchemeShard::NOperations {

class TTieringRuleInfo final: public TMetadataObjectInfoImpl<NColumnShard::NTiers::TTieringRuleInfo> {
private:
    using TTieringRule = NColumnShard::NTiers::TTieringRuleInfo;

public:
    std::optional<TTieringRule> DoDeserializeFromProto(const NKikimrSchemeOp::TMetadataObjectProperties& proto) const override {
        TTieringRule object;
        if (!object.DeserializeFromProto(proto.GetTieringRule())) {
            return std::nullopt;
        }
        return object;
    }

    bool DoApplyPatch(TTieringRule& object, const NKikimrSchemeOp::TMetadataObjectProperties& patch) const override {
        return object.ApplyPatch(patch.GetTieringRule());
    }

    NKikimrSchemeOp::TMetadataObjectProperties DoSerializeToProto(const TTieringRule& object) const override {
        NKikimrSchemeOp::TMetadataObjectProperties serialized;
        *serialized.MutableTieringRule() = object.SerializeToProto();
        return serialized;
    }
};
}
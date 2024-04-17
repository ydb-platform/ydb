#pragma once

#include <ydb/library/conclusion/result.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>

namespace NKikimr::NArrow::NDictionary {

class TEncodingSettings {
private:
    bool Enabled = false;
    TEncodingSettings() = default;
    friend class TEncodingDiff;
public:
    bool IsEqualTo(const TEncodingSettings& item) const {
        return Enabled == item.Enabled;
    }
    NTransformation::ITransformer::TPtr BuildEncoder() const;
    NTransformation::ITransformer::TPtr BuildDecoder() const;

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TDictionaryEncodingSettings& proto);
    static TConclusion<TEncodingSettings> BuildFromProto(const NKikimrSchemeOp::TDictionaryEncodingSettings& proto) {
        TEncodingSettings result;
        auto resultParse = result.DeserializeFromProto(proto);
        if (resultParse.IsFail()) {
            return resultParse;
        }
        return result;
    }

    NKikimrSchemeOp::TDictionaryEncodingSettings SerializeToProto() const;

    explicit TEncodingSettings(const bool enabled)
        : Enabled(enabled)
    {

    }

    TString DebugString() const;
};

}

#include "kqp_script_execution_compression.h"

#include <util/generic/size_literals.h>

#include <library/cpp/blockcodecs/codecs.h>

#include <ydb/core/tx/datashard/const.h>

namespace NKikimr::NKqp {

namespace {

const NBlockCodecs::ICodec* ProvideCodec(const TString& compressionMethod) {
    const auto codec = NBlockCodecs::Codec(compressionMethod);
    if (!codec) {
        throw yexception() << "Unable to find codec for compression method " << compressionMethod;
    }

    return codec;
}

TString TruncateString(const TString& str, size_t size) {
    return str.substr(0, std::min(str.size(), size)) + "...\n(TRUNCATED)";
}

}  // anonymous namespace

TCompressor::TCompressor(const TString& compressionMethod, ui64 minCompressionSize)
    : CompressionMethod(compressionMethod)
    , MinCompressionSize(minCompressionSize)
    , Codec(compressionMethod ? ProvideCodec(compressionMethod) : nullptr)
{}

bool TCompressor::IsEnabled() const {
    return Codec != nullptr;
}

std::pair<TString, TString> TCompressor::Compress(const TString& data) const {
    if (!IsEnabled() || data.size() < MinCompressionSize) {
        return {"", data};
    }

    return {CompressionMethod, Codec->Encode(data)};
}

TString TCompressor::Decompress(const TString& data) const {
    if (!IsEnabled()) {
        return data;
    }

    return Codec->Decode(data);
}

TScriptArtifacts CompressScriptArtifacts(const std::optional<TString>& ast, const std::optional<TString>& plan, const TCompressor& compressor) {
    TScriptArtifacts result;

    const auto saveValue = [&](const std::optional<TString>& value, std::optional<TString>& resultValue, std::optional<TString>& resultCompressionMethod, const TString& name) {
        if (!value) {
            return;
        }

        std::tie(resultCompressionMethod, resultValue) = compressor.Compress(*value);
        if (resultValue->size() > NDataShard::NLimits::MaxWriteValueSize) {
            result.Issues.AddIssue(
                NYql::TIssue(TStringBuilder() << "Query " << name << " size is " << resultValue->size() << " bytes, that is larger than allowed limit " << NDataShard::NLimits::MaxWriteValueSize << " bytes, " << name << " was truncated")
                    .SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO)
            );
            resultValue = TruncateString(*value, NDataShard::NLimits::MaxWriteValueSize - 1_KB);
            resultCompressionMethod = std::nullopt;
        }
    };

    saveValue(ast, result.Ast, result.AstCompressionMethod, "ast");
    saveValue(plan, result.Plan, result.PlanCompressionMethod, "plan");

    return result;
}

}  // namespace NKikimr::NKqp

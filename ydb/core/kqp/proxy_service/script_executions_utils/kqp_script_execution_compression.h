#pragma once

#include <util/generic/string.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NBlockCodecs {
    struct ICodec;
}

namespace NKikimr::NKqp {

class TCompressor {
public:
    explicit TCompressor(const TString& compressionMethod, ui64 minCompressionSize = 0);

    bool IsEnabled() const;

    // return (compressionMethod, possiblyCompressedData)
    // if compressionMethod is empty - data is not compressed
    std::pair<TString, TString> Compress(const TString& data) const;

    TString Decompress(const TString& data) const;

private:
    const TString CompressionMethod;
    const ui64 MinCompressionSize;
    const NBlockCodecs::ICodec* Codec;
};

struct TScriptArtifacts {
    NYql::TIssues Issues;
    std::optional<TString> Ast;
    std::optional<TString> AstCompressionMethod;
    std::optional<TString> Plan;
    std::optional<TString> PlanCompressionMethod;
};

TScriptArtifacts CompressScriptArtifacts(const std::optional<TString>& ast, const std::optional<TString>& plan, const TCompressor& compressor);

}  // namespace NKikimr::NKqp

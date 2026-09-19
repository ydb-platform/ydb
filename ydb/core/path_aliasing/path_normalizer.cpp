#include "path_normalizer.h"

#include <ydb/core/protos/config.pb.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/yexception.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace NKikimr::NPathAliasing {

    struct TPathNormalizer::TImpl {
        struct TRule {
            std::unique_ptr<const re2::RE2> Pattern;
            std::string Replacement;
        };

        std::vector<TRule> Rules;
    };

    TPathNormalizer::TPathNormalizer(const NKikimrConfig::TPathRewriteConfig& config) {
        if (config.RulesSize() == 0) {
            return;
        }

        auto impl = std::make_shared<TImpl>();
        impl->Rules.reserve(config.RulesSize());

        size_t index = 0;
        for (const auto& rule : config.GetRules()) {
            ++index;
            Y_ENSURE(rule.HasPattern(), "path_rewrite_config rule " << index << ": missing pattern");
            Y_ENSURE(rule.HasReplacement(), "path_rewrite_config rule " << index << ": missing replacement");

            const auto& pattern = rule.GetPattern();
            auto compiled = std::make_unique<re2::RE2>(
                re2::StringPiece(pattern.data(), pattern.size()), re2::RE2::Quiet);
            Y_ENSURE(compiled->ok(),
                     "path_rewrite_config rule " << index << ": invalid pattern: " << compiled->error());

            const auto& replacement = rule.GetReplacement();
            std::string error;
            Y_ENSURE(compiled->CheckRewriteString(
                         re2::StringPiece(replacement.data(), replacement.size()), &error),
                     "path_rewrite_config rule " << index << ": invalid replacement: " << error);

            impl->Rules.push_back({std::move(compiled), replacement});
        }

        Impl = std::move(impl);
    }

    TString TPathNormalizer::NormalizePath(TStringBuf path) const {
        if (path.empty() || !Impl) {
            return TString(path);
        }

        std::string result(path.data(), path.size());
        for (const auto& rule : Impl->Rules) {
            if (re2::RE2::Replace(&result, *rule.Pattern, rule.Replacement)) {
                return TString(result);
            }
        }

        return TString(path);
    }

} // namespace NKikimr::NPathAliasing

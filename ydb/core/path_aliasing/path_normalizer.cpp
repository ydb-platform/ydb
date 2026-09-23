#include "path_normalizer.h"

#include <ydb/core/protos/config.pb.h>

#include <util/generic/yexception.h>

#include <memory>
#include <utility>
#include <vector>

namespace NKikimr::NPathAliasing {

    struct TPathNormalizer::TImpl {
        struct TRule {
            TString Src;
            TString Dst;
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
            const TStringBuf src(rule.GetSrc());
            const TStringBuf dst(rule.GetDst());
            Y_ENSURE(src.StartsWith("/"), "path_rewrite_config rule " << index << ": src must be a nonempty absolute path");
            Y_ENSURE(dst.StartsWith("/"), "path_rewrite_config rule " << index << ": dst must be a nonempty absolute path");

            impl->Rules.push_back({TString(src), TString(dst)});
        }

        Impl = std::move(impl);
    }

    TString TPathNormalizer::NormalizePath(TStringBuf path) const {
        if (!Impl || !path.StartsWith("/")) {
            return TString(path);
        }

        for (const auto& rule : Impl->Rules) {
            if (path.StartsWith(rule.Src)
                && (rule.Src.EndsWith("/") || path.size() == rule.Src.size() || path[rule.Src.size()] == '/')) {
                TString result(rule.Dst);
                result.append(path.data() + rule.Src.size(), path.size() - rule.Src.size());
                return result;
            }
        }

        return TString(path);
    }

} // namespace NKikimr::NPathAliasing

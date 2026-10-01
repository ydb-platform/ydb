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
            TString src(rule.GetSrc());
            const TStringBuf dst(rule.GetDst());
            Y_ENSURE(src.StartsWith("/"), "resource_path_prefix_mapping rule " << index << ": src must be a nonempty absolute path");
            Y_ENSURE(dst.StartsWith("/"), "resource_path_prefix_mapping rule " << index << ": dst must be a nonempty absolute path");

            while (src.size() > 1 && src.back() == '/') {
                src.pop_back();
            }
            impl->Rules.push_back({std::move(src), TString(dst)});
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
                if (path.size() > rule.Src.size() && result.back() != '/' && path[rule.Src.size()] != '/') {
                    result.push_back('/');
                }
                result.append(path.data() + rule.Src.size(), path.size() - rule.Src.size());
                size_t write = 0;
                for (size_t read = 0; read < result.size(); ++read) {
                    const char c = result[read];
                    if (c != '/' || write == 0 || result[write - 1] != '/') {
                        result[write++] = c;
                    }
                }
                if (write > 1 && result[write - 1] == '/') {
                    --write;
                }
                result.resize(write);
                return result;
            }
        }

        return TString(path);
    }

} // namespace NKikimr::NPathAliasing

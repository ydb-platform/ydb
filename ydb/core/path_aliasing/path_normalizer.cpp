#include "path_normalizer.h"

#include <ydb/core/base/path.h>
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

        std::vector<TImpl::TRule> prefixes;
        prefixes.reserve(impl->Rules.size());
        for (const auto& rule : impl->Rules) {
            prefixes.push_back({CanonizePath(rule.Src), CanonizePath(rule.Dst)});
        }
        const auto isPrefix = [](TStringBuf prefix, TStringBuf path) {
            return path.StartsWith(prefix)
                && (path.size() == prefix.size() || path[prefix.size()] == '/');
        };
        for (size_t i = 0; i < prefixes.size(); ++i) {
            for (size_t j = 0; j < prefixes.size(); ++j) {
                Y_ENSURE(!isPrefix(prefixes[j].Src, prefixes[i].Dst)
                    && !isPrefix(prefixes[i].Dst, prefixes[j].Src),
                    "resource_path_prefix_mapping rule " << i + 1 << ": dst '" << impl->Rules[i].Dst
                    << "' overlaps src '" << impl->Rules[j].Src << "' of rule " << j + 1
                    << "; alias chains and cycles are not allowed");
            }
        }

        Impl = std::move(impl);
    }

    TString TPathNormalizer::NormalizePath(TStringBuf path) const {
        if (!Impl || !path.StartsWith("/")) {
            return TString(path);
        }

        TStringBuf normalizedPath = path;
        while (normalizedPath.StartsWith("//")) {
            normalizedPath = normalizedPath.SubStr(1);
        }

        for (const auto& rule : Impl->Rules) {
            if (normalizedPath.StartsWith(rule.Src)
                && (rule.Src.EndsWith("/") || normalizedPath.size() == rule.Src.size() || normalizedPath[rule.Src.size()] == '/')) {
                TString result(rule.Dst);
                if (normalizedPath.size() > rule.Src.size() && result.back() != '/' && normalizedPath[rule.Src.size()] != '/') {
                    result.push_back('/');
                }
                result.append(normalizedPath.data() + rule.Src.size(), normalizedPath.size() - rule.Src.size());
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

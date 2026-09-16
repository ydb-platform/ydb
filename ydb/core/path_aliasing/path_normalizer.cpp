#include "path_normalizer.h"

#include <ydb/core/protos/config.pb.h>

#include <contrib/libs/re2/re2/re2.h>
#include <library/cpp/openssl/crypto/sha.h>

#include <util/generic/yexception.h>
#include <util/string/hex.h>

#include <array>
#include <string>
#include <utility>
#include <vector>

namespace NKikimr::NPathAliasing {
    namespace {

        void UpdateSize(NOpenSsl::NSha256::TCalcer& fingerprint, ui64 value) {
            // Specify byte order so fingerprints agree across builds and architectures.
            std::array<ui8, sizeof(value)> encoded;
            for (auto& byte : encoded) {
                byte = static_cast<ui8>(value & 0xff);
                value >>= 8;
            }
            fingerprint.Update(encoded.data(), encoded.size());
        }

        void UpdateField(NOpenSsl::NSha256::TCalcer& fingerprint, TStringBuf value) {
            UpdateSize(fingerprint, value.size());
            fingerprint.Update(value);
        }

    } // namespace

    struct TPathNormalizer::TImpl {
        struct TRule {
            std::unique_ptr<const re2::RE2> Pattern;
            TString Replacement;
            int SubmatchCount;
        };

        std::vector<TRule> Rules;
        TString Fingerprint;
    };

    TPathNormalizer::TPathNormalizer(const NKikimrConfig::TPathRewriteConfig& config) {
        if (config.RulesSize() == 0) {
            return;
        }

        auto impl = std::make_shared<TImpl>();
        impl->Rules.reserve(config.RulesSize());
        NOpenSsl::NSha256::TCalcer fingerprint;
        UpdateField(fingerprint, "YDB path rewrite rules v1");
        UpdateSize(fingerprint, config.RulesSize());

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
            const re2::StringPiece rewrite(replacement.data(), replacement.size());
            std::string error;
            Y_ENSURE(compiled->CheckRewriteString(rewrite, &error),
                     "path_rewrite_config rule " << index << ": invalid replacement: " << error);

            const int submatchCount = 1 + re2::RE2::MaxSubmatch(rewrite);
            impl->Rules.push_back({std::move(compiled), replacement, submatchCount});
            UpdateField(fingerprint, pattern);
            UpdateField(fingerprint, replacement);
        }

        const auto digest = fingerprint.Final();
        impl->Fingerprint = HexEncode(digest.data(), digest.size());
        Impl = std::move(impl);
    }

    bool TPathNormalizer::Empty() const noexcept {
        return !Impl;
    }

    bool IsValidRewrittenPath(TStringBuf path) noexcept {
        if (path.empty() || path.front() != '/' || path.find('\0') != TStringBuf::npos) {
            return false;
        }
        size_t start = 1;
        while (start <= path.size()) {
            const size_t end = path.find('/', start);
            const TStringBuf part = path.SubStr(start,
                                                end == TStringBuf::npos ? path.size() - start : end - start);
            if (part == "." || part == "..") {
                return false;
            }
            if (end == TStringBuf::npos) {
                break;
            }
            start = end + 1;
        }
        return true;
    }

    TString TPathNormalizer::NormalizePath(TStringBuf absoluteLogicalPath) const {
        TString result;
        if (TryRewritePath(absoluteLogicalPath, result)) {
            return result;
        }
        return TString(absoluteLogicalPath);
    }

    bool TPathNormalizer::TryRewritePath(TStringBuf absoluteLogicalPath, TString& output) const {
        if (!Impl) {
            return false;
        }

        // RE2 represents an unmatched capture with nullptr. Keep a matched empty
        // input distinguishable, and make subtraction of capture pointers valid.
        const re2::StringPiece input(
            absoluteLogicalPath.data() ? absoluteLogicalPath.data() : "", absoluteLogicalPath.size());
        // RE2 replacement references are single digits: the whole match and \1..\9.
        std::array<re2::StringPiece, 10> matches;
        for (const auto& rule : Impl->Rules) {
            if (!rule.Pattern->Match(input, 0, input.size(), re2::RE2::UNANCHORED,
                                     matches.data(), rule.SubmatchCount)) {
                continue;
            }

            std::string replacement;
            Y_ENSURE(rule.Pattern->Rewrite(&replacement,
                                           re2::StringPiece(rule.Replacement.data(), rule.Replacement.size()),
                                           matches.data(), rule.SubmatchCount), "Validated path rewrite failed");

            const size_t prefixSize = matches[0].data() - input.data();
            const size_t suffixStart = prefixSize + matches[0].size();
            TString result;
            result.reserve(input.size() - matches[0].size() + replacement.size());
            result.append(input.data(), prefixSize);
            result.append(replacement.data(), replacement.size());
            result.append(input.data() + suffixStart, input.size() - suffixStart);
            // Assign only after using the input: it may refer to output's buffer.
            output = std::move(result);
            return true;
        }

        return false;
    }

    TString TPathNormalizer::GetFingerprint() const {
        return Impl ? Impl->Fingerprint : TString{};
    }

} // namespace NKikimr::NPathAliasing

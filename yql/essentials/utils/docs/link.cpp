#include "link.h"

#include "name.h"

#include <yql/essentials/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/string/builder.h>
#include <util/string/split.h>

namespace NYql::NDocs {

    TLinkTarget TLinkTarget::Parse(TStringBuf string) {
        static const RE2 Regex(R"re(([^#?()]*)(#[^?()]*)?)re");

        TString path;
        TString anchor;
        if (RE2::FullMatch(string, Regex, &path, &anchor)) {
            if (!anchor.empty()) {
                YQL_ENSURE(anchor.StartsWith('#'));
                anchor.erase(0, 1);
            }

            return {
                .RelativePath = path,
                .Anchor = !anchor.empty() ? TMaybe<TString>(anchor) : Nothing(),
            };
        }

        throw yexception()
            << "invalid link target '" << string << "': "
            << "does not match regex '" << Regex.pattern() << "'";
    }

    TMaybe<TLinkTarget> LookupUDF(const TLinks& links, TStringBuf name) {
        const auto udf = SplitUDF(TString(name));
        YQL_ENSURE(udf, "Invalid UDF: " << name);

        const auto [module, function] = *udf;

        if (const TLinkTarget* target = nullptr;
            (target = links.FindPtr(module + "::" + function)) ||
            (target = links.FindPtr(module + "::" + "*"))) {
            return *target;
        }

        return Nothing();
    }

    TMaybe<TLinkTarget> LookupBasic(const TLinks& links, TStringBuf name) {
        TMaybe<TLinkKey> key = NormalizedName(TString(name));
        if (!key) {
            return Nothing();
        }

        if (const TLinkTarget* target = links.FindPtr(*key)) {
            return *target;
        }

        return Nothing();
    }

    TMaybe<TLinkTarget> Lookup(const TLinks& links, TStringBuf name) {
        if (IsUDF(name)) {
            return LookupUDF(links, name);
        }

        return LookupBasic(links, name);
    }

    TLinkKey ParseLinkKey(TStringBuf string) {
        static RE2 UDFRegex(TStringBuilder()
                            << "(" << NormalizedNameRegex.pattern() << ")\\:\\:("
                            << "\\*|" << NormalizedNameRegex.pattern() << ")");

        if (IsNormalizedName(string)) {
            return TString(string);
        }

        if (RE2::FullMatch(string, UDFRegex)) {
            return TString(string);
        }

        ythrow yexception()
            << "invalid link key '" << string << "': "
            << "does not match any regex";
    }

    TLinks ParseLinks(const NJson::TJsonValue& json) {
        TLinks links;
        for (const auto& [keyString, value] : json.GetMapSafe()) {
            TLinkKey key = ParseLinkKey(keyString);
            TLinkTarget target = TLinkTarget::Parse(value.GetStringSafe());
            links[std::move(key)] = std::move(target);
        }
        return links;
    }

    TLinks Merge(TLinks&& lhs, TLinks&& rhs) {
        for (auto& [k, v] : rhs) {
            YQL_ENSURE(
                !lhs.contains(k),
                "Duplicate '" << k << "', old '" << lhs[k] << "', new '" << v << "'");

            lhs[k] = std::move(v);
        }
        return lhs;
    }

} // namespace NYql::NDocs

template <>
void Out<NYql::NDocs::TLinkTarget>(IOutputStream& out, const NYql::NDocs::TLinkTarget& target) {
    out << target.RelativePath;
    if (target.Anchor) {
        out << "#" << *target.Anchor;
    }
}

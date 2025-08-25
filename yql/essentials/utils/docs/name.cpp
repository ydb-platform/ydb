#include "name.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/core/sql_types/normalize_name.h>

#include <util/string/split.h>

namespace NYql::NDocs {

    const RE2 NormalizedNameRegex(R"re([a-z_]{1,2}[a-z0-9]*)re");

    bool IsNormalizedName(TStringBuf name) {
        return RE2::FullMatch(name, NormalizedNameRegex);
    }

    TMaybe<TString> NormalizedName(TString name) {
        if (TMaybe<TIssue> issue = NormalizeName(TPosition(), name)) {
            return Nothing();
        }

        if (!IsNormalizedName(name)) {
            return Nothing();
        }

        return name;
    }

    bool IsUDF(TStringBuf name) {
        return name.Contains("::");
    }

    TMaybe<std::pair<TString, TString>> SplitUDF(TString name) {
        if (!IsUDF(name)) {
            return Nothing();
        }

        TVector<TString> words;
        words.reserve(2);
        StringSplitter(name).SplitByString("::").Collect(&words);
        YQL_ENSURE(words.size() == 2, "Invalid UDF pattern: " << name);

        TMaybe<TString> module = NormalizedName(std::move(words[0]));
        TMaybe<TString> function = NormalizedName(std::move(words[1]));
        YQL_ENSURE(module && function, "Unable to normalize " << name);

        return std::make_pair(*module, *function);
    }

} // namespace NYql::NDocs

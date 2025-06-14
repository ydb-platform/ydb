#pragma once

#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    NJson::TJsonValue LoadJsonResource(TStringBuf filename);

    template <class T, class U>
    T Merge(T lhs, U rhs) {
        std::copy(std::begin(rhs), std::end(rhs), std::back_inserter(lhs));
        return lhs;
    }

    TVector<TString> ParsePragmas(NJson::TJsonValue json);

    TVector<TString> ParseTypes(NJson::TJsonValue json);

    TVector<TString> ParseFunctions(NJson::TJsonValue json);

    TVector<TString> ParseUdfs(NJson::TJsonValue json);

    THashMap<EStatementKind, TVector<TString>> ParseHints(NJson::TJsonValue json);

} // namespace NSQLComplete

#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <util/generic/string.h>

namespace NYqlLangModule {

struct TSql2YqlInput {
    TString Query;
    TString LangVersion;
    TString GatewaysCfg;
};

struct TSql2YqlOutput {
    bool IsOk = false;
    TVector<TString> Issues;
};

TSql2YqlOutput Sql2Yql(const TSql2YqlInput& input) noexcept;

} // namespace NYqlLangModule

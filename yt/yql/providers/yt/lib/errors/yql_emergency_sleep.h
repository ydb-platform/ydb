#pragma once

#include <yql/essentials/utils/yql_panic.h>
#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

namespace NPrivate {

bool IsInsideJob();
void ReportAndSleep(const TString& msg);

} // NPrivate

#define YQL_EMERGENCY_SLEEP(condition, ...)                                                 \
    if (!NYql::NPrivate::IsInsideJob()) {                                                   \
        YQL_ENSURE(condition, __VA_ARGS__);                                                 \
    } else if (Y_UNLIKELY(!(condition))) {                                                  \
        NYql::NPrivate::ReportAndSleep(TStringBuilder() << #condition << ", " __VA_ARGS__); \
    }

} // namespace NYql

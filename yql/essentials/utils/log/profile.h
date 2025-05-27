#pragma once

#include "log_level.h"
#include "context.h"

#include <util/system/datetime.h>


#define YQL_PROFILE_SCOPE(level, name) \
    ::NYql::NLog::TProfilingScope Y_GENERATE_UNIQUE_ID(ps)( \
            name, ::NYql::NLog::ELevel::level, __FILE__, __LINE__)

#define YQL_PROFILE_BLOCK_IMPL(level, name) \
    ::NYql::NLog::TProfilingScope( \
            name, ::NYql::NLog::ELevel::level, __FILE__, __LINE__)

#define YQL_PROFILE_SCOPE_VAL(level, name) \
    TAutoPtr<::NYql::NLog::TProfilingScope>(new ::NYql::NLog::TProfilingScope(\
            name, ::NYql::NLog::ELevel::level, __FILE__, __LINE__, \
            ::NYql::NLog::CurrentLogContextPath()))

#define YQL_PROFILE_BIND_VAL(future, scopeVal) \
    future.Apply([scopeVal](const decltype(future)& f) { \
        return f.GetValue(); \
    });

#define YQL_PROFILE_BLOCK(level, name) \
    if (auto Y_GENERATE_UNIQUE_ID(t) = YQL_PROFILE_SCOPE_VAL(level, name)) { \
        goto Y_CAT(YQL_LOG_CTX_LABEL, __LINE__); \
    } else Y_CAT(YQL_LOG_CTX_LABEL, __LINE__):

#define YQL_PROFILE_FUNC(level) YQL_PROFILE_SCOPE(level, __FUNCTION__)
#define YQL_PROFILE_FUNCSIG(level) YQL_PROFILE_SCOPE(level, Y_FUNC_SIGNATURE)

#define YQL_PROFILE_FUNC_VAL(level) YQL_PROFILE_SCOPE_VAL(level, __FUNCTION__)
#define YQL_PROFILE_FUNCSIG_VAL(level) YQL_PROFILE_SCOPE_VAL(level, Y_FUNC_SIGNATURE)


namespace NYql {
namespace NLog {

/**
 * @brief Adds elapsed execution time to log when goes outside of scope.
 */
class TProfilingScope {
public:
    TProfilingScope(const char* name, ELevel level, const char* file, int line,
                    std::pair<TString, TString> logCtxPath = std::make_pair(TString(), TString()))
        : Name_(name)
        , Level_(level)
        , File_(file)
        , Line_(line)
        , StartedAt_(::MicroSeconds())
        , LogCtxPath_(std::move(logCtxPath))
    {
    }

    ~TProfilingScope();

    explicit inline operator bool() const noexcept {
        return true;
    }

private:
    const char* Name_;
    ELevel Level_;
    const char* File_;
    int Line_;
    ui64 StartedAt_;
    std::pair<TString, TString> LogCtxPath_;
};

} // namspace NLog
} // namspace NYql

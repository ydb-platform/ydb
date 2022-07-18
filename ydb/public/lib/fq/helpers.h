#pragma once

#include "scope.h"

namespace NYdb {
namespace NFq {

template<typename T>
T CreateFqSettings(const TScope& scope)
{
    T settings;
    settings.Header_ = {
        { "x-ydb-fq-project", scope.ToString() }
    };
    return settings;
}

template<typename T>
T CreateFqSettings(const TString& folderId)
{
    return CreateFqSettings<T>(TScope{TScope::YandexCloudScopeSchema + "://" + folderId});
}

} // namespace NFq
} // namespace Ndb

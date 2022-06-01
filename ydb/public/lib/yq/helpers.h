#pragma once

#include "scope.h"

namespace NYdb {
namespace NYq {

template<typename T>
T CreateYqSettings(const TScope& scope)
{
    T settings;
    settings.Header_ = {
        { "x-yq-scope", scope.ToString() }, // TODO: remove YQ-1055
        { "x-fq-scope", scope.ToString() }
    };
    return settings;
}

template<typename T>
T CreateYqSettings(const TString& folderId)
{
    return CreateYqSettings<T>(TScope{TScope::YandexCloudScopeSchema + "://" + folderId});
}

} // namespace NYq
} // namespace Ndb

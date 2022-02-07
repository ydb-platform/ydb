#pragma once

#include "scope.h"

namespace NYdb {
namespace NYq {

template<typename T>
T CreateYqSettings(const TString& folderId)
{
    T settings;
    settings.Header_ = {{ "x-yq-scope", TScope::YandexCloudScopeSchema + "://" + folderId }};
    return settings;
}

template<typename T>
T CreateYqSettings(const TScope& scope)
{
    T settings;
    settings.Header_ = {{ "x-yq-scope",  scope.ToString() }};
    return settings;
}

} // namespace NYq
} // namespace Ndb

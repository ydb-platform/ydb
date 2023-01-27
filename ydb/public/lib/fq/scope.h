#pragma once

#include <util/string/builder.h>

namespace NYdb {
namespace NFq {

class TScope {
public:
    static TString YandexCloudScopeSchema;

    TScope()
    { }

    TScope(const TString& scope)
        : Scope(scope)
    { }

    TScope(TString&& scope)
        : Scope(std::move(scope))
    { }

    const TString& ToString() const {
        return Scope;
    }

    bool Empty() const {
        return Scope.empty();
    }

    TString ParseFolder() const;

private:
    TString Scope;
};

} // namespace NFq
} // namespace Ndb

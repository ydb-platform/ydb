#pragma once

#include <ydb/core/tx/general_cache/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NGeneralCache::NSource {

template <typename TPolicy>
class TFetchingContext {
private:
    using TCallback = typename NPublic::ICallback<TPolicy>;

    std::vector<std::shared_ptr<TCallback>> Callbacks;
    YDB_READONLY_DEF(ui64, Cookie);

public:
    TFetchingContext(std::vector<std::shared_ptr<TCallback>>&& callbacks, const ui64 cookie)
        : Callbacks(std::move(callbacks))
        , Cookie(cookie)
    {
    }

    bool IsAborted() const {
        return std::all_of(Callbacks.begin(), Callbacks.end(), [](const auto& c) {
            return c->IsAborted();
        });
    }
};

}   // namespace NKikimr::NGeneralCache::NSource

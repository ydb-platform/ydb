#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_control.h>

#include <memory>

namespace NYdb::inline Dev {

    struct TRequestControlAccess {
        static std::shared_ptr<void> Subscribe(const std::shared_ptr<TRequestControl>& control, std::function<void()> callback) {
            return control->Subscribe(std::move(callback));
        }
    };

} // namespace NYdb::inline Dev

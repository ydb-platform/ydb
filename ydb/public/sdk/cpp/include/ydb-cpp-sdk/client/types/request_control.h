#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace NYdb::inline Dev {

    struct TRequestControlAccess;

    //! Cancels transport operations using this handle, including direct table
    //! CreateSession, DescribeTable, Close and query stream creation/read operations.
    //! Cancellation also covers their credential/discovery wait before RPC start.
    //! GetSession pool waiters are not removed by this handle; use CreateSession
    //! when acquisition must be cancellable. Cancellation is permanent and thread safe.
    class TRequestControl {
    public:
        void Cancel() noexcept {
            std::vector<std::weak_ptr<std::function<void()>>> callbacks;
            {
                std::lock_guard guard(Mutex_);
                if (Cancelled_) {
                    return;
                }
                Cancelled_ = true;
                callbacks.swap(Callbacks_);
            }
            for (auto& weak : callbacks) {
                if (auto callback = weak.lock()) {
                    try {
                        (*callback)();
                    } catch (...) {
                        // Continue cancelling independent requests if a transport callback fails.
                        continue;
                    }
                }
            }
        }

        bool IsCancelled() const {
            std::lock_guard guard(Mutex_);
            return Cancelled_;
        }

    private:
        friend struct TRequestControlAccess;

        std::shared_ptr<void> Subscribe(std::function<void()> callback) {
            auto registration = std::make_shared<std::function<void()>>(std::move(callback));
            {
                std::lock_guard guard(Mutex_);
                if (!Cancelled_) {
                    std::erase_if(Callbacks_, [](const auto& weak) { return weak.expired(); });
                    Callbacks_.push_back(registration);
                    return registration;
                }
            }
            (*registration)();
            return {};
        }

        mutable std::mutex Mutex_;
        bool Cancelled_ = false;
        std::vector<std::weak_ptr<std::function<void()>>> Callbacks_;
    };

} // namespace NYdb::inline Dev

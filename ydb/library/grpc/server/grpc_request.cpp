#include "grpc_request.h"

namespace NYdbGrpc {

const char* GRPC_USER_AGENT_HEADER = "user-agent";

class TStreamAdaptor: public IStreamAdaptor {
public:
    TStreamAdaptor()
        : StreamIsReady_(true)
    {}

    void Enqueue(std::function<void()>&& fn, bool urgent) override {
        with_lock(Mtx_) {
            if (!UrgentQueue_.empty() || !NormalQueue_.empty()) {
                Y_ABORT_UNLESS(!StreamIsReady_);
            }
            auto& queue = urgent ? UrgentQueue_ : NormalQueue_;
            if (StreamIsReady_ && queue.empty()) {
                StreamIsReady_ = false;
            } else {
                queue.push_back(std::move(fn));
                return;
            }
        }
        fn();
    }

    size_t ProcessNext() override {
        size_t left = 0;
        std::function<void()> fn;
        with_lock(Mtx_) {
            Y_ABORT_UNLESS(!StreamIsReady_);
            auto& queue = UrgentQueue_.empty() ? NormalQueue_ : UrgentQueue_;
            if (queue.empty()) {
                // Both queues are empty
                StreamIsReady_ = true;
            } else {
                fn = std::move(queue.front());
                queue.pop_front();
                left = UrgentQueue_.size() + NormalQueue_.size();
            }
        }
        if (fn)
            fn();
        return left;
    }
private:
    bool StreamIsReady_;
    TList<std::function<void()>> NormalQueue_;
    TList<std::function<void()>> UrgentQueue_;
    TMutex Mtx_;
};

IStreamAdaptor::TPtr CreateStreamAdaptor() {
    return std::make_unique<TStreamAdaptor>();
}

} // namespace NYdbGrpc

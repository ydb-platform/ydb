#include "util/system/compiler.h"
#include "util/system/types.h"

#include <queue>
#include <functional>
#include <thread>
#include <vector>
#include <mutex>

// naive thread pool: it's enough for our example
class TThreadPool {
public:
    using TTask = std::function<void()>;

    TThreadPool(ui32 n = std::thread::hardware_concurrency()) noexcept {
        Threads.reserve(n);
        for (ui32 i = 0; i != n; ++i) {
            Threads.emplace_back([this] { Work(); });
        }
    }

    void Submit(TTask&& task) noexcept {
        {
            std::lock_guard lock{M};
            Tasks.emplace(std::move(task));
        }
        CV.notify_one();
    }

    ~TThreadPool() noexcept {
        {
            std::lock_guard lock{M};
            Tasks.emplace(nullptr);
        }
        CV.notify_all();
        for (auto& thread : Threads) {
            thread.join();
        }
    }

private:
    void Work() noexcept {
        std::unique_lock lock{M};
        while (true) {
            while (!Tasks.empty()) {
                auto task = std::move(Tasks.front());
                if (Y_UNLIKELY(!task))
                    return;
                Tasks.pop();
                lock.unlock();
                task();
                lock.lock();
            }
            CV.wait(lock);
        }
    }

    std::mutex M;
    std::condition_variable CV;
    std::queue<TTask> Tasks;
    std::vector<std::thread> Threads;
};

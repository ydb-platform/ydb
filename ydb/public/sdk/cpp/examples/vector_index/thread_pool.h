#pragma once

#include "vector_index.h"

#include "util/stream/output.h"
#include "util/system/compiler.h"
#include "util/system/types.h"

#include <queue>
#include <thread>
#include <vector>
#include <mutex>

namespace NVectorIndex {

// naive thread pool: it's enough for our example
class TThreadPool {
public:
    struct TTask {
        virtual void Call() noexcept = 0;
    };

    template <typename TFunc>
    struct TTaskImpl final: TFunc, TTask {
        template <typename TArg>
        TTaskImpl(TArg&& func)
            : TFunc{std::forward<TArg>(func)}
        {
        }

        void Call() noexcept final {
            try {
                (*this)();
            } catch (const std::exception& e) {
                Cerr << "Call failed: " << e.what() << Endl;
            }
            delete this;
        }
    };

    TThreadPool(ui32 n = std::thread::hardware_concurrency()) noexcept {
        n = std::max<ui32>(1, n);
        Threads.reserve(n);
        for (ui32 i = 0; i != n; ++i) {
            Threads.emplace_back([this] { Work(); });
        }
    }

    ui32 Size() const noexcept {
        return Threads.size();
    }

    template <typename TFunc>
    void Submit(TFunc&& func) noexcept {
        auto task = std::make_unique<TTaskImpl<std::decay_t<TFunc>>>(std::forward<TFunc>(func));
        std::unique_lock lock{M};
        while (true) {
            auto tasks = Tasks.size();
            auto threads = Threads.size();
            if (Y_LIKELY(tasks < threads * 2)) {
                break;
            }
            lock.unlock();
            Cout << "ThreadPool overloaded " << tasks << " / " << threads << Endl;
            std::this_thread::sleep_for(std::chrono::seconds{1});
            lock.lock();
        }
        Tasks.emplace(task.release());
        lock.unlock();
        CV.notify_one();
    }

    void Join() noexcept {
        {
            std::lock_guard lock{M};
            Tasks.emplace(nullptr);
        }
        CV.notify_all();
        for (auto& thread : Threads) {
            thread.join();
        }
        Threads.clear();
    }

private:
    void Work() noexcept {
        std::unique_lock lock{M};
        while (true) {
            while (!Tasks.empty()) {
                auto* task = Tasks.front();
                if (Y_UNLIKELY(!task))
                    return;
                Tasks.pop();
                lock.unlock();
                task->Call();
                lock.lock();
            }
            CV.wait(lock);
        }
    }

    std::mutex M;
    std::condition_variable CV;
    std::queue<TTask*> Tasks;
    std::vector<std::thread> Threads;
};

// naive wait group: it's enough for our example
struct TWaitGroup {
    explicit TWaitGroup(size_t counter = 0) noexcept
        : counter_{counter + 1}
    {
    }

    void Add(size_t counter = 1) noexcept {
        counter_.fetch_add(counter, std::memory_order_relaxed);
    }

    void Done(size_t counter = 1) noexcept {
        if (counter_.fetch_sub(counter, std::memory_order_acq_rel) == counter) {
            std::lock_guard lock{m_};
            cv_.notify_one();
        }
    }

    void Wait(size_t counter = 0) noexcept {
        if (counter_.fetch_sub(1, std::memory_order_acq_rel) != 1) {
            std::unique_lock lock{m_};
            while (counter_.load(std::memory_order_acquire) != 0) {
                cv_.wait(lock);
            }
        }
        Reset(counter);
    }

    void Reset(size_t counter) noexcept {
        counter_.store(counter + 1, std::memory_order_relaxed);
    }

private:
    std::atomic_size_t counter_;
    std::condition_variable cv_;
    std::mutex m_;
};

}

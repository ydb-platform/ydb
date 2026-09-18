#pragma once

#include "helpers/test_server.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

class TMemoryTokenCacher: public NYdb::ITokenCacher {
public:
    std::optional<NYdb::TTokenCache> Read() const override {
        std::lock_guard lock(Mutex);
        return Tokens;
    }

    void Write(const NYdb::TTokenCache& tokens) override {
        std::lock_guard lock(Mutex);
        Tokens = tokens;
    }

private:
    mutable std::mutex Mutex;
    std::optional<NYdb::TTokenCache> Tokens;
};

class TTestAcceptor: public NYdb::IAuthAcceptor {
public:
    void Accept(const NYdb::TDeviceAuthInfo& info) override {
        std::lock_guard lock(Mutex);
        Info = info;
        Changed.notify_all();
    }

    NYdb::TDeviceAuthInfo Wait() {
        std::unique_lock lock(Mutex);
        UNIT_ASSERT(Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Info.has_value(); }));
        return Info.value();
    }

private:
    std::mutex Mutex;
    std::condition_variable Changed;
    std::optional<NYdb::TDeviceAuthInfo> Info;
};

class TQueuedOidcFacility: public NYdb::ICoreFacility {
public:
    void AddPeriodicTask(NYdb::TPeriodicCb&&, NYdb::TDeadline::Duration) override {
    }

    void PostToResponseQueue(NYdb::TPostTaskCb&& callback) override {
        std::lock_guard lock(Mutex);
        Tasks.push_back(std::move(callback));
        Changed.notify_all();
    }

    bool WaitForTask() {
        std::unique_lock lock(Mutex);
        return Changed.wait_for(lock, std::chrono::seconds(10), [&] { return !Tasks.empty(); });
    }

    void RunTasks() {
        std::vector<NYdb::TPostTaskCb> tasks;
        {
            std::lock_guard lock(Mutex);
            tasks.swap(Tasks);
        }
        for (auto& task : tasks) {
            task();
        }
    }

    void DiscardTasks() {
        std::lock_guard lock(Mutex);
        Tasks.clear(); // Deliberately discard callbacks under an executor lock.
    }

private:
    std::mutex Mutex;
    std::condition_variable Changed;
    std::vector<NYdb::TPostTaskCb> Tasks;
};

class TGatedOidcCacher: public TMemoryTokenCacher {
public:
    void Write(const NYdb::TTokenCache& tokens) override {
        Entered.TrySetValue();
        Release.GetFuture().Wait();
        TMemoryTokenCacher::Write(tokens);
    }

    NThreading::TPromise<void> Entered = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Release = NThreading::NewPromise<void>();
};

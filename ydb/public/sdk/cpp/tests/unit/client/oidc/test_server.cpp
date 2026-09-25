#include "test_server.h"

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <utility>

std::optional<NYdb::NOidc::TTokenCache> TMemoryTokenCacher::Read() const {
    with_lock (Mutex) {
        return Tokens;
    }
}

void TMemoryTokenCacher::Write(const NYdb::NOidc::TTokenCache& tokens) {
    with_lock (Mutex) {
        Tokens = tokens;
    }
}

void TTestAcceptor::Accept(const NYdb::NOidc::TDeviceAuthInfo& info) {
    with_lock (Mutex) {
        Info = info;
    }
    Changed.notify_all();
}

NYdb::NOidc::TDeviceAuthInfo TTestAcceptor::Wait() {
    with_lock (Mutex) {
        UNIT_ASSERT(Changed.wait_for(Mutex, std::chrono::seconds(10), [&] { return Info.has_value(); }));
        return Info.value();
    }
}

void TQueuedOidcFacility::AddPeriodicTask(NYdb::TPeriodicCb&&, NYdb::TDeadline::Duration) {
}

void TQueuedOidcFacility::PostToResponseQueue(NYdb::TPostTaskCb&& callback) {
    with_lock (Mutex) {
        Tasks.push_back(std::move(callback));
    }
    Changed.notify_all();
}

bool TQueuedOidcFacility::WaitForTask() {
    with_lock (Mutex) {
        return Changed.wait_for(Mutex, std::chrono::seconds(10), [&] { return !Tasks.empty(); });
    }
}

void TQueuedOidcFacility::RunTasks() {
    std::vector<NYdb::TPostTaskCb> tasks;
    with_lock (Mutex) {
        tasks.swap(Tasks);
    }
    for (auto& task : tasks) {
        task();
    }
}

void TQueuedOidcFacility::DiscardTasks() {
    with_lock (Mutex) {
        Tasks.clear();
    }
}

void TGatedOidcCacher::Write(const NYdb::NOidc::TTokenCache& tokens) {
    Entered.TrySetValue();
    Release.GetFuture().Wait();
    TMemoryTokenCacher::Write(tokens);
}

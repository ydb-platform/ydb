#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/tests/unit/client/oidc/helpers/test_server.h>

#include <util/system/mutex.h>

#include <condition_variable>

class TMemoryTokenCacher: public NYdb::NOidc::ITokenCacher {
public:
    std::optional<NYdb::NOidc::TTokenCache> Read() const override;

    void Write(const NYdb::NOidc::TTokenCache& tokens) override;

private:
    mutable TMutex Mutex;
    std::optional<NYdb::NOidc::TTokenCache> Tokens;
};

class TTestAcceptor: public NYdb::NOidc::IAuthAcceptor {
public:
    void Accept(const NYdb::NOidc::TDeviceAuthInfo& info) override;

    NYdb::NOidc::TDeviceAuthInfo Wait();

private:
    TMutex Mutex;
    std::condition_variable_any Changed;
    std::optional<NYdb::NOidc::TDeviceAuthInfo> Info;
};

class TQueuedOidcFacility: public NYdb::ICoreFacility {
public:
    void AddPeriodicTask(NYdb::TPeriodicCb&&, NYdb::TDeadline::Duration) override;

    void PostToResponseQueue(NYdb::TPostTaskCb&& callback) override;

    bool WaitForTask();

    void RunTasks();

    void DiscardTasks();

private:
    TMutex Mutex;
    std::condition_variable_any Changed;
    std::vector<NYdb::TPostTaskCb> Tasks;
};

class TGatedOidcCacher: public TMemoryTokenCacher {
public:
    void Write(const NYdb::NOidc::TTokenCache& tokens) override;

    NThreading::TPromise<void> Entered = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Release = NThreading::NewPromise<void>();
};

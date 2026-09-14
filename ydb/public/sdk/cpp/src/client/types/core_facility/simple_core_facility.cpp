#include "simple_core_facility.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/runtime/runtime.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>

namespace NYdb::inline Dev {
namespace {

struct TPeriodicTask {
    TPeriodicCb Callback;
    TDeadline::Duration Period;

    void operator()() {
        bool repeat = true;
        try {
            repeat = Callback({}, EStatus::SUCCESS);
        } catch (...) {
            Cerr << "TSimpleCoreFacility periodic task failed: " << CurrentExceptionMessage() << Endl;
        }
        if (repeat) {
            GetRuntime().Schedule(Period, std::move(*this));
        }
    }
};

} // namespace

void TSimpleCoreFacility::AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) {
    GetRuntime().Post(TPeriodicTask{std::move(cb), period});
}

void TSimpleCoreFacility::PostToResponseQueue(TPostTaskCb&& f) {
    if (f) {
        GetRuntime().Post(std::move(f));
    }
}

std::shared_ptr<ICoreFacility> CreateSimpleCoreFacility() {
    static const auto* facility = new std::shared_ptr<ICoreFacility>(std::make_shared<TSimpleCoreFacility>());
    return *facility;
}

} // namespace NYdb::inline Dev

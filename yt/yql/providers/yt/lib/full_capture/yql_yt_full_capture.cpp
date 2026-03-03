#include "yql_yt_full_capture.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/system/mutex.h>

namespace NYql {

namespace {

class TYtFullCapture : public IYtFullCapture {
public:
    void ReportError(const std::exception& e) override {
        YQL_CLOG(WARN, ProviderYt) << "YT full capture error: " << e.what();

        auto guard = Guard(Lock_);
        if (State_ != EState::Ready) {
            State_ = EState::Error;
        } else {
            YQL_CLOG(ERROR, ProviderYt) << "Reporting YT full capture error after succesful seal - full capture may be corrupted";
        }
    }

    void AddOperationFuture(const NThreading::TFuture<NCommon::TOperationResult>& future) override {
        auto guard = Guard(Lock_);
        if (State_ != EState::Ready) {
            OperationFutures_.push_back(future);
        } else {
            YQL_CLOG(ERROR, ProviderYt) << "Adding YT full capture operation future after succesful seal - full capture will be corrupted";
        }
    }

    bool Seal() override {
        auto guard = Guard(Lock_);

        if (State_ == EState::Error) {
            YQL_CLOG(WARN, ProviderYt) << "YT full capture has not been taken";
            return false;
        }
        YQL_ENSURE(State_ == EState::None, "bad state");

        auto captureFuture = NThreading::WaitAll(OperationFutures_);
        if (!captureFuture.IsReady()) {
            YQL_CLOG(WARN, ProviderYt) << "YT full capture has not been taken - deadline exceeded";
            return false;
        }
        YQL_ENSURE(captureFuture.HasValue());

        for (auto& future : OperationFutures_) {
            auto& result = future.GetValue();
            if (!result.Success()) {
                YQL_CLOG(WARN, ProviderYt) << "YT full capture has not been taken";
                return false;
            }
        }

        State_ = EState::Ready;
        return true;
    }

    bool IsReady() const override {
        auto guard = Guard(Lock_);
        return State_ == EState::Ready;
    }

private:
    enum class EState {
        None,
        Ready,
        Error,
    };

    EState State_ = EState::None;

    TMutex Lock_;
    TVector<NThreading::TFuture<NCommon::TOperationResult>> OperationFutures_;
};

} // namespace

IYtFullCapture::TPtr CreateYtFullCapture() {
    return MakeIntrusive<TYtFullCapture>();
}

} // namespace NYql

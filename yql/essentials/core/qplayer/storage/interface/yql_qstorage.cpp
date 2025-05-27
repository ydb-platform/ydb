#include "yql_qstorage.h"

#include <util/system/mutex.h>

namespace NYql {
class TQWriterDecorator : public IQWriter {
    public:
    TQWriterDecorator(IQWriterPtr&& underlying) : Underlying_(std::move(underlying)) {}
    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) override final {
        decltype(Underlying_) underlying;
        with_lock(Mutex_) {
            underlying = Underlying_;
        }
        if (Closed_) {
            return NThreading::MakeFuture();
        }
        try {
            return underlying->Put(key, value);
        } catch (...) {
            auto message = CurrentExceptionMessage();
            with_lock(Mutex_) {
                Exception_ = std::move(message);
            }
            Close();
            return NThreading::MakeFuture();
        }
    }

    NThreading::TFuture<void> Commit() override final {
        with_lock(Mutex_) {
            if (Exception_) {
                throw yexception() << "QWriter exception while Put(): " << *Exception_ << ")";
            }
        }
        bool expected = false;
        if (!Closed_.compare_exchange_strong(expected, true)) {
            throw yexception() << "QWriter closed";
        }
        decltype(Underlying_) underlying;
        with_lock(Mutex_) {
            underlying = Underlying_;
        }
        auto result = underlying->Commit();
        with_lock(Mutex_) {
            Underlying_ = {};
        }
        return result;
    }

    // Close all used files, doesn't commit anything
    void Close() override final {
        bool expected = false;
        if (Closed_.compare_exchange_strong(expected, true)) {
            with_lock(Mutex_) {
                Underlying_ = {};
            }
        }
    }
private:
    TMaybe<TString> Exception_;
    IQWriterPtr Underlying_;
    std::atomic<bool> Closed_ = false;
    TMutex Mutex_;
};

IQWriterPtr MakeCloseAwareWriterDecorator(IQWriterPtr&& rhs) {
    return std::make_shared<TQWriterDecorator>(std::move(rhs));
}

}

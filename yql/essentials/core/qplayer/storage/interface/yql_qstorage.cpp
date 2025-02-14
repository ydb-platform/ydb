#include "yql_qstorage.h"

namespace NYql {
class TQWriterDecorator : public IQWriter {
    public:
    TQWriterDecorator(IQWriterPtr&& underlying) : Underlying_(std::move(underlying)) {}
    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) override final {
        if (Closed_) {
            return NThreading::MakeFuture();
        }
        return Underlying_->Put(key, value);
    }

    NThreading::TFuture<void> Commit() override final {
        if (Closed_) {
            throw yexception() << "QWriter closed";
        }
        return Underlying_->Commit();
    }

    // Close all used files, doesn't commit anything
    void Close() override final {
        bool expected = false;
        if (Closed_.compare_exchange_strong(expected, true)) {
            Underlying_ = {};
        }
    }
private:
    IQWriterPtr Underlying_;
    std::atomic<bool> Closed_ = false;
};

IQWriterPtr MakeCloseAwareWriterDecorator(IQWriterPtr&& rhs) {
    return std::make_shared<TQWriterDecorator>(std::move(rhs));
}

}

// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <functional>
#include <atomic>

namespace NKikimr::NArrow {

class IInputStream {
public:
    using TPtr = std::shared_ptr<IInputStream>;

    IInputStream() = default;
    IInputStream(const IInputStream&) = delete;
    IInputStream& operator = (const IInputStream&) = delete;
    virtual ~IInputStream() = default;

    virtual std::shared_ptr<arrow::Schema> Schema() const = 0;
    bool IsCancelled() const { return Cancelled; }

    std::shared_ptr<arrow::RecordBatch> Read() {
        if (auto res = ReadImpl()) {
            return res;
        }
        Cancel();
        return {};
    }

    void Start() {
        FinishImpl();
        VisitChildren(Children, [](IInputStream& child) { child.Finish(); });
    }

    void Finish() {
        VisitChildren(Children, [](IInputStream& child) { child.Start(); });
        StartImpl();
    }

    void Cancel() {
        if (!CancelImpl()) {
            return;
        }
        VisitChildren(Children, [](IInputStream& child) { child.Cancel(); });
    }

protected:
    std::vector<TPtr> Children;
    std::atomic<bool> Cancelled{false};

private:
    virtual std::shared_ptr<arrow::RecordBatch> ReadImpl() = 0;
    virtual void StartImpl() {}
    virtual void FinishImpl() {}

    virtual bool CancelImpl() {
        bool expected = false;
        if (!Cancelled.compare_exchange_strong(expected, true, std::memory_order_seq_cst, std::memory_order_relaxed)) {
            return false;
        }
        return true;
    }

    // Single thread version of visitor
    static void VisitChildren(std::vector<IInputStream::TPtr>& children, std::function<void(IInputStream&)> f) {
        for (auto & child : children) {
            f(*child);
        }
    }
};

}

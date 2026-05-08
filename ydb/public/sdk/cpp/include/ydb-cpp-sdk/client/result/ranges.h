#pragma once
#include "result.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NYdb::inline Dev {
class TResultIterEnd {};
class TResultIterator {
public:
    TResultIterator(const TResultIterator&) = delete;
    TResultIterator(TResultIterator&&) = default;
    ~TResultIterator() = default;
    TResultIterator& operator=(const TResultIterator&) = delete;
    TResultIterator& operator=(TResultIterator&&) = default;
    bool operator==(const TResultIterator& other) const;
    bool operator!=(const TResultIterator& other) const;
    bool operator==(const TResultIterEnd& other) const;
    bool operator!=(const TResultIterEnd& other) const;
    const TResultSetParser& operator*() const;
    const TResultSetParser* operator->() const;
    TResultIterator& operator++();
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

class TResultSetRange {
    public:
        TResultSetRange(const TResultSet&);
        TResultSetRange(NQuery::TExecuteQueryIterator&&);
        TResultIterEnd end();
        TResultIterator begin();
    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl_;
    };
} // namespace NYdb::inline Dev
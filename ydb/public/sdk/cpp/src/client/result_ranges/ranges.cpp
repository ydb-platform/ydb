#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/ranges.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <memory>
#include <optional>
#include <utility>

namespace NYdb::inline Dev {

class TResultSetRange::TImpl {
public:
    explicit TImpl(TResultSet&& resultSet)
        : Producer_(std::make_unique<TOneShotProducer>(std::move(resultSet)))
    {}

    explicit TImpl(NQuery::TExecuteQueryIterator&& iterator)
        : Producer_(std::make_unique<TStreamProducer>(std::move(iterator)))
    {}

    bool Start() {
        if (Started_) {
            return Parser_.has_value();
        }
        Started_ = true;
        return AdvanceRow();
    }

    bool Advance() {
        Started_ = true;
        return AdvanceRow();
    }

    bool HasCurrent() const {
        return Parser_.has_value();
    }

    TResultSetParser& CurrentParser() {
        return *Parser_;
    }

private:
    bool AdvanceRow() {
        while (!Parser_ || !Parser_->TryNextRow()) {
            auto next = Producer_->TryGetNextResultSet();
            if (!next) {
                Parser_.reset();
                return false;
            }
            Parser_.emplace(*next);
        }
        return true;
    }

    class IResultSetProducer {
    public:
        virtual ~IResultSetProducer() = default;
        virtual std::optional<TResultSet> TryGetNextResultSet() = 0;
    };

    class TOneShotProducer : public IResultSetProducer {
    public:
        explicit TOneShotProducer(TResultSet&& resultSet)
            : ResultSet_(std::move(resultSet))
        {}

        std::optional<TResultSet> TryGetNextResultSet() override {
            if (!ResultSet_) {
                return std::nullopt;
            }
            std::optional<TResultSet> result;
            result.swap(ResultSet_);
            return result;
        }

    private:
        std::optional<TResultSet> ResultSet_;
    };

    class TStreamProducer : public IResultSetProducer {
    public:
        explicit TStreamProducer(NQuery::TExecuteQueryIterator&& iterator)
            : Iterator_(std::move(iterator))
        {}

        std::optional<TResultSet> TryGetNextResultSet() override {
            for (;;) {
                auto part = Iterator_.ReadNext().ExtractValueSync();
                if (!part.IsSuccess()) {
                    if (part.EOS()) {
                        return std::nullopt;
                    }
                    TStatus status(std::move(part));
                    throw NStatusHelpers::TYdbErrorException(status) << status;
                }
                if (part.HasResultSet()) {
                    return part.ExtractResultSet();
                }
            }
            return std::nullopt;
        }

    private:
        NQuery::TExecuteQueryIterator Iterator_;
    };

    std::unique_ptr<IResultSetProducer> Producer_;
    std::optional<TResultSetParser> Parser_;
    bool Started_ = false;
};

TResultRowParser::TResultRowParser(TResultSetParser& parser)
    : Parser_(parser)
{}

size_t TResultRowParser::ColumnsCount() const {
    return Parser_.ColumnsCount();
}

size_t TResultRowParser::RowsCount() const {
    return Parser_.RowsCount();
}

ssize_t TResultRowParser::ColumnIndex(const std::string& columnName) {
    return Parser_.ColumnIndex(columnName);
}

TValueParser& TResultRowParser::ColumnParser(size_t columnIndex) {
    return Parser_.ColumnParser(columnIndex);
}

TValueParser& TResultRowParser::ColumnParser(const std::string& columnName) {
    return Parser_.ColumnParser(columnName);
}

TValue TResultRowParser::GetValue(size_t columnIndex) const {
    return Parser_.GetValue(columnIndex);
}

TValue TResultRowParser::GetValue(const std::string& columnName) const {
    return Parser_.GetValue(columnName);
}

class TResultIterator::TImpl {
public:
    explicit TImpl(TResultSetRange::TImpl& range)
        : Range_(range)
    {}

    bool IsAtEnd() const {
        return !Range_.HasCurrent();
    }

    void Advance() {
        Range_.Advance();
    }

    TResultRowParser& Current() {
        RowView_.reset(new TResultRowParser(Range_.CurrentParser()));
        return *RowView_;
    }

private:
    TResultSetRange::TImpl& Range_;
    std::unique_ptr<TResultRowParser> RowView_;
};

TResultSetRange::TResultSetRange(TResultSet&& resultSet)
    : Impl_(std::make_unique<TImpl>(std::move(resultSet)))
{}

TResultSetRange::TResultSetRange(NQuery::TExecuteQueryIterator&& iterator)
    : Impl_(std::make_unique<TImpl>(std::move(iterator)))
{}

TResultSetRange::TResultSetRange(TResultSetRange&&) noexcept = default;
TResultSetRange& TResultSetRange::operator=(TResultSetRange&&) noexcept = default;
TResultSetRange::~TResultSetRange() = default;

TResultIterator TResultSetRange::begin() {
    Impl_->Start();
    return TResultIterator(std::make_unique<TResultIterator::TImpl>(*Impl_));
}

TResultIterEnd TResultSetRange::end() {
    return TResultIterEnd{};
}

TResultIterator::TResultIterator(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{}

TResultIterator::TResultIterator(TResultIterator&&) noexcept = default;
TResultIterator& TResultIterator::operator=(TResultIterator&&) noexcept = default;
TResultIterator::~TResultIterator() = default;

bool TResultIterator::operator==(const TResultIterator& other) const {
    return Impl_->IsAtEnd() && other.Impl_->IsAtEnd();
}

bool TResultIterator::operator!=(const TResultIterator& other) const {
    return !(*this == other);
}

bool TResultIterator::operator==(const TResultIterEnd&) const {
    return Impl_->IsAtEnd();
}

bool TResultIterator::operator!=(const TResultIterEnd& end) const {
    return !(*this == end);
}

TResultRowParser& TResultIterator::operator*() const {
    return Impl_->Current();
}

TResultRowParser* TResultIterator::operator->() const {
    return &Impl_->Current();
}

TResultIterator& TResultIterator::operator++() {
    Impl_->Advance();
    return *this;
}

} // namespace NYdb::inline Dev

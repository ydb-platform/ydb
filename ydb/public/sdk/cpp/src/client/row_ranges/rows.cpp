#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/rows.h>

#include "rows_stream_drain.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/generic/yexception.h>

#include <memory>
#include <optional>
#include <utility>

namespace NYdb::inline Dev {

namespace {

template <typename TStatusCarrier>
void ThrowIfUnsuccessful(TStatusCarrier&& carrier) {
    if (!carrier.IsSuccess()) {
        TStatus status(std::move(carrier));
        throw NStatusHelpers::TYdbRangeErrorException(status) << status;
    }
}

} // namespace

class TRowRange::TImpl {
public:
    struct TEmptyTag {};

    explicit TImpl(TEmptyTag)
        : Producer_(std::make_unique<TEmptyProducer>())
    {}

    explicit TImpl(TResultSet&& resultSet)
        : Producer_(std::make_unique<TOneShotProducer>(std::move(resultSet)))
    {}

    explicit TImpl(NQuery::TExecuteQueryIterator&& iterator)
        : Producer_(std::make_unique<TStreamProducer<NQuery::TExecuteQueryIterator>>(std::move(iterator)))
    {}

    explicit TImpl(NTable::TScanQueryPartIterator&& iterator)
        : Producer_(std::make_unique<TStreamProducer<NTable::TScanQueryPartIterator>>(std::move(iterator)))
    {}

    explicit TImpl(NTable::TTablePartIterator&& iterator)
        : Producer_(std::make_unique<TStreamProducer<NTable::TTablePartIterator>>(std::move(iterator)))
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

    class TEmptyProducer : public IResultSetProducer {
    public:
        std::optional<TResultSet> TryGetNextResultSet() override {
            return std::nullopt;
        }
    };

    template <typename Iterator>
    class TStreamProducer : public IResultSetProducer {
    public:
        explicit TStreamProducer(Iterator&& iterator)
            : Iterator_(std::move(iterator))
        {}

        std::optional<TResultSet> TryGetNextResultSet() override {
            return NRowRangesDetail::DrainStreamIterator(Iterator_);
        }

    private:
        Iterator Iterator_;
    };

    std::unique_ptr<IResultSetProducer> Producer_;
    std::optional<TResultSetParser> Parser_;
    bool Started_ = false;
};

TRowParser::TRowParser(TResultSetParser& parser)
    : Parser_(parser)
{}

class TRowParserHolder {
public:
    explicit TRowParserHolder(TResultSetParser& parser)
        : Parser_(parser)
    {}

    TRowParser& Get() {
        return Parser_;
    }

private:
    TRowParser Parser_;
};

size_t TRowParser::ColumnsCount() const {
    return Parser_.ColumnsCount();
}

size_t TRowParser::RowsCount() const {
    return Parser_.RowsCount();
}

ssize_t TRowParser::ColumnIndex(const std::string& columnName) {
    return Parser_.ColumnIndex(columnName);
}

TValueParser& TRowParser::ColumnParser(size_t columnIndex) {
    return Parser_.ColumnParser(columnIndex);
}

TValueParser& TRowParser::ColumnParser(const std::string& columnName) {
    return Parser_.ColumnParser(columnName);
}

TValue TRowParser::GetValue(size_t columnIndex) const {
    return Parser_.GetValue(columnIndex);
}

TValue TRowParser::GetValue(const std::string& columnName) const {
    return Parser_.GetValue(columnName);
}

class TRowIterator::TImpl {
public:
    explicit TImpl(std::shared_ptr<TRowRange::TImpl> range)
        : Range_(std::move(range))
    {
        ResetRowView();
    }

    bool IsAtEnd() const noexcept {
        return !Range_ || !Range_->HasCurrent();
    }

    void Advance() {
        RowView_.reset();
        if (Range_) {
            Range_->Advance();
        }
        ResetRowView();
    }

    TRowParser& Current() {
        Y_ABORT_UNLESS(RowView_.has_value(), "TRowIterator dereference at end");
        return RowView_->Get();
    }

private:
    void ResetRowView() {
        if (Range_ && Range_->HasCurrent()) {
            RowView_.emplace(Range_->CurrentParser());
        }
    }

    std::shared_ptr<TRowRange::TImpl> Range_;
    std::optional<TRowParserHolder> RowView_;
};

TRowRange::TRowRange(TResultSet&& resultSet)
    : Impl_(std::make_shared<TImpl>(std::move(resultSet)))
{}

TRowRange::TRowRange(NTable::TDataQueryResult&& result) {
    ThrowIfUnsuccessful(result);
    auto sets = std::move(result).ExtractResultSets();
    if (sets.size() > 1) {
        ythrow yexception() << "multiple queries in one range is not allowed";
    }
    if (sets.empty()) {
        Impl_ = std::make_shared<TImpl>(TRowRange::TImpl::TEmptyTag{});
    } else {
        Impl_ = std::make_shared<TImpl>(std::move(sets[0]));
    }
}

TRowRange::TRowRange(NQuery::TExecuteQueryIterator&& iterator) {
    ThrowIfUnsuccessful(iterator);
    Impl_ = std::make_shared<TImpl>(std::move(iterator));
}

TRowRange::TRowRange(NTable::TScanQueryPartIterator&& iterator) {
    ThrowIfUnsuccessful(iterator);
    Impl_ = std::make_shared<TImpl>(std::move(iterator));
}

TRowRange::TRowRange(NTable::TTablePartIterator&& iterator) {
    ThrowIfUnsuccessful(iterator);
    Impl_ = std::make_shared<TImpl>(std::move(iterator));
}

TRowRange::TRowRange(TRowRange&&) noexcept = default;
TRowRange& TRowRange::operator=(TRowRange&&) noexcept = default;
TRowRange::~TRowRange() = default;

TRowIterator TRowRange::begin() {
    return BeginIterator(Impl_);
}

TRowIterEnd TRowRange::end() noexcept {
    return TRowIterEnd{};
}

TRowIterator TRowRange::BeginIterator(const std::shared_ptr<TImpl>& state) {
    if (state) {
        state->Start();
    }
    return TRowIterator(std::make_unique<TRowIterator::TImpl>(state));
}

TRowIterator::TRowIterator(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{}

TRowIterator::TRowIterator(TRowIterator&&) noexcept = default;
TRowIterator& TRowIterator::operator=(TRowIterator&&) noexcept = default;
TRowIterator::~TRowIterator() = default;

bool TRowIterator::operator==(const TRowIterEnd&) const noexcept {
    return Impl_->IsAtEnd();
}

bool TRowIterator::operator!=(const TRowIterEnd& end) const noexcept {
    return !(*this == end);
}

TRowParser& TRowIterator::operator*() const {
    return Impl_->Current();
}

TRowParser* TRowIterator::operator->() const {
    return &Impl_->Current();
}

TRowIterator& TRowIterator::operator++() {
    Impl_->Advance();
    return *this;
}

} // namespace NYdb::inline Dev

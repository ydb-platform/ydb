#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/ranges.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>

namespace NYdb::inline Dev {
class TResultIterator::TImpl {
public:


private:

};
class TResultSetRange::TImpl {
public:
    TImpl(const TResultSet& resultSet)
            : resultSetProducer_(MakeResultSetProducer(resultSet))
            , currentSet_(resultSetProducer_->TryGetNextResultSet())
    {}
private:
    class IResultSetProducer {
        public:
            virtual ~IResultSetProducer() = default;
            virtual std::optional<TResultSet> TryGetNextResultSet() = 0;
    };
    using IResultSetProducerPtr = std::unique_ptr<IResultSetProducer>;
    IResultSetProducerPtr MakeResultSetProducer(const TResultSet& resultSet) {
        return std::make_unique<OneResultSetProducer>(resultSet);
    }
    IResultSetProducerPtr MakeResultSetProducer(NQuery::TExecuteQueryIterator&& iterator) {
        return std::make_unique<StreamResultSetProducer>(std::move(iterator));
    }
    class OneResultSetProducer : public IResultSetProducer {
        public:
            OneResultSetProducer(const TResultSet& resultSet)
                : resultSet_(resultSet)
            {}
            std::optional<TResultSet> TryGetNextResultSet() override {
                if (!resultSet_) {
                    return std::nullopt;
                }
                const auto& resultSet = resultSet_->get();
                resultSet_ = std::nullopt;
                return resultSet;
            }
        private:
            std::optional<std::reference_wrapper<const TResultSet>> resultSet_;
    };
    class StreamResultSetProducer : public IResultSetProducer {
        public: 
            StreamResultSetProducer(NQuery::TExecuteQueryIterator&& iterator)
                : iterator_(std::move(iterator))
            {}
            std::optional<TResultSet> TryGetNextResultSet() override {
                auto result = iterator_.ReadNext().ExtractValueSync();
               
                if (nextResultSet.) {
                    return nextResultSet;
                }
                return std::nullopt;
            }
        private:
            NQuery::TExecuteQueryIterator iterator_;
    };
};

TResultSetRange::TResultSetRange(const TResultSet& resultSet)
    : Impl_(new TImpl(resultSet))
{
}

TResultSetRange::TResultSetRange(NQuery::TExecuteQueryIterator&& iterator)
    : Impl_(new TImpl(iterator))
{
}

} // namespace NYdb::inline Dev
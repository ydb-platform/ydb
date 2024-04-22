#include "yql_qstorage_memory.h"

#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NYql {

namespace {

struct TOperationMap {
    TMutex Mutex;
    using TMap = THashMap<TQItemKey, TString>;
    TMap ReadMap, WriteMap;
};

using TOperationMapPtr = std::shared_ptr<TOperationMap>;

struct TState {
    TMutex Mutex;
    using TMap = THashMap<TString, TOperationMapPtr>;
    TMap Operations;
};

using TStatePtr = std::shared_ptr<TState>;

class TReader : public IQReader {
public:
    TReader(const TOperationMapPtr& operation)
        : Operation_(operation)
    {}

    NThreading::TFuture<TMaybe<TQItem>> Get(const TQItemKey& key) const final {
        with_lock(Operation_->Mutex) {
            auto it = Operation_->ReadMap.find(key);
            if (it == Operation_->ReadMap.end()) {
                return NThreading::MakeFuture(TMaybe<TQItem>());
            }

            return NThreading::MakeFuture(TMaybe<TQItem>(TQItem({key, it->second})));
        }
    }

private:
    const TOperationMapPtr Operation_;
};

class TWriter : public IQWriter {
public:
    TWriter(const TOperationMapPtr& operation)
        : Operation_(operation)
    {}

    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) final {
        with_lock(Operation_->Mutex) {
            Operation_->WriteMap[key] = value;
            return NThreading::MakeFuture();
        }
    }

    NThreading::TFuture<void> Commit() final {
        with_lock(Operation_->Mutex) {
            Operation_->ReadMap = Operation_->WriteMap;
            return NThreading::MakeFuture();
        }
    }

private:
    const TOperationMapPtr Operation_;
};

class TIterator : public IQIterator {
public:
    TIterator(const TQIteratorSettings& settings, TOperationMap::TMap map)
        : Settings_(settings)
        , Map_(std::move(map))
        , It_(Map_.begin())
    {}

    NThreading::TFuture<TMaybe<TQItem>> Next() final {
        if (It_ == Map_.end()) {
            return NThreading::MakeFuture(TMaybe<TQItem>());
        }

        auto res =TMaybe<TQItem>({It_->first, Settings_.DoNotLoadValue ? TString() : It_->second});
        ++It_;
        return NThreading::MakeFuture(res);
    }

private:
    const TQIteratorSettings Settings_;
    const TOperationMap::TMap Map_;
    TOperationMap::TMap::const_iterator It_;
};

class TStorage : public IQStorage {
public:
    TStorage()
        : State_(std::make_shared<TState>())
    {
    }
    IQReaderPtr MakeReader(const TString& operationId) const final {
        return std::make_shared<TReader>(GetOperation(operationId));
    }

    IQWriterPtr MakeWriter(const TString& operationId) const final {
        return std::make_shared<TWriter>(GetOperation(operationId));
    }

    IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& settings) const final {
        // clones the whole map for given operation id
        return std::make_shared<TIterator>(settings, GetOperation(operationId)->ReadMap);
    }

private:
    TOperationMapPtr GetOperation(const TString& operationId) const {
        with_lock(State_->Mutex) {
            auto &op = State_->Operations[operationId];
            if (!op) {
                op = std::make_shared<TOperationMap>();
            }

            return op;
        }
    }

private:
    TStatePtr State_;
};

}

IQStoragePtr MakeMemoryQStorage() {
    return std::make_shared<TStorage>();
}

}

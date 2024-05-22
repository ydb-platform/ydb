#include "yql_qstorage_memory.h"

#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NYql {

namespace {

struct TOperationMap {
    TMutex Mutex;
    using TMap = THashMap<TQItemKey, TString>;
    using TMapPtr = std::shared_ptr<TMap>;
    TMapPtr ReadMap, WriteMap;
    bool Committed = false;
    bool Overflow = false;
    ui64 TotalBytes = 0;
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
    TReader(const TOperationMap::TMapPtr& map)
        : Map_(map)
    {}

    NThreading::TFuture<TMaybe<TQItem>> Get(const TQItemKey& key) const final {
        auto it = Map_->find(key);
        if (it == Map_->end()) {
            return NThreading::MakeFuture(TMaybe<TQItem>());
        }

        return NThreading::MakeFuture(TMaybe<TQItem>(TQItem({key, it->second})));
    }

private:
    const TOperationMap::TMapPtr Map_;
};

class TWriter : public IQWriter {
public:
    TWriter(const TOperationMapPtr& operation, const TQWriterSettings& settings)
        : Operation_(operation)
        , Settings_(settings)
    {}

    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) final {
        with_lock(Operation_->Mutex) {
            Y_ENSURE(!Operation_->Committed);
            if (!Operation_->Overflow) {
                if (Operation_->WriteMap->emplace(key, value).second) {
                    Operation_->TotalBytes += key.Component.size() + key.Label.size() + value.size();
                }

                if (Settings_.ItemsLimit && Operation_->WriteMap->size() > *Settings_.ItemsLimit) {
                    Operation_->Overflow = true;
                }

                if (Settings_.BytesLimit && Operation_->TotalBytes > *Settings_.BytesLimit) {
                    Operation_->Overflow = true;
                }
            }

            return NThreading::MakeFuture();
        }
    }

    NThreading::TFuture<void> Commit() final {
        with_lock(Operation_->Mutex) {
            if (Operation_->Overflow) {
                throw yexception() << "Overflow of qwriter";
            }

            Y_ENSURE(!Operation_->Committed);
            Operation_->ReadMap = Operation_->WriteMap;
            Operation_->Committed = true;
            return NThreading::MakeFuture();
        }
    }

private:
    const TOperationMapPtr Operation_;
    const TQWriterSettings Settings_;
};

class TIterator : public IQIterator {
public:
    TIterator(const TQIteratorSettings& settings, const TOperationMap::TMapPtr& map)
        : Settings_(settings)
        , Map_(map)
        , It_(Map_->begin())
    {}

    NThreading::TFuture<TMaybe<TQItem>> Next() final {
        if (It_ == Map_->end()) {
            return NThreading::MakeFuture(TMaybe<TQItem>());
        }

        auto res =TMaybe<TQItem>({It_->first, It_->second});
        ++It_;
        return NThreading::MakeFuture(res);
    }

private:
    const TQIteratorSettings Settings_;
    const TOperationMap::TMapPtr Map_;
    TOperationMap::TMap::const_iterator It_;
};

class TStorage : public IQStorage {
public:
    TStorage()
        : State_(std::make_shared<TState>())
    {
    }

    IQReaderPtr MakeReader(const TString& operationId, const TQReaderSettings& readerSettings) const final {
        Y_UNUSED(readerSettings);
        return std::make_shared<TReader>(GetOperation(operationId, false)->ReadMap);
    }

    IQWriterPtr MakeWriter(const TString& operationId, const TQWriterSettings& writerSettings) const final {
        return std::make_shared<TWriter>(GetOperation(operationId, true), writerSettings);
    }

    IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& iteratorSettings) const final {
        return std::make_shared<TIterator>(iteratorSettings, GetOperation(operationId, false)->ReadMap);
    }

private:
    TOperationMapPtr GetOperation(const TString& operationId, bool forWrite) const {
        with_lock(State_->Mutex) {
            auto &op = State_->Operations[operationId];
            if (!op) {
                op = std::make_shared<TOperationMap>();
            }

            if (forWrite) {
                Y_ENSURE(!op->WriteMap);
                op->WriteMap = std::make_shared<TOperationMap::TMap>();
            } else {
                if (!op->ReadMap) {
                    op->ReadMap = std::make_shared<TOperationMap::TMap>();
                }
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

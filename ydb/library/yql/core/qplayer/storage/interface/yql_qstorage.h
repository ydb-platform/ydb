#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/digest/numeric.h>
#include <util/generic/string.h>
#include <memory>

namespace NYql {

struct TQItemKey {
    TString Component;
    TString Label;

    bool operator==(const TQItemKey& other) const {
        return Component == other.Component && Label == other.Label;
    }

    bool operator<(const TQItemKey& other) const {
        return Component < other.Component || Component == other.Component && Label < other.Label;
    }

    size_t Hash() const {
        return CombineHashes(
            THash<TString>()(Component),
            THash<TString>()(Label)
        );
    }
};

struct TQItem {
    TQItemKey Key;
    TString Value;

    bool operator<(const TQItem& other) const {
        return Key < other.Key;
    }
};

class IQReader {
public:
    virtual ~IQReader() = default;

    virtual NThreading::TFuture<TMaybe<TQItem>> Get(const TQItemKey& key) const = 0;
};

using IQReaderPtr = std::shared_ptr<IQReader>;

class IQWriter {
public:
    virtual ~IQWriter() = default;

    virtual NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) = 0;
    // Commmit should be called at most once, no more Put are allowed after it
    virtual NThreading::TFuture<void> Commit() = 0;
};

using IQWriterPtr = std::shared_ptr<IQWriter>;

class IQIterator {
public:
    virtual ~IQIterator() = default;

    virtual NThreading::TFuture<TMaybe<TQItem>> Next() = 0;
};

using IQIteratorPtr = std::shared_ptr<IQIterator>;

struct TQWriterSettings {
    TMaybe<TInstant> WrittenAt;
    TMaybe<ui64> ItemsLimit;
    TMaybe<ui64> BytesLimit;
};

struct TQReaderSettings {
};

struct TQIteratorSettings {
    TMaybe<ui64> ItemsLimit;
    TMaybe<ui64> BytesLimit;
    TMaybe<ui32> ConcurrencyLimit;
};

class IQStorage {
public:
    virtual ~IQStorage() = default;

    // it's an UB to open writer twice for the same operationId, implementations may check it
    virtual IQWriterPtr MakeWriter(const TString& operationId, const TQWriterSettings& writerSettings) const = 0;
    // readers & iterators may not see results of writer until commit
    virtual IQReaderPtr MakeReader(const TString& operationId, const TQReaderSettings& readerSettings) const = 0;
    virtual IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& iteratorSettings) const = 0;
};

using IQStoragePtr = std::shared_ptr<IQStorage>;

class TQContext {
public:
    TQContext()
    {}

    TQContext(IQReaderPtr reader)
        : Reader_(reader)
    {}

    TQContext(IQWriterPtr writer)
        : Writer_(writer)
    {}

    TQContext(const TQContext&) = default;
    TQContext& operator=(const TQContext&) = default;

    operator bool() const {
        return CanRead() || CanWrite();
    }

    bool CanRead() const {
        return Reader_ != nullptr;
    }

    bool CanWrite() const {
        return Writer_ != nullptr;
    }

    const IQReaderPtr& GetReader() const {
        return Reader_;
    }

    const IQWriterPtr& GetWriter() const {
        return Writer_;
    }

private:
    IQReaderPtr Reader_;
    IQWriterPtr Writer_;
};

}

template <>
struct THash<NYql::TQItemKey> {
    size_t operator()(const NYql::TQItemKey& value) const {
        return value.Hash();
    }
};

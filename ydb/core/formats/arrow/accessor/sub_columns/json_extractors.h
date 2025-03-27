#pragma once
#include "direct_builder.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TJsonStorage {
private:
    std::deque<TString> Data;

public:
    TStringBuf Store(const TString& data) {
        Data.emplace_back(data);
        return TStringBuf(Data.back().data(), Data.back().size());
    }
};

class IJsonObjectExtractor {
private:
    const TStringBuf Prefix;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) = 0;

protected:
    TStringBuf GetPrefix() const {
        return Prefix;
    }

public:
    virtual ~IJsonObjectExtractor() = default;

    IJsonObjectExtractor(const TStringBuf prefix)
        : Prefix(prefix) {
    }

    [[nodiscard]] TConclusionStatus Fill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) {
        return DoFill(dataBuilder, iterators);
    }
};

class TKVExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TObjectIterator Iterator;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) override;
    const bool FirstLevelOnly = false;

public:
    TKVExtractor(const NBinaryJson::TObjectIterator& iterator, const TStringBuf prefix, const bool firstLevelOnly = false)
        : TBase(prefix)
        , Iterator(iterator)
        , FirstLevelOnly(firstLevelOnly) {
    }
};

class TArrayExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TArrayIterator Iterator;

    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TArrayExtractor(const NBinaryJson::TArrayIterator& iterator, const TStringBuf prefix)
        : TBase(prefix)
        , Iterator(iterator) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

#pragma once
#include "direct_builder.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/simdjson/include/simdjson.h>
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class IJsonObjectExtractor {
private:
    const TStringBuf Prefix;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) = 0;

protected:
    const bool FirstLevelOnly = false;
    TStringBuf GetPrefix() const {
        return Prefix;
    }

    [[nodiscard]] TConclusionStatus AddDataToBuilder(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators,
        const TStringBuf key, const NBinaryJson::TEntryCursor& value) const;

public:
    virtual ~IJsonObjectExtractor() = default;

    IJsonObjectExtractor(const TStringBuf prefix, const bool firstLevelOnly)
        : Prefix(prefix)
        , FirstLevelOnly(firstLevelOnly) {
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

public:
    TKVExtractor(const NBinaryJson::TObjectIterator& iterator, const TStringBuf prefix, const bool firstLevelOnly = false)
        : TBase(prefix, firstLevelOnly)
        , Iterator(iterator) {
    }
};

class TArrayExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TArrayIterator Iterator;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TArrayExtractor(const NBinaryJson::TArrayIterator& iterator, const TStringBuf prefix, const bool firstLevelOnly = false)
        : TBase(prefix, firstLevelOnly)
        , Iterator(iterator) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

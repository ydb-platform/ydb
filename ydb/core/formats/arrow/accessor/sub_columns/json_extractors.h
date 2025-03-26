#pragma once
#include "direct_builder.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class IJsonObjectExtractor {
private:
    std::vector<TStringBuf> Prefix;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) = 0;

protected:
    std::vector<TStringBuf> GetPrefixWith(const TStringBuf key) const {
        auto result = Prefix;
        result.emplace_back(key);
        return result;
    }
    const std::vector<TStringBuf>& GetPrefix() const {
        return Prefix;
    }

public:
    virtual ~IJsonObjectExtractor() = default;

    IJsonObjectExtractor(const std::vector<TStringBuf>& prefix)
        : Prefix(prefix) {
    }

    [[nodiscard]] TConclusionStatus Fill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) {
        return DoFill(dataBuilder, iterators);
    }
};

class TKVExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TObjectIterator Iterator;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TKVExtractor(const NBinaryJson::TObjectIterator& iterator, const std::vector<TStringBuf>& prefix)
        : TBase(prefix)
        , Iterator(iterator) {
    }
};

class TArrayExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TArrayIterator Iterator;

    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TArrayExtractor(const NBinaryJson::TArrayIterator& iterator, const std::vector<TStringBuf>& prefix)
        : TBase(prefix)
        , Iterator(iterator) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

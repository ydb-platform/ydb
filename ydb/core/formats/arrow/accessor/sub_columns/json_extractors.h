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
    std::vector<TStringBuf> Prefix;
    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) = 0;

protected:
    std::shared_ptr<TJsonStorage> Storage;

    std::vector<TStringBuf> GetPrefixWith(const TStringBuf key) {
        auto result = Prefix;
        if (key.find(".") != TString::npos) {
            result.emplace_back(Storage->Store(TString("'") + key + "'"));
        } else {
            result.emplace_back(key);
        }
        return result;
    }
    std::vector<TStringBuf> GetPrefixWithOwn(const TString& key) {
        auto result = Prefix;
        if (key.find(".")) {
            result.emplace_back(Storage->Store("'" + key + "'"));
        } else {
            result.emplace_back(Storage->Store(key));
        }
        return result;
    }
    const std::vector<TStringBuf>& GetPrefix() const {
        return Prefix;
    }

public:
    virtual ~IJsonObjectExtractor() = default;

    IJsonObjectExtractor(const std::shared_ptr<TJsonStorage>& storage, const std::vector<TStringBuf>& prefix)
        : Prefix(prefix)
        , Storage(storage) {
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
    TKVExtractor(
        const std::shared_ptr<TJsonStorage>& storage, const NBinaryJson::TObjectIterator& iterator, const std::vector<TStringBuf>& prefix)
        : TBase(storage, prefix)
        , Iterator(iterator) {
    }
};

class TArrayExtractor: public IJsonObjectExtractor {
private:
    using TBase = IJsonObjectExtractor;
    NBinaryJson::TArrayIterator Iterator;

    virtual TConclusionStatus DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) override;

public:
    TArrayExtractor(
        const std::shared_ptr<TJsonStorage>& storage, const NBinaryJson::TArrayIterator& iterator, const std::vector<TStringBuf>& prefix)
        : TBase(storage, prefix)
        , Iterator(iterator) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

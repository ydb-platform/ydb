#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/string_view.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

template <class TArrowInt>
class TIntSeqFiller {
public:
    using TValue = TArrowInt;
    typename TArrowInt::c_type GetValue(const ui32 idx) const {
        return idx;
    }
};

class TStringPoolFiller {
private:
    std::vector<TString> Data;
public:
    using TValue = arrow::StringType;
    arrow::util::string_view GetValue(const ui32 idx) const;

    TStringPoolFiller(const ui32 poolSize, const ui32 strLen);
};

template <class TValueExt>
class TLinearArrayAccessor {
private:
    using TArray = typename arrow::TypeTraits<TValueExt>::ArrayType;
    const TArray& Data;
public:
    using TValue = TValueExt;
    auto GetValue(const ui32 idx) const {
        return Data.Value(idx);
    }

    TLinearArrayAccessor(const arrow::Array& data)
        : Data(static_cast<const TArray&>(data)) {
    }
};

template <class TDictionaryValue, class TIndices>
class TDictionaryArrayAccessor {
private:
    using TDictionary = typename arrow::TypeTraits<TDictionaryValue>::ArrayType;
    const TDictionary& Dictionary;
    const TIndices& Indices;
public:
    using TValue = TDictionaryValue;
    auto GetValue(const ui32 idx) const {
        return Dictionary.Value(Indices.Value(idx));
    }

    TDictionaryArrayAccessor(const TDictionary& dictionary, const TIndices& indices)
        : Dictionary(dictionary)
        , Indices(indices)
    {
    }
};

}

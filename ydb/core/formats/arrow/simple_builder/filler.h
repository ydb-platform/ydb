#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/string_view.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NArrow::NConstruction {

template <class TArrowInt>
class TIntSeqFiller {
public:
    using TValue = TArrowInt;

private:
    using CType = typename TArrowInt::c_type;
    const CType Delta;

public:
    CType GetValue(const CType idx) const {
        return Delta + idx;
    }
    TIntSeqFiller(const CType delta = 0)
        : Delta(delta) {
    }
};

template <class TArrowInt>
class TIntConstFiller {
public:
    using TValue = TArrowInt;

private:
    using CType = typename TArrowInt::c_type;
    const CType Value;

public:
    CType GetValue(const CType /*idx*/) const {
        return Value;
    }
    TIntConstFiller(const CType value)
        : Value(value) {
    }
};

class TStringPoolFiller {
private:
    std::vector<TString> Data;

public:
    using TValue = arrow::StringType;
    arrow::util::string_view GetValue(const ui32 idx) const;

    TStringPoolFiller(const ui32 poolSize, const ui32 strLen, const TString& defaultValue = "", const double defaultValueFrq = 0);
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

template <class TValueExt>
class TBinaryArrayAccessor {
private:
    using TArray = typename arrow::TypeTraits<TValueExt>::ArrayType;
    const TArray& Data;

public:
    using TValue = TValueExt;
    const char* GetValueView(const ui32 idx) const {
        return Data.GetView(idx).data();
    }

    TBinaryArrayAccessor(const arrow::Array& data)
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
        , Indices(indices) {
    }
};

template <class TDictionaryValue, class TIndices>
class TBinaryDictionaryArrayAccessor {
private:
    using TDictionary = typename arrow::TypeTraits<TDictionaryValue>::ArrayType;
    const TDictionary& Dictionary;
    const TIndices& Indices;
    std::vector<TString> DictionaryStrings;

public:
    using TValue = TDictionaryValue;
    const char* GetValueView(const ui32 idx) const {
        return DictionaryStrings[Indices.Value(idx)].data();
    }

    TBinaryDictionaryArrayAccessor(const TDictionary& dictionary, const TIndices& indices)
        : Dictionary(dictionary)
        , Indices(indices) {
        DictionaryStrings.reserve(Dictionary.length());
        for (i64 idx = 0; idx < Dictionary.length(); ++idx) {
            auto sView = Dictionary.Value(idx);
            DictionaryStrings.emplace_back(TString(sView.data(), sView.size()));
        }
    }
};

}   // namespace NKikimr::NArrow::NConstruction

#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NQueryClient {

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

using TTypeParameter = int;
using TUnionType = std::vector<EValueType>;
using TType = std::variant<EValueType, TTypeParameter, TUnionType>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECallingConvention,
    ((Simple)           (0))
    ((UnversionedValue) (1))
);

DEFINE_ENUM(ETypeTag,
    ((ConcreteType) (0))
    ((TypeParameter) (1))
    ((UnionType)    (2))
);

////////////////////////////////////////////////////////////////////////////////

class TTypeSet
{
public:
    TTypeSet()
        : Value_(0)
    { }

    explicit TTypeSet(ui64 value)
        : Value_(value)
    { }

    TTypeSet(std::initializer_list<EValueType> values)
        : Value_(0)
    {
        Assign(values.begin(), values.end());
    }

    template <class TIterator>
    TTypeSet(TIterator begin, TIterator end)
        : Value_(0)
    {
        Assign(begin, end);
    }

    template <class TIterator>
    void Assign(TIterator begin, TIterator end)
    {
        Value_ = 0;
        for (; begin != end; ++begin) {
            Set(*begin);
        }
    }

    void Set(EValueType type)
    {
        Value_ |= 1 << ui8(type);
    }

    bool Get(EValueType type) const
    {
        return Value_ & (1 << ui8(type));
    }

    EValueType GetFront() const;

    bool IsEmpty() const
    {
        return Value_ == 0;
    }

    size_t GetSize() const;

    template <class TFunctor>
    void ForEach(TFunctor functor) const
    {
        ui64 mask = 1;
        for (size_t index = 0; index < 8 * sizeof(ui64); ++index, mask <<= 1) {
            if (Value_ & mask) {
                functor(EValueType(index));
            }
        }
    }

    friend TTypeSet operator | (const TTypeSet& lhs, const TTypeSet& rhs);
    friend TTypeSet operator & (const TTypeSet& lhs, const TTypeSet& rhs);

private:
    ui64 Value_ = 0;

};

void FormatValue(TStringBuilderBase* builder, const TTypeSet& typeSet, TStringBuf spec);
TString ToString(const TTypeSet& typeSet);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

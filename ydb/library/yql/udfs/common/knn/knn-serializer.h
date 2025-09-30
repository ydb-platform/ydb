#pragma once

#include "knn-defines.h"
#include "knn-enumerator.h"
#include "knn-serializer-shared.h"

#include <yql/essentials/public/udf/udf_helpers.h>

#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

template <typename TTo, typename TFrom = TTo>
class TKnnVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&](IOutputStream& outStream) {
            NKnnVectorSerialization::TSerializer<TTo> serializer(&outStream);
            EnumerateVector<TFrom>(x, [&](TFrom from) {
                serializer.HandleElement(from);
            });
            serializer.Finish();
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(NKnnVectorSerialization::GetBufferSize<TTo>(x.GetListLength()));
            auto strRef = str.AsStringRef();
            TMemoryOutput memoryOutput(strRef.Data(), strRef.Size());

            serialize(memoryOutput);
            return str;
        } else {
            TString str;
            TStringOutput stringOutput(str);

            serialize(stringOutput);
            return valueBuilder->NewString(str);
        }
    }

    static TUnboxedValue Deserialize(const IValueBuilder* valueBuilder, const TStringRef& str) {
        NKnnVectorSerialization::TDeserializer<TTo> deserializer(str);
        if (Y_UNLIKELY(deserializer.GetElementCount() == 0)) {
            return {};
        }

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(deserializer.GetElementCount(), items);

        deserializer.DoDeserialize([&](const TTo& element) {
            *items++ = TUnboxedValuePod{static_cast<TFrom>(element)};
        });

        return res.Release();
    }
};

class TKnnSerializerFacade {
public:
    static TUnboxedValue Deserialize(const IValueBuilder* valueBuilder, const TStringRef& str) {
        if (Y_UNLIKELY(str.Size() == 0)) {
            return {};
        }

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<float>::Deserialize(valueBuilder, str);
            case EFormat::Int8Vector:
                return TKnnVectorSerializer<i8, float>::Deserialize(valueBuilder, str);
            case EFormat::Uint8Vector:
                return TKnnVectorSerializer<ui8, float>::Deserialize(valueBuilder, str);
            case EFormat::BitVector:
            default:
                return {};
        }
    }
};

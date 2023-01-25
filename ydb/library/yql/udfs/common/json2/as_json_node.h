#pragma once

#include "resource.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/minikql/dom/node.h>
#include <ydb/library/yql/minikql/dom/json.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;
    using namespace NDom;

    template <typename TSource>
    class TAsJsonNode: public TBoxedValue {
    public:
        TAsJsonNode(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name();

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (name != Name()) {
                return false;
            }

            auto optionalSourceType = builder.Optional()->Item<TSource>().Build();
            auto resourceType = builder.Resource(JSON_NODE_RESOURCE_NAME);
            builder.Args()
                ->Add(optionalSourceType)
                .Done()
                .Returns(resourceType);

            if (!typesOnly) {
                builder.Implementation(new TAsJsonNode<TSource>(builder.GetSourcePosition()));
            }

            builder.IsStrict();
            return true;
        }

    private:
        const size_t MaxParseErrors = 10;

        static TUnboxedValue Interpret(const TUnboxedValue& sourceValue, const IValueBuilder* valueBuilder);

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            try {
                if (!args[0].HasValue()) {
                    return MakeEntity();
                }
                return Interpret(args[0], valueBuilder);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    template <>
    TStringRef TAsJsonNode<TUtf8>::Name() {
        return TStringRef::Of("Utf8AsJsonNode");
    }

    template <>
    TUnboxedValue TAsJsonNode<TUtf8>::Interpret(const TUnboxedValue& sourceValue, const IValueBuilder* valueBuilder) {
        return MakeString(sourceValue.AsStringRef(), valueBuilder);
    }

    template <>
    TStringRef TAsJsonNode<double>::Name() {
        return TStringRef::Of("DoubleAsJsonNode");
    }

    template <>
    TUnboxedValue TAsJsonNode<double>::Interpret(const TUnboxedValue& sourceValue, const IValueBuilder* valueBuilder) {
        Y_UNUSED(valueBuilder);
        return MakeDouble(sourceValue.Get<double>());
    }

    template <>
    TStringRef TAsJsonNode<bool>::Name() {
        return TStringRef::Of("BoolAsJsonNode");
    }

    template <>
    TUnboxedValue TAsJsonNode<bool>::Interpret(const TUnboxedValue& sourceValue, const IValueBuilder* valueBuilder) {
        Y_UNUSED(valueBuilder);
        return MakeBool(sourceValue.Get<bool>());
    }

    template <>
    TStringRef TAsJsonNode<TJson>::Name() {
        return TStringRef::Of("JsonAsJsonNode");
    }

    template <>
    TUnboxedValue TAsJsonNode<TJson>::Interpret(const TUnboxedValue& sourceValue, const IValueBuilder* valueBuilder) {
        return TryParseJsonDom(sourceValue.AsStringRef(), valueBuilder);
    }
}


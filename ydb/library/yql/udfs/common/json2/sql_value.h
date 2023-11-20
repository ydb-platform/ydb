#pragma once

#include "resource.h"
#include "compile_path.h"

#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/minikql/dom/node.h>

#include <ydb/library/binary_json/read.h>

#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>
#include <util/string/cast.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;
    using namespace NDom;
    using namespace NJsonPath;

    namespace {
        template <class TValueType, bool ForceConvert = false>
        TUnboxedValue TryConvertJson(const IValueBuilder* valueBuilder, const TUnboxedValue& source) {
            Y_UNUSED(valueBuilder);
            Y_UNUSED(source);
            Y_ABORT("Unsupported type");
        }

        template <>
        TUnboxedValue TryConvertJson<TUtf8>(const IValueBuilder* valueBuilder, const TUnboxedValue& source) {
            Y_UNUSED(valueBuilder);
            if (IsNodeType(source, ENodeType::String)) {
                return source;
            }
            return {};
        }

        template <>
        TUnboxedValue TryConvertJson<TUtf8, true>(const IValueBuilder* valueBuilder, const TUnboxedValue& source) {
            switch (GetNodeType(source)) {
                case ENodeType::String:
                    return source;
                case ENodeType::Uint64:
                    return valueBuilder->NewString(ToString(source.Get<ui64>())).Release();
                case ENodeType::Int64:
                    return valueBuilder->NewString(ToString(source.Get<i64>())).Release();
                case ENodeType::Bool:
                    return source.Get<bool>() ? TUnboxedValuePod::Embedded("true") : TUnboxedValuePod::Embedded("false");
                case ENodeType::Double:
                    return valueBuilder->NewString(ToString(source.Get<double>())).Release();
                case ENodeType::Entity:
                    return TUnboxedValuePod::Embedded("null");
                case ENodeType::List:
                case ENodeType::Dict:
                case ENodeType::Attr:
                    return {};
            }
        }

        template <>
        TUnboxedValue TryConvertJson<i64>(const IValueBuilder* valueBuilder, const TUnboxedValue& source) {
            Y_UNUSED(valueBuilder);
            if (!source.IsEmbedded()) {
                return {};
            }

            if (IsNodeType(source, ENodeType::Int64)) {
                return TUnboxedValuePod(source.Get<i64>());
            } else if (IsNodeType(source, ENodeType::Uint64) && source.Get<ui64>() < Max<i64>()) {
                return TUnboxedValuePod(static_cast<i64>(source.Get<ui64>()));
            } else if (IsNodeType(source, ENodeType::Double) && static_cast<i64>(source.Get<double>()) == source.Get<double>()) {
                return TUnboxedValuePod(static_cast<i64>(source.Get<double>()));
            }

            return {};
        }

        template <>
        TUnboxedValue TryConvertJson<double>(const IValueBuilder* valueBuilder, const TUnboxedValue& source) {
            Y_UNUSED(valueBuilder);
            if (!source.IsEmbedded()) {
                return {};
            }

            if (IsNodeType(source, ENodeType::Double)) {
                return TUnboxedValuePod(source.Get<double>());
            } else if (IsNodeType(source, ENodeType::Int64)) {
                return TUnboxedValuePod(static_cast<double>(source.Get<i64>()));
            } else if (IsNodeType(source, ENodeType::Uint64)) {
                return TUnboxedValuePod(static_cast<double>(source.Get<ui64>()));
            }

            return {};
        }

        template <>
        TUnboxedValue TryConvertJson<bool>(const IValueBuilder* valueBuilder, const TUnboxedValue& source) {
            Y_UNUSED(valueBuilder);
            if (!source.IsEmbedded() || !IsNodeType(source, ENodeType::Bool)) {
                return {};
            }
            return {TUnboxedValuePod(source.Get<bool>())};
        }
    }

    template <EDataSlot InputType, class TValueType, bool ForceConvert = false>
    class TSqlValue: public TBoxedValue {
    public:
        enum class TErrorCode : ui8 {
            Empty = 0,
            Error = 1
        };

        TSqlValue(TSourcePosition pos)
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

            auto optionalValueType = builder.Optional()->Item<TValueType>().Build();
            auto errorTupleType = builder.Tuple(2)->Add<ui8>().Add<char*>().Build();
            auto returnTypeTuple = builder.Tuple(2)
                ->Add(errorTupleType)
                .Add(optionalValueType)
                .Build();
            auto returnType = builder.Variant()->Over(returnTypeTuple).Build();

            TType* jsonType = nullptr;
            if constexpr (InputType == EDataSlot::Json) {
                jsonType = builder.Resource(JSON_NODE_RESOURCE_NAME);
            } else {
                jsonType = builder.SimpleType<TJsonDocument>();
            }
            auto optionalJsonType = builder.Optional()->Item(jsonType).Build();
            auto jsonPathType = builder.Resource(JSONPATH_RESOURCE_NAME);
            auto dictType = builder.Dict()->Key<TUtf8>().Value(builder.Resource(JSON_NODE_RESOURCE_NAME)).Build();

            builder.Args()
                ->Add(optionalJsonType)
                .Add(jsonPathType)
                .Add(dictType)
                .Done()
                .Returns(returnType);

            builder.IsStrict();

            if (!typesOnly) {
                builder.Implementation(new TSqlValue(builder.GetSourcePosition()));
            }
            return true;
        }

    private:
        TUnboxedValue BuildErrorResult(const IValueBuilder* valueBuilder, TErrorCode code, const TStringBuf message) const {
            TUnboxedValue* items = nullptr;
            auto errorTuple = valueBuilder->NewArray(2, items);
            items[0] = TUnboxedValuePod(static_cast<ui8>(code));
            items[1] = valueBuilder->NewString(message);
            return valueBuilder->NewVariant(0, std::move(errorTuple));
        }

        TUnboxedValue BuildSuccessfulResult(const IValueBuilder* valueBuilder, TUnboxedValue&& value) const {
            return valueBuilder->NewVariant(1, std::move(value));
        }

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final {
            try {
                if (!args[0].HasValue()) {
                    return BuildSuccessfulResult(valueBuilder, TUnboxedValuePod());
                }

                TValue jsonDom;
                if constexpr (InputType == EDataSlot::JsonDocument) {
                    jsonDom = TValue(NBinaryJson::TBinaryJsonReader::Make(args[0].AsStringRef())->GetRootCursor());
                } else {
                    jsonDom = TValue(args[0]);
                }

                auto* jsonPathResource = static_cast<TJsonPathResource*>(args[1].AsBoxed().Get());
                const auto& jsonPath = *jsonPathResource->Get();
                const auto variables = DictToVariables(args[2]);

                const auto result = ExecuteJsonPath(jsonPath, jsonDom, variables, valueBuilder);

                if (result.IsError()) {
                    return BuildErrorResult(valueBuilder, TErrorCode::Error, TStringBuilder() << "Error executing jsonpath:" << Endl << result.GetError() << Endl);
                }

                const auto& nodes = result.GetNodes();
                if (nodes.empty()) {
                    return BuildErrorResult(valueBuilder, TErrorCode::Empty, "Result is empty");
                }

                if (nodes.size() > 1) {
                    return BuildErrorResult(valueBuilder, TErrorCode::Error, "Result consists of multiple items");
                }

                const auto& value = nodes[0];
                if (value.Is(EValueType::Array) || value.Is(EValueType::Object)) {
                    // SqlValue can return only scalar values
                    return BuildErrorResult(valueBuilder, TErrorCode::Error, "Extracted JSON value is either object or array");
                }

                if (value.Is(EValueType::Null)) {
                    // JSON nulls must be converted to SQL nulls
                    return BuildSuccessfulResult(valueBuilder, TUnboxedValuePod());
                }

                const auto source = value.ConvertToUnboxedValue(valueBuilder);
                TUnboxedValue convertedValue = TryConvertJson<TValueType, ForceConvert>(valueBuilder, source);
                if (!convertedValue) {
                    // error while converting JSON value type to TValueType
                    return BuildErrorResult(valueBuilder, TErrorCode::Error, "Cannot convert extracted JSON value to target type");
                }

                return BuildSuccessfulResult(valueBuilder, std::move(convertedValue));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    template <EDataSlot InputType, class TValueType, bool ForceConvert>
    TStringRef TSqlValue<InputType, TValueType, ForceConvert>::Name() {
        Y_ABORT("Unknown name");
    }

    template<>
    TStringRef TSqlValue<EDataSlot::Json, TUtf8, true>::Name() {
        return TStringRef::Of("SqlValueConvertToUtf8");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::Json, TUtf8>::Name() {
        return TStringRef::Of("SqlValueUtf8");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::Json, i64>::Name() {
        return TStringRef::Of("SqlValueInt64");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::Json, double>::Name() {
        return TStringRef::Of("SqlValueNumber");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::Json, bool>::Name() {
        return TStringRef::Of("SqlValueBool");
    }

    template<>
    TStringRef TSqlValue<EDataSlot::JsonDocument, TUtf8, true>::Name() {
        return TStringRef::Of("JsonDocumentSqlValueConvertToUtf8");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::JsonDocument, TUtf8>::Name() {
        return TStringRef::Of("JsonDocumentSqlValueUtf8");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::JsonDocument, i64>::Name() {
        return TStringRef::Of("JsonDocumentSqlValueInt64");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::JsonDocument, double>::Name() {
        return TStringRef::Of("JsonDocumentSqlValueNumber");
    }

    template <>
    TStringRef TSqlValue<EDataSlot::JsonDocument, bool>::Name() {
        return TStringRef::Of("JsonDocumentSqlValueBool");
    }

}

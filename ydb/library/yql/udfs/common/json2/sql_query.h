#pragma once

#include "resource.h"
#include "compile_path.h"

#include <ydb/library/yql/core/yql_atom_enums.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/minikql/dom/node.h>

#include <util/generic/yexception.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;
    using namespace NDom;
    using namespace NJsonPath;

    template <EDataSlot InputType, EJsonQueryWrap Mode>
    class TSqlQuery: public TBoxedValue {
    public:
        explicit TSqlQuery(TSourcePosition pos)
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

            auto jsonType = builder.Resource(JSON_NODE_RESOURCE_NAME);
            auto optionalJsonType = builder.Optional()->Item(jsonType).Build();
            TType* inputType = nullptr;
            if constexpr (InputType == EDataSlot::JsonDocument) {
                inputType = builder.SimpleType<TJsonDocument>();
            } else {
                inputType = jsonType;
            }
            auto inputOptionalType = builder.Optional()->Item(inputType).Build();
            auto jsonPathType = builder.Resource(JSONPATH_RESOURCE_NAME);
            auto dictType = builder.Dict()->Key<TUtf8>().Value(jsonType).Build();

            /*
            Arguments:
                0. Resource<JsonNode>? or JsonDocument?. Input json
                1. Resource<JsonPath>. Jsonpath to execute on json
                2. Dict<TUtf8, Resource<JsonNode>>. Variables to pass into jsonpath
                3. Bool. True - throw on empty result, false otherwise
                4. Resource<JsonNode>?. Default value to return on empty result. Ignored if 2d argument is true
                5. Bool. True - throw on error, false - otherwise
                6. Resource<JsonNode>?. Default value to return on error. Ignored if 4th argument is true
            */
            // we can't mark TSqlQuery as strict due to runtime throw policy setting
            // TODO: optimizer can mark SqlQuery as strict if 3th/5th arguments are literal booleans
            builder.Args()
                ->Add(inputOptionalType)
                .Add(jsonPathType)
                .Add(dictType)
                .Add<bool>()
                .Add(optionalJsonType)
                .Add<bool>()
                .Add(optionalJsonType)
                .Done()
                .Returns(optionalJsonType);

            if (!typesOnly) {
                builder.Implementation(new TSqlQuery(builder.GetSourcePosition()));
            }
            return true;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            try {
                if (!args[0].HasValue()) {
                    return TUnboxedValuePod();
                }

                TValue jsonDom;
                if constexpr (InputType == EDataSlot::JsonDocument) {
                    jsonDom = TValue(NBinaryJson::TBinaryJsonReader::Make(args[0].AsStringRef())->GetRootCursor());
                } else {
                    jsonDom = TValue(args[0]);
                }

                auto* jsonPathResource = static_cast<TJsonPathResource*>(args[1].AsBoxed().Get());
                const auto& jsonPath = *jsonPathResource->Get();

                const bool throwOnEmpty = args[3].Get<bool>();
                const auto emptyDefault = args[4];
                const bool throwOnError = args[5].Get<bool>();
                const auto errorDefault = args[6];
                const auto variables = DictToVariables(args[2]);

                auto result = ExecuteJsonPath(jsonPath, jsonDom, variables, valueBuilder);

                const auto handleCase = [](TStringBuf message, bool throws, const TUnboxedValuePod& caseDefault) {
                    if (throws) {
                        ythrow yexception() << message;
                    }
                    return caseDefault;
                };

                if (result.IsError()) {
                    return handleCase(TStringBuilder() << "Error executing jsonpath:" << Endl << result.GetError() << Endl, throwOnError, errorDefault);
                }

                auto& nodes = result.GetNodes();
                const bool isSingleStruct = nodes.size() == 1 && (nodes[0].Is(EValueType::Array) || nodes[0].Is(EValueType::Object));
                if (Mode == EJsonQueryWrap::Wrap || (Mode == EJsonQueryWrap::ConditionalWrap && !isSingleStruct)) {
                    TVector<TUnboxedValue> converted;
                    converted.reserve(nodes.size());
                    for (auto& node : nodes) {
                        converted.push_back(node.ConvertToUnboxedValue(valueBuilder));
                    }
                    return MakeList(converted.data(), converted.size(), valueBuilder);
                }

                if (nodes.empty()) {
                    return handleCase("Empty result", throwOnEmpty, emptyDefault);
                }

                // No wrapping is applicable and result is not empty. Result must be a single object or array
                if (nodes.size() > 1) {
                    return handleCase("Result consists of multiple items", throwOnError, errorDefault);
                }

                if (!nodes[0].Is(EValueType::Array) && !nodes[0].Is(EValueType::Object)) {
                    return handleCase("Result is neither object nor array", throwOnError, errorDefault);
                }

                return nodes[0].ConvertToUnboxedValue(valueBuilder);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    template <>
    TStringRef TSqlQuery<EDataSlot::Json, EJsonQueryWrap::NoWrap>::Name() {
        return "SqlQuery";
    }

    template <>
    TStringRef TSqlQuery<EDataSlot::Json, EJsonQueryWrap::Wrap>::Name() {
        return "SqlQueryWrap";
    }

    template <>
    TStringRef TSqlQuery<EDataSlot::Json, EJsonQueryWrap::ConditionalWrap>::Name() {
        return "SqlQueryConditionalWrap";
    }

    template <>
    TStringRef TSqlQuery<EDataSlot::JsonDocument, EJsonQueryWrap::NoWrap>::Name() {
        return "JsonDocumentSqlQuery";
    }

    template <>
    TStringRef TSqlQuery<EDataSlot::JsonDocument, EJsonQueryWrap::Wrap>::Name() {
        return "JsonDocumentSqlQueryWrap";
    }

    template <>
    TStringRef TSqlQuery<EDataSlot::JsonDocument, EJsonQueryWrap::ConditionalWrap>::Name() {
        return "JsonDocumentSqlQueryConditionalWrap";
    }
}


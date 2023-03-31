#pragma once

#include "resource.h"
#include "compile_path.h"

#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/yexception.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;
    using namespace NJsonPath;

    template <EDataSlot InputType, bool ThrowException>
    class TSqlExists: public TBoxedValue {
    public:
        explicit TSqlExists(TSourcePosition pos)
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
            TType* inputType = nullptr;
            if constexpr (InputType == EDataSlot::JsonDocument) {
                inputType = builder.SimpleType<TJsonDocument>();
            } else {
                inputType = jsonType;
            }
            auto inputOptionalType = builder.Optional()->Item(inputType).Build();
            auto jsonPathType = builder.Resource(JSONPATH_RESOURCE_NAME);
            auto dictType = builder.Dict()->Key<TUtf8>().Value(jsonType).Build();
            auto optionalBoolType = builder.Optional()->Item<bool>().Build();

            if constexpr (ThrowException) {
                builder.Args()
                    ->Add(inputOptionalType)
                    .Add(jsonPathType)
                    .Add(dictType)
                    .Done()
                    .Returns(optionalBoolType);
            } else {
                builder.Args()
                    ->Add(inputOptionalType)
                    .Add(jsonPathType)
                    .Add(dictType)
                    .Add(optionalBoolType)
                    .Done()
                    .Returns(optionalBoolType);
            }

            if (!typesOnly) {
                builder.Implementation(new TSqlExists(builder.GetSourcePosition()));
            }
            if constexpr (!ThrowException) {
                builder.IsStrict();
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
                const auto variables = DictToVariables(args[2]);

                const auto result = ExecuteJsonPath(jsonPath, jsonDom, variables, valueBuilder);
                if (result.IsError()) {
                    if constexpr (ThrowException) {
                        ythrow yexception() << "Error executing jsonpath:" << Endl << result.GetError() << Endl;
                    } else {
                        return args[3];
                    }
                }

                return TUnboxedValuePod(!result.GetNodes().empty());
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    template <>
    TStringRef TSqlExists<EDataSlot::Json, false>::Name() {
        return "SqlExists";
    }

    template <>
    TStringRef TSqlExists<EDataSlot::Json, true>::Name() {
        return "SqlTryExists";
    }

    template <>
    TStringRef TSqlExists<EDataSlot::JsonDocument, false>::Name() {
        return "JsonDocumentSqlExists";
    }

    template <>
    TStringRef TSqlExists<EDataSlot::JsonDocument, true>::Name() {
        return "JsonDocumentSqlTryExists";
    }
}


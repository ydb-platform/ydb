#pragma once

#include "resource.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;

    class TCompilePath: public TBoxedValue {
    public:
        TCompilePath(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("CompilePath");
            return name;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (name != Name()) {
                return false;
            }

            auto resourceType = builder.Resource(JSONPATH_RESOURCE_NAME);
            builder.Args()
                ->Add<NUdf::TUtf8>()
                .Done()
                .Returns(resourceType);

            if (!typesOnly) {
                builder.Implementation(new TCompilePath(builder.GetSourcePosition()));
            }
            return true;
        }

    private:
        const size_t MaxParseErrors = 10;

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            try {
                TIssues issues;
                const auto jsonPath = NJsonPath::ParseJsonPath(args[0].AsStringRef(), issues, MaxParseErrors);
                if (!issues.Empty()) {
                    ythrow yexception() << "Error parsing jsonpath:" << Endl << issues.ToString();
                }

                return TUnboxedValuePod(new TJsonPathResource(jsonPath));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };
}


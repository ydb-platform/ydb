#pragma once

#include "resource.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/minikql/dom/json.h>

#include <library/cpp/json/json_reader.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;
    using namespace NDom;

    class TParse: public TBoxedValue {
    public:
        TParse(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("Parse");
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

            builder.Args()
                ->Add<TAutoMap<TJson>>()
                .Done()
                .Returns<TJsonNodeResource>();

            if (!typesOnly) {
                builder.Implementation(new TParse(builder.GetSourcePosition()));
            }
            return true;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final {
            Y_UNUSED(valueBuilder);
            try {
                const auto json = args[0].AsStringRef();
                return TryParseJsonDom(json, valueBuilder);
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };
}


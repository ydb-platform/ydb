#pragma once

#include "resource.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/minikql/dom/json.h>

#include <ydb/library/binary_json/write.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;
    using namespace NDom;
    using namespace NBinaryJson;

    template <EDataSlot ResultType>
    class TSerialize : public TBoxedValue {
    public:
        TSerialize(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static const TStringRef& Name();

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            Y_UNUSED(userType);
            if (name != Name()) {
                return false;
            }

            TType* resultType = nullptr;
            if constexpr (ResultType == EDataSlot::Json) {
                resultType = builder.SimpleType<TJson>();
            } else {
                resultType = builder.SimpleType<TJsonDocument>();
            }

            builder.Args()
                ->Add<TAutoMap<TJsonNodeResource>>()
                .Done()
                .Returns(resultType);

            if (!typesOnly) {
                builder.Implementation(new TSerialize(builder.GetSourcePosition()));
            }
            return true;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const final {
            try {
                const TUnboxedValue& jsonDom = args[0];

                if constexpr (ResultType == EDataSlot::Json) {
                    return valueBuilder->NewString(SerializeJsonDom(jsonDom));
                } else {
                    const auto binaryJson = SerializeToBinaryJson(jsonDom);
                    return valueBuilder->NewString(TStringBuf(binaryJson.Data(), binaryJson.Size()));
                }
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    template <>
    const TStringRef& TSerialize<EDataSlot::Json>::Name() {
        static auto name = TStringRef::Of("Serialize");
        return name;
    }

    template <>
    const TStringRef& TSerialize<EDataSlot::JsonDocument>::Name() {
        static auto name = TStringRef::Of("SerializeToJsonDocument");
        return name;
    }
}


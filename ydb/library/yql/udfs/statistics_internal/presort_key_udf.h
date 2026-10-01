#pragma once

#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>

#include <util/system/yassert.h>

namespace NKikimr::NStat::NAggFuncs {

// Scalar UDF: StatisticsInternal::PresortKey(AsTuple(a, b, ...)) -> String
// Encodes a tuple of values into memcomparable byte keys using
// NMiniKQL::TPresortEncoder. The result preserves the intended value order under
// memcmp.
class TPresortKeyFunc : public NYql::NUdf::TBoxedValue {
    struct TElementDesc {
        NYql::NUdf::EDataSlot Slot = NYql::NUdf::EDataSlot::Bool;
        bool IsOptional = false;
    };

    TVector<TElementDesc> Elements_;
    bool IsTuple_ = false;
    NYql::NUdf::TSourcePosition Pos_;
    mutable NMiniKQL::TPresortEncoder Encoder_;

public:
    explicit TPresortKeyFunc(TVector<TElementDesc> elements, bool isTuple,
                             NYql::NUdf::TSourcePosition pos)
        : Elements_(std::move(elements)), IsTuple_(isTuple), Pos_(pos)
    {
        for (const auto& e : Elements_) {
            Encoder_.AddType(e.Slot, e.IsOptional, /*isDesc=*/false);
        }
    }

    static NYql::NUdf::TStringRef Name() {
        static const TString name = "PresortKey";
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef &name,
                                 NYql::NUdf::TType *userType,
                                 NYql::NUdf::IFunctionTypeInfoBuilder &builder,
                                 bool typesOnly) {
        if (name != Name()) {
            return false;
        }

        if (!userType) {
            builder.SetError("User type is not specified");
            return true;
        }
        builder.UserType(userType);

        auto typeHelper = builder.TypeInfoHelper();
        auto userTypeInspector =
            NYql::NUdf::TTupleTypeInspector(*typeHelper, userType);
        if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
            builder.SetError("Invalid user type");
            return true;
        }

        // The userType is a tuple of (args_tuple, optional return type).
        // args_tuple is the first element; unwrap it to get the single argument.
        auto argsTupleType = userTypeInspector.GetElementType(0);
        auto argsTupleInspector =
            NYql::NUdf::TTupleTypeInspector(*typeHelper, argsTupleType);
        if (!argsTupleInspector || argsTupleInspector.GetElementsCount() != 1) {
            builder.SetError("PresortKey expects exactly one argument");
            return true;
        }
        auto argType = argsTupleInspector.GetElementType(0);
        builder.Args()->Add(argType);
        builder.Returns<char *>();

        // Validate even when typesOnly, so Json fails annotation not Encode().
        TVector<TElementDesc> elements;
        bool isTuple = false;

        auto addElement = [&](const NYql::NUdf::TType* elemType) -> bool {
            TElementDesc desc;
            auto optInspector =
                NYql::NUdf::TOptionalTypeInspector(*typeHelper, elemType);
            if (optInspector) {
                desc.IsOptional = true;
                elemType = optInspector.GetItemType();
            }
            auto dataInspector =
                NYql::NUdf::TDataTypeInspector(*typeHelper, elemType);
            if (!dataInspector) {
                return false;
            }
            auto slot = NYql::NUdf::FindDataSlot(dataInspector.GetTypeId());
            if (!slot) {
                return false;
            }
            desc.Slot = *slot;
            // Encode() has no case for these.
            switch (desc.Slot) {
            case NYql::NUdf::EDataSlot::Json:
            case NYql::NUdf::EDataSlot::Yson:
            case NYql::NUdf::EDataSlot::JsonDocument:
                return false;
            default:
                break;
            }
            elements.push_back(desc);
            return true;
        };

        // If argType is a Tuple, walk its elements; otherwise treat it as a
        // 1-element tuple.
        auto argTupleInspector =
            NYql::NUdf::TTupleTypeInspector(*typeHelper, argType);
        if (argTupleInspector) {
            isTuple = true;
            for (ui32 i = 0; i < argTupleInspector.GetElementsCount(); ++i) {
                if (!addElement(argTupleInspector.GetElementType(i))) {
                    builder.SetError("Unsupported element type in PresortKey");
                    return true;
                }
            }
            if (elements.empty()) {
                builder.SetError("PresortKey expects a non-empty tuple");
                return true;
            }
        } else if (!addElement(argType)) {
            builder.SetError("Unsupported argument type in PresortKey");
            return true;
        }

        if (!typesOnly) {
            builder.Implementation(new TPresortKeyFunc(std::move(elements), isTuple,
                                                     GetSourcePosition(builder)));
        }
        return true;
    }

private:
    NYql::NUdf::TUnboxedValue
    Run(const NYql::NUdf::IValueBuilder *valueBuilder,
        const NYql::NUdf::TUnboxedValuePod *args) const override {
        try {
            Y_DEBUG_ABORT_UNLESS(!Elements_.empty());
            Y_DEBUG_ABORT_UNLESS(IsTuple_ || Elements_.size() == 1);

            Encoder_.Start();
            for (size_t i = 0; i < Elements_.size(); ++i) {
                if (IsTuple_) {
                    Encoder_.Encode(args[0].GetElement(i));
                } else {
                    Encoder_.Encode(args[0]);
                }
            }
            const TStringBuf encoded = Encoder_.Finish();
            return valueBuilder->NewString(
                NYql::NUdf::TStringRef(encoded.data(), encoded.size()));
        } catch (const std::exception &ex) {
            TStringBuilder sb;
            APPEND_SOURCE_LOCATION(sb, valueBuilder, Pos_)
            sb << ex.what();
            UdfTerminate(sb.c_str());
        }
    }
};

} // namespace NKikimr::NStat::NAggFuncs

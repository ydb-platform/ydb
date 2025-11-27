#include <yql/essentials/public/udf/udf_helpers.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

SIMPLE_UDF(TProducer, TLinear<i32>(i32)) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(args[0].Get<i32>());
}

using TExchangeRet = TTuple<TLinear<i32>, i32>;
SIMPLE_UDF(TExchange, TExchangeRet(TLinear<i32>, i32)) {
    TUnboxedValue* items;
    TUnboxedValue ret = valueBuilder->NewArray(2, items);
    items[0] = args[1];
    items[1] = args[0];
    return ret;
}

class TUnsafeConsumer: public TBoxedValue {
public:
    typedef bool TTypeAwareMarker;

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        Y_UNUSED(valueBuilder);
        // it's unsafe to unwrap linear type for mutable values
        return args[0];
    }

    static const TStringRef& Name() {
        static auto Name = TStringRef::Of("UnsafeConsumer");
        return Name;
    }

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (Name() == name) {
            if (!userType) {
                builder.SetError("Missing user type.");
                return true;
            }

            builder.UserType(userType);
            const auto typeHelper = builder.TypeInfoHelper();
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                builder.SetError("Invalid user type.");
                return true;
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                builder.SetError("Invalid user type - expected tuple.");
                return true;
            }

            if (const auto argsCount = argsTypeInspector.GetElementsCount(); argsCount != 1) {
                ::TStringBuilder sb;
                sb << "Invalid user type - expected one argument, got: " << argsCount;
                builder.SetError(sb);
                return true;
            }

            const auto inputType = argsTypeInspector.GetElementType(0);
            const auto linear = TLinearTypeInspector(*typeHelper, inputType);
            if (!linear || linear.IsDynamic()) {
                ::TStringBuilder sb;
                sb << "Expected static linear type";
                builder.SetError(sb);
                return true;
            }

            builder.Args()->Add(inputType).Done().Returns(linear.GetItemType());
            if (!typesOnly) {
                builder.Implementation(new TUnsafeConsumer());
            }

            return true;
        } else {
            return false;
        }
    }
};

SIMPLE_MODULE(TLinearModule, TProducer, TUnsafeConsumer, TExchange)

} // namespace

REGISTER_MODULES(TLinearModule)

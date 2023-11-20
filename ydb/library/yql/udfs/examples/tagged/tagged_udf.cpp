#include <ydb/library/yql/public/udf/udf_helpers.h>

using namespace NKikimr;
using namespace NUdf;

namespace {
    extern const char TagFoo[] = "foo";
    extern const char TagBar[] = "bar";
    extern const char TagBaz[] = "baz";
    using TTaggedFoo = TTagged<i32, TagFoo>;
    using TTaggedBar = TTagged<i32, TagBar>;
    using TTaggedBaz = TTagged<i32, TagBaz>;

    SIMPLE_UDF(TExample, TTaggedBaz(TTaggedFoo, TTaggedBar)) {
        Y_UNUSED(valueBuilder);
        const auto input1 = args[0].Get<i32>();
        const auto input2 = args[1].Get<i32>();
        return TUnboxedValuePod(input1 + input2);
    }

    class TGenericTag : public TBoxedValue {
    public:
        typedef bool TTypeAwareMarker;

        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
            auto tagStr = valueBuilder->NewString(Tag_);
            return valueBuilder->ConcatStrings(args[0], static_cast<const TUnboxedValuePod&>(tagStr));
        }

        static const TStringRef& Name() {
            static auto name = TStringRef::Of("GenericTag");
            return name;
        }

        TGenericTag(TStringRef tag)
            : Tag_(tag)
        {}

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
                const auto tagged = TTaggedTypeInspector(*typeHelper, inputType);
                if (!tagged) {
                    ::TStringBuilder sb;
                    sb << "Expected tagged string";
                    builder.SetError(sb);
                    return true;
                }

                const auto data = TDataTypeInspector(*typeHelper, tagged.GetBaseType());
                if (!data || data.GetTypeId() != TDataType<const char*>::Id) {
                    ::TStringBuilder sb;
                    sb << "Expected tagged string";
                    builder.SetError(sb);
                    return true;
                }

                builder.Args()->Add(inputType).Done().Returns(inputType);
                if (!typesOnly) {
                    builder.Implementation(new TGenericTag(tagged.GetTag()));
                }
                return true;
            }
            else {
                return false;
            }
        }
    private:
        TStringRef Tag_;
    };


    SIMPLE_MODULE(TTaggedModule, TExample, TGenericTag)
}

REGISTER_MODULES(TTaggedModule)

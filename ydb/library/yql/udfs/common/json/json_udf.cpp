#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/json/easy_parse/json_easy_parser.h>

using namespace NKikimr;
using namespace NUdf;

namespace {
    class TGetField: public TBoxedValue {
    public:
        typedef bool TTypeAwareMarker;

    public:
        static TStringRef Name() {
            return TStringRef::Of("GetField");
        }

        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override {
            if (!args[0]) {
                return valueBuilder->NewEmptyList();
            }

            const TString json(args[0].AsStringRef());
            const TString field(args[1].AsStringRef());

            if (field.empty()) {
                return valueBuilder->NewEmptyList();
            }

            NJson::TJsonParser parser;
            parser.AddField(field, false);

            TVector<TString> result;
            parser.Parse(json, &result);

            TUnboxedValue* items = nullptr;
            const auto list = valueBuilder->NewArray(result.size(), items);
            for (const TString& item : result) {
                *items++ = valueBuilder->NewString(item);
            }

            return list;
        }

        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
            if (Name() == name) {
                bool useString = true;
                bool isOptional = true;
                if (userType) {
                    // support of an overload with Json/Json? input type
                    auto typeHelper = builder.TypeInfoHelper();
                    auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
                    if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                        builder.SetError("Missing or invalid user type.");
                        return true;
                    }

                    auto argsTypeTuple = userTypeInspector.GetElementType(0);
                    auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
                    if (!argsTypeInspector) {
                        builder.SetError("Invalid user type - expected tuple.");
                        return true;
                    }

                    if (argsTypeInspector.GetElementsCount() != 2) {
                        builder.SetError("Invalid user type - expected two arguments.");
                        return true;
                    }

                    auto inputType = argsTypeInspector.GetElementType(0);
                    auto optInspector = TOptionalTypeInspector(*typeHelper, inputType);
                    auto dataType = inputType;
                    if (optInspector) {
                        dataType = optInspector.GetItemType();
                    } else {
                        isOptional = false;
                    }

                    auto dataInspector = TDataTypeInspector(*typeHelper, dataType);
                    if (dataInspector && dataInspector.GetTypeId() == TDataType<TJson>::Id) {
                        useString = false;
                        builder.UserType(userType);
                    }
                }

                auto retType = builder.List()->Item<char*>().Build();
                if (useString) {
                    builder.Args()->Add(builder.Optional()->Item<char*>().Build()).Add<char*>().Done().Returns(retType);
                } else {
                    auto type = builder.SimpleType<TJson>();
                    if (isOptional) {
                        builder.Args()->Add(builder.Optional()->Item(type).Build()).Add<char*>().Done().Returns(retType);
                    } else {
                        builder.Args()->Add(type).Add<char*>().Done().Returns(retType);
                    }
                }

                if (!typesOnly) {
                    builder.Implementation(new TGetField);
                }

                builder.IsStrict();
                return true;
            } else {
                return false;
            }
        }
    };
}

SIMPLE_MODULE(TJsonModule,
              TGetField)

REGISTER_MODULES(TJsonModule)

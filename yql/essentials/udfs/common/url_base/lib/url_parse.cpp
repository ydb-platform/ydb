#include "url_parse.h"

#define FIELD_ADD(name) structBuilder->AddField(#name, optionalStringType, &urlParseIndexes.name);
#define FIELD_FILL(name) \
    if (value.FldIsSet(TUri::Field##name)) { \
        fields[UrlParseIndexes.name] = valueBuilder->NewString(value.GetField(TUri::Field##name)); \
    }

namespace NUrlUdf {
    using namespace NUri;
    using namespace NKikimr;
    using namespace NUdf;

    TUnboxedValue TParse::Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const {
        TUri value;
        const auto ParseError = value.ParseAbs(args[0].AsStringRef(), ParseFlags);
        TUnboxedValue* fields = nullptr;
        const auto result = valueBuilder->NewArray(FieldsCount, fields);
        if (ParseError == TUri::ParsedOK) {
            FIELD_MAP(FIELD_FILL)
        } else {
            fields[UrlParseIndexes.ParseError] = valueBuilder->NewString(TStringBuilder() << ParseError);
        }
        return result;
    }

    bool TParse::DeclareSignature(
        const TStringRef& name,
        TType* userType,
        IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() == name) {
            TUrlParseIndexes urlParseIndexes;

            builder.Args(1)->Add<TAutoMap<char*>>();
            const auto optionalStringType = builder.Optional()->Item<char*>().Build();
            const auto structBuilder = builder.Struct(FieldsCount);
            structBuilder->AddField("ParseError", optionalStringType, &urlParseIndexes.ParseError);
            FIELD_MAP(FIELD_ADD)
            builder.Returns(structBuilder->Build());

            if (!typesOnly) {
                builder.Implementation(new TParse(urlParseIndexes));
            }
            return true;
        } else {
            return false;
        }
    }
}

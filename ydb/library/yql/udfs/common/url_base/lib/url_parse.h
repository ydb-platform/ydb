#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <library/cpp/uri/uri.h>

#define FIELD_MAP(XX) \
    XX(Scheme)        \
    XX(User)          \
    XX(Pass)          \
    XX(Host)          \
    XX(Port)          \
    XX(Path)          \
    XX(Query)         \
    XX(Frag)

#define FIELD_INDEXES(name) ui32 name;

namespace NUrlUdf {
    using namespace NUri;
    using namespace NKikimr;
    using namespace NUdf;

    struct TUrlParseIndexes {
        ui32 ParseError;
        FIELD_MAP(FIELD_INDEXES)
    };

    class TParse: public TBoxedValue {
    public:
        TParse(const TUrlParseIndexes& UrlParseIndexes)
            : UrlParseIndexes(UrlParseIndexes)
            , ParseFlags(TUri::FeaturesRecommended)
        {
        }

        static const TStringRef& Name() {
            static auto nameRef = TStringRef("Parse");
            return nameRef;
        }

    private:
        TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override;

    public:
        static bool DeclareSignature(
            const TStringRef& name,
            TType* userType,
            IFunctionTypeInfoBuilder& builder,
            bool typesOnly);

    private:
        const TUrlParseIndexes UrlParseIndexes;
        const NUri::TParseFlags ParseFlags;

        static constexpr ui32 FieldsCount = sizeof(TUrlParseIndexes) / sizeof(ui32);
    };
}

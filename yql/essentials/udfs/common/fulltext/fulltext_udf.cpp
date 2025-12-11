#include <yql/essentials/public/udf/udf_helpers.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

class TFulltextContains: public TBoxedValue {
public:
    explicit TFulltextContains(const TSourcePosition& pos)
        : Pos_(pos)
    {
    }

    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = ::NYql::NUdf::TStringRef::Of("FulltextContains");
        return name;
    }

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        Y_UNUSED(valueBuilder);
        try {
            if (!args[0] || !args[1]) {
                return TUnboxedValuePod(false);
            }

            TStringBuf text = args[0].AsStringRef();
            TStringBuf query = args[1].AsStringRef();

            if (query.empty()) {
                return TUnboxedValuePod(true);
            }

            if (text.empty()) {
                return TUnboxedValuePod(false);
            }

            // Case-insensitive substring search for fulltext matching
            // Convert both strings to lowercase for Unicode-aware case-insensitive comparison
            TString textLower = to_lower(TString(text));
            TString queryLower = to_lower(TString(query));

            // Simple substring search after case normalization
            return TUnboxedValuePod(textLower.Contains(queryLower));
        } catch (const std::exception& e) {
            TStringBuilder sb;
            sb << Pos_ << " " << e.what();
            UdfTerminate(sb.c_str());
        }
    }

    static bool DeclareSignature(const TStringRef& name, TType* userType, IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        Y_UNUSED(userType);
        if (Name() != name) {
            return false;
        }

        builder.Args()
            ->Add(builder.Optional()->Item<const char*>().Build())
            .Add<const char*>()
            .Done()
            .Returns(builder.SimpleType<bool>());

        if (!typesOnly) {
            builder.Implementation(new TFulltextContains(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    const TSourcePosition Pos_;
};

SIMPLE_MODULE(TFullTextModule,
              TFulltextContains)

} // namespace

REGISTER_MODULES(TFullTextModule)

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
        if (!args[0] || !args[1]) {
            TString errorMessage = TStringBuilder() << Pos_ << " FulltextContains: arguments are null";
            UdfTerminate(errorMessage.c_str());
            return TUnboxedValuePod();
        }

        TStringBuf text = args[0].AsStringRef();
        TStringBuf query = args[1].AsStringRef();

        TStringBuilder sb;
        sb << Pos_ << " Unsupported full text index access. "
            << "Query has been failed to be rewritten to a fulltext index scan, "
            << "full text query: " << query << ", "
            << "text: " << text << ".";
        UdfTerminate(sb.c_str());
        return TUnboxedValuePod();
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

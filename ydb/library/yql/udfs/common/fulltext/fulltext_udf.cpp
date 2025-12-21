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
        static auto name = ::NYql::NUdf::TStringRef::Of("Contains");
        return name;
    }

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        Y_UNUSED(valueBuilder);
        if (!args[0] || !args[1]) {
            TString errorMessage = TStringBuilder() << Pos_ << " Contains: arguments are null";
            UdfTerminate(errorMessage.c_str());
            return TUnboxedValuePod();
        }

        TStringBuilder sb;
        sb << Pos_ << " Unsupported full text index access. "
            << "Query has been failed to be rewritten to a fulltext index scan.";
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

class TFulltextRelevance: public TBoxedValue {
public:
    explicit TFulltextRelevance(const TSourcePosition& pos)
        : Pos_(pos)
    {
    }

    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = ::NYql::NUdf::TStringRef::Of("Relevance");
        return name;
    }

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        Y_UNUSED(valueBuilder);
        if (!args[0] || !args[1]) {
            TString errorMessage = TStringBuilder() << Pos_ << " Relevance: arguments are null";
            UdfTerminate(errorMessage.c_str());
            return TUnboxedValuePod();
        }

        TStringBuilder sb;
        sb << Pos_ << " Unsupported full text index access. "
            << "Query has been failed to be rewritten to a fulltext index scan.";
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
            .Returns(builder.SimpleType<double>());

        if (!typesOnly) {
            builder.Implementation(new TFulltextRelevance(GetSourcePosition(builder)));
        }
        return true;
    }

private:
    const TSourcePosition Pos_;
};


SIMPLE_MODULE(TFullTextModule,
              TFulltextContains,
              TFulltextRelevance)

} // namespace

REGISTER_MODULES(TFullTextModule)

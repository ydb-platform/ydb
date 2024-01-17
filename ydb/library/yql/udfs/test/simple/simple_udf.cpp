#include <util/generic/bt_exception.h>
#include <util/generic/hash.h>
#include <util/string/cast.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

SIMPLE_UDF(TCrash, ui64(char*)) {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    int *ptr = nullptr;
    *ptr = 1;
    return TUnboxedValuePod(0);
}

void Throws(const TString& msg) {
    ythrow TWithBackTrace<yexception>() << msg;
}

SIMPLE_UDF(TException, ui64(char*)) {
    Y_UNUSED(valueBuilder);
    TString msg(args[0].AsStringRef());
    Throws(msg);
    return TUnboxedValuePod(0);
}

SIMPLE_UDF(TReturnNull, char*(char*)) {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    return TUnboxedValuePod(NULL);
}

SIMPLE_UDF(TReturnVoid, void(char*)) {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    return TUnboxedValuePod::Void();
}

SIMPLE_UDF(TReturnEmpty, TOptional<char*>(char*)) {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    return TUnboxedValuePod();
}

SIMPLE_UDF(TReturnBrokenInt, ui32()) {
    Y_UNUSED(args);
    return valueBuilder->NewString("Bunny");
}

SIMPLE_UDF(TEcho, char*(TOptional<char*>)) {
    if (args[0]) {
        return valueBuilder->NewString(args[0].AsStringRef());
    } else {
        return valueBuilder->NewString("<empty optional>");
    }
}

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TEchoWithPrefix, char*(char*,TOptional<char*>), 1) {
    if (!args[1])
        return TUnboxedValuePod(args[0]);
    return valueBuilder->ConcatStrings(args[1], args[0]);
}

SIMPLE_UDF_RUN(TEchoWithRunPrefix, char*(char*), TOptional<char*>) {
    if (!RunConfig)
        return TUnboxedValuePod(args[0]);

    return valueBuilder->PrependString(RunConfig.AsStringRef(), args[0]);
}

SIMPLE_UDF(TConst, char*()) {
    Y_UNUSED(args);
    return valueBuilder->NewString(TStringBuf("Constant response"));
}

SIMPLE_UDF(TConcat, char*(char*, char*)) {
    return valueBuilder->ConcatStrings(args[0], args[1]);
}

SIMPLE_UDF(TRepeat, char*(char*, ui64)) {
    TString orig(args[0].AsStringRef());
    ui64 times = args[1].Get<ui64>();
    TString res = "";
    for (ui64 i = 0; i < times; i++) {
        res += orig;
    }
    return valueBuilder->NewString(res);
}

SIMPLE_UDF(TSleep, ui64(ui64)) {
    Y_UNUSED(valueBuilder);
    ui64 time = args[0].Get<ui64>();
    usleep(time);
    return TUnboxedValuePod(static_cast<ui64>(0));
}

using TComplexReturnTypeSignature = TDict<char*, ui32>(char*);
SIMPLE_UDF(TComplexReturnType, TComplexReturnTypeSignature) {
    const TStringBuf s = args[0].AsStringRef();
    THashMap<TString, ui32> stat;
    for(auto c: s) {
       ++stat[TString{c}];
    }
    auto dictBuilder = valueBuilder->NewDict(ReturnType_, 0);
    for(const auto& [k, v]: stat) {
        dictBuilder->Add(valueBuilder->NewString(k), TUnboxedValuePod{v});
    }
    return dictBuilder->Build();
}

extern const char c[] = "C";
extern const char d[] = "D";

using TNamedC = TNamedArg<ui32, c>;
using TNamedD = TNamedArg<ui32, d>;

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TNamedArgs, char*(ui32, TOptional<ui32>, TNamedC, TNamedD), 3) {
    TString res;
    res += "A=" + ToString(args[0].Get<ui32>());
    res += " B=" + (args[1] ? ToString(args[1].Get<ui32>()) : "none");
    res += " C=" + (args[2] ? ToString(args[2].Get<ui32>()) : "none");
    res += " D=" + (args[3] ? ToString(args[3].Get<ui32>()) : "none");
    return valueBuilder->NewString(res);
}

UDF(TIncrement, builder.Args(2)->
    Add<ui32>().Name("Arg1").Flags(ICallablePayload::TArgumentFlags::AutoMap)
    .Add(builder.SimpleType<TOptional<ui32>>()).Name("Arg2")
    .Done().Returns<ui32>().OptionalArgs(1);) {
    Y_UNUSED(valueBuilder);
    return TUnboxedValuePod(args[0].Get<ui32>() + args[1].GetOrDefault<ui32>(1));
}

UDF(TIncrementOpt, builder.Args(2)->
    Add<ui32>().Name("Arg1").Flags(ICallablePayload::TArgumentFlags::AutoMap)
    .Add(builder.SimpleType<TOptional<ui32>>()).Name("Arg2")
    .Done().Returns(builder.SimpleType<TOptional<ui32>>()).OptionalArgs(1);) {
    Y_UNUSED(valueBuilder);
    if (const ui32 by = args[1].GetOrDefault<ui32>(0)) {
        return TUnboxedValuePod(args[0].Get<ui32>() + by);
    }
    else {
        return TUnboxedValuePod();
    }
}

UDF_IMPL(TIncrementWithCounters,
    builder.Args(1)->Add<ui32>().Done().Returns<ui32>();
    ,
    mutable ::NKikimr::NUdf::TCounter Counter_;
    mutable ::NKikimr::NUdf::TScopedProbe Scope_;
    ,
    Counter_ = builder.GetCounter("IncrementWithCounters_Calls", true);
    Scope_ = builder.GetScopedProbe("IncrementWithCounters_Time");
    ,
    ""
    ,
    ""
    ,
    void
) {
    Y_UNUSED(valueBuilder);
    Counter_.Inc();
    with_lock(Scope_) {
        return TUnboxedValuePod(args[0].Get<ui32>() + 1);
    }
}

class TGenericAsStruct : public TBoxedValue {
public:
    typedef bool TTypeAwareMarker;

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        TUnboxedValue* items = nullptr;
        auto result = valueBuilder->NewArray(Argc, items);
        for (size_t i = 0; i < Argc; ++i) {
            items[i] = std::move(args[i]);
        }
        return result;
    }

    static const TStringRef& Name() {
        static auto name = TStringRef::Of("GenericAsStruct");
        return name;
    }

    TGenericAsStruct(size_t argc)
        : Argc(argc)
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

            const size_t argsCount = argsTypeInspector.GetElementsCount();
            auto argBuilder = builder.Args(argsCount);
            auto structBuilder = builder.Struct(argsCount);
            for (size_t i = 0; i < argsCount; ++i) {
                auto argType = argsTypeInspector.GetElementType(i);
                argBuilder->Add(argType);
                TString name = TStringBuilder() << "arg_" << i;
                structBuilder->AddField(name, argType, nullptr);
            }

            argBuilder->Done().Returns(builder.Optional()->Item(structBuilder->Build()).Build());

            if (!typesOnly) {
                builder.Implementation(new TGenericAsStruct(argsCount));
            }
            return true;
        }
        else {
            return false;
        }
    }
private:
    const size_t Argc;
};


SIMPLE_MODULE(TSimpleUdfModule,
                TCrash,
                TException,
                TReturnNull,
                TReturnVoid,
                TReturnEmpty,
                TReturnBrokenInt,
                TEcho,
                TEchoWithPrefix,
                TEchoWithRunPrefix,
                TConst,
                TConcat,
                TRepeat,
                TSleep,
                TComplexReturnType,
                TNamedArgs,
                TIncrement,
                TIncrementOpt,
                TIncrementWithCounters,
                TGenericAsStruct
              )

} // namespace

REGISTER_MODULES(TSimpleUdfModule)

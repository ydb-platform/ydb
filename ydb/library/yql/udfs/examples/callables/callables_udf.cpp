#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>


using namespace NKikimr;
using namespace NUdf;

namespace {

//////////////////////////////////////////////////////////////////////////////
// TFromString
//////////////////////////////////////////////////////////////////////////////
class TFromString: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("FromString");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        Y_UNUSED(valueBuilder);
        auto str = args[0].AsStringRef();
        int val = FromString<int>(str);
        return TUnboxedValuePod(val);
    }
};

//////////////////////////////////////////////////////////////////////////////
// TSum
//////////////////////////////////////////////////////////////////////////////
class TSum: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Sum");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        int sum = 0;

        auto it = args[0].GetListIterator();
        for (TUnboxedValue arg; it.Next(arg);) {
            auto value = args[1].Run(valueBuilder, &arg);
            sum += value.Get<int>();
        }

        return TUnboxedValuePod(sum);
    }
};

//////////////////////////////////////////////////////////////////////////////
// TMul
//////////////////////////////////////////////////////////////////////////////
class TMul: public TBoxedValue
{
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("Mul");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        int mul = 1;

        const auto it = args[0].GetListIterator();
        for (TUnboxedValue arg; it.Next(arg);) {
            auto value = args[1].Run(valueBuilder, &arg);
            mul *= value.Get<int>();
        }

        return TUnboxedValuePod(mul);
    }
};

extern const char A[] = "a";
using TNamedA = TNamedArg<i32, A>;

//////////////////////////////////////////////////////////////////////////////
// TNamedArgUdf
//////////////////////////////////////////////////////////////////////////////

class TNamedArgUdf: public TBoxedValue {
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("NamedArgUdf");
        return name;
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        Y_UNUSED(valueBuilder);
        auto res = args[0] ? args[0].Get<i32>() : 123;
        return TUnboxedValuePod(res + 1);
    }
};

//////////////////////////////////////////////////////////////////////////////
// TReturnNamedArgCallable
//////////////////////////////////////////////////////////////////////////////

class TReturnNamedArgCallable: public TBoxedValue {
public:
    static TStringRef Name() {
        static auto name = TStringRef::Of("ReturnNamedArgCallable");
        return name;
    }

    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(args);
        return TUnboxedValuePod(new TNamedArgUdf());
    }
};

//////////////////////////////////////////////////////////////////////////////
// TCallablesModule
//////////////////////////////////////////////////////////////////////////////
class TCallablesModule: public IUdfModule
{
public:
    TStringRef Name() const {
        return TStringRef::Of("Callables");
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(TFromString::Name());
        sink.Add(TSum::Name());
        sink.Add(TMul::Name());
    }

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final
    {
        try {
            Y_UNUSED(userType);
            Y_UNUSED(typeConfig);

            bool typesOnly = (flags & TFlags::TypesOnly);

            if (TFromString::Name() == name) {
                // function signature:
                //      int (String)
                // run config: void
                builder.SimpleSignature<int(char*)>();

                if (!typesOnly) {
                    builder.Implementation(new TFromString);
                }
            }
            else if (TSum::Name() == name) {
                // function signature:
                //      int (ListOf(String), int(*)(String))
                // run config: void
                builder.Returns<int>().Args()->
                        Add(builder.List()->Item<char*>())
                        .Add(builder.Callable()->Returns<int>().Arg<char*>())
                        .Done();

                if (!typesOnly) {
                    builder.Implementation(new TSum);
                }
            }
            else if (TMul::Name() == name) {
                // function signature:
                //      int (ListOf(String), int(*)(String))
                // run config: void
                using TFuncType = int(*)(char*);
                builder.SimpleSignature<int(TListType<char*>, TFuncType)>();

                if (!typesOnly) {
                    builder.Implementation(new TMul);
                }
            } else if (TNamedArgUdf::Name() == name) {
                builder.SimpleSignature<int(TNamedA)>();

                if (!typesOnly) {
                    builder.Implementation(new TNamedArgUdf());
                }
            } else if (TReturnNamedArgCallable::Name() == name) {
                builder.Returns(builder.Callable()->Returns<int>().Arg<int>().Name(A));

                if (!typesOnly) {
                    builder.Implementation(new TReturnNamedArgCallable());
                }
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TCallablesModule)

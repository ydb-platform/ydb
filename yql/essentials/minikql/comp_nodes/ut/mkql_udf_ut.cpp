#include "mkql_computation_node_ut.h"
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/datetime/datetime64.h>

namespace NKikimr {
namespace NMiniKQL {

// XXX: Emulate type transformations similar to the one made by
// type annotation and compilation phases. As a result, the name
// (i.e. "UDF") of callable type is lost. Hence, the type resolved
// at the type annotation phase (and, ergo, used for bytecode
// compilation) differs from the type, resolved for the underline
// function at runtime.
template<typename TUdf>
static TType* TweakUdfType(const NYql::NUdf::TStringRef& name, TType* userType,
                           const TTypeEnvironment& env)
{
    TFunctionTypeInfoBuilder typeInfoBuilder(env, new TTypeInfoHelper(),
                                             "", nullptr, {});

    // Obtain the callable type of the particular UDF.
    TFunctionTypeInfo funcInfo;
    UNIT_ASSERT(TUdf::DeclareSignature(name, userType, typeInfoBuilder, true));
    typeInfoBuilder.Build(&funcInfo);

    // Create the new MiniKQL type to emulate two conversions:
    // * Convert the given MiniKQL type to the expression type.
    //   See <NYql::NCommon::ConvertMiniKQLType>.
    // * Convert the expression type back to the MiniKQL one.
    //   See <NYql::NCommon::TMkqlBuildContext::BuildType>.
    // The aforementioned conversions are made by the pipeline in
    // scope of type annotation and compilation phases.
    // As a result of the first conversion, the name of the
    // callable type is lost, so the new MiniKQL type has to be
    // the same as the resolved one, but the name is omitted.
    const auto funcType = AS_TYPE(TCallableType, funcInfo.FunctionType);
    TVector<TType*> argsTypes;
    for (size_t i = 0; i < funcType->GetArgumentsCount(); i++) {
        argsTypes.push_back(funcType->GetArgumentType(i));
    }
    const auto nodeType = TCallableType::Create("", /* Name has to be empty. */
                                                funcType->GetReturnType(),
                                                funcType->GetArgumentsCount(),
                                                argsTypes.data(),
                                                funcType->GetPayload(),
                                                env);
    nodeType->SetOptionalArgumentsCount(funcType->GetOptionalArgumentsCount());
    return nodeType;
};

class TImpl : public NYql::NUdf::TBoxedValue {
public:
    explicit TImpl(NYql::NUdf::TSourcePosition pos,
                   const std::string_view upvalue)
        : Pos_(pos)
        , Upvalue_(upvalue)
    {}

    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder* valueBuilder,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const override try {
        TStringStream concat;
        concat << Upvalue_ << " " << args[0].AsStringRef();
        return valueBuilder->NewString(NYql::NUdf::TStringRef(concat.Data(),
                                                              concat.Size()));
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
    }


private:
    const NYql::NUdf::TSourcePosition Pos_;
    const TString Upvalue_;
};

// Class, implementing the closure with run config.
class TRunConfig : public NYql::NUdf::TBoxedValue {
public:
    explicit TRunConfig(NYql::NUdf::TSourcePosition pos)
        : Pos_(pos)
    {}

    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef::Of("Test");
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name,
                                 NYql::NUdf::TType*,
                                 NYql::NUdf::IFunctionTypeInfoBuilder& builder,
                                 bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        builder.RunConfig<char*>().Args(1)->Add<char*>();
        builder.Returns<char*>();
        if (!typesOnly) {
            builder.Implementation(new TRunConfig(builder.GetSourcePosition()));
        }

        return true;
    }

    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder*,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const final try {
        const std::string_view upvalue(args[0].AsStringRef());
        return NYql::NUdf::TUnboxedValuePod(new TImpl(Pos_, upvalue));
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
    }

private:
    const NYql::NUdf::TSourcePosition Pos_;
};

// Class, implementing the closure with currying.
class TCurrying : public NYql::NUdf::TBoxedValue {
public:
    explicit TCurrying(NYql::NUdf::TSourcePosition pos)
        : Pos_(pos)
    {}

    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef::Of("Test");
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name,
                                 NYql::NUdf::TType*,
                                 NYql::NUdf::IFunctionTypeInfoBuilder& builder,
                                 bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        builder.OptionalArgs(1).Args(2)->Add<char*>()
            .Add<NYql::NUdf::TOptional<bool>>().Name("NewOptionalArg");
        builder.Returns(builder.SimpleSignatureType<char*(char*)>());
        if (!typesOnly) {
            builder.Implementation(new TCurrying(builder.GetSourcePosition()));
        }

        return true;
    }

    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder*,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const final try {
        const std::string_view upvalue(args[0].AsStringRef());
        UNIT_ASSERT(!args[1]);
        return NYql::NUdf::TUnboxedValuePod(new TImpl(Pos_, upvalue));
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
    }

private:
    const NYql::NUdf::TSourcePosition Pos_;
};


// XXX: To properly test the issue described in YQL-19967, there
// should be two modules, registered by the same name, which
// provide the functions with different signatures, exported by
// the same name. Hence, class names for UDFs are different, but
// use the same name in MKQL bytecode.
SIMPLE_MODULE(TRunConfigUTModule, TRunConfig)
SIMPLE_MODULE(TCurryingUTModule, TCurrying)


SIMPLE_STRICT_UDF(TTest, char*(char*, char*, char*)) {
    TStringStream concat;
    concat << args[0].AsStringRef() << " "
           << args[1].AsStringRef() << " "
           << args[2].AsStringRef();
    return valueBuilder->NewString(NYql::NUdf::TStringRef(concat.Data(),
                                                          concat.Size()));
}

template<bool Old>
class TNewTest : public NYql::NUdf::TBoxedValue {
public:
    explicit TNewTest(NYql::NUdf::TSourcePosition pos)
        : Pos_(pos)
    {}

    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef::Of("Test");
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name,
                                 NYql::NUdf::TType*,
                                 NYql::NUdf::IFunctionTypeInfoBuilder& builder,
                                 bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (Old && typesOnly) {
            builder.SimpleSignature<char*(char*, char*, char*)>();
            return true;
        }

        builder.SimpleSignature<char*(char*, char*, char*, NYql::NUdf::TOptional<char*>)>()
            .OptionalArgs(1);
        if (!typesOnly) {
            builder.Implementation(new TNewTest(builder.GetSourcePosition()));
        }

        return true;
    }

    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder* valueBuilder,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const override try {
        TStringStream concat;
        concat << args[0].AsStringRef() << " "
               << args[1].AsStringRef() << " ";
        if (args[3]) {
            concat << args[3].AsStringRef() << " ";
        }
        concat << args[2].AsStringRef();
        return valueBuilder->NewString(NYql::NUdf::TStringRef(concat.Data(),
                                                              concat.Size()));
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
    }

private:
    const NYql::NUdf::TSourcePosition Pos_;
};

// XXX: "Old" UDF is declared via SIMPLE_UDF helper, so it has to
// use the *actual* function name as a class name. Furthermore,
// the UDF, declared by SIMPLE_UDF has to provide the same
// semantics as TNewTest<true>.
SIMPLE_MODULE(TOldUTModule, TTest)
SIMPLE_MODULE(TIncrementalUTModule, TNewTest<true>)
SIMPLE_MODULE(TNewUTModule, TNewTest<false>)

Y_UNIT_TEST_SUITE(TMiniKQLUdfTest) {
    Y_UNIT_TEST_LLVM(RunconfigToCurrying) {
        // Create the test setup, using TRunConfig implementation
        // for TestModule.Test UDF.
        TVector<TUdfModuleInfo> compileModules;
        compileModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TRunConfigUTModule()}
        );
        TSetup<LLVM> compileSetup(GetTestFactory(), std::move(compileModules));
        TProgramBuilder& pb = *compileSetup.PgmBuilder;

        // Build the graph on the setup with TRunConfig implementation.
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto upvalue = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary");
        const auto value = pb.NewDataLiteral<NUdf::EDataSlot::String>("is alive");
        const auto userType = pb.NewTupleType({
            pb.NewTupleType({strType}),
            pb.NewEmptyStructType(),
            pb.NewEmptyTupleType()});

        const auto udfType = TweakUdfType<TRunConfig>("Test", userType, *compileSetup.Env);
        const auto udf = pb.TypedUdf("TestModule.Test", udfType, upvalue, userType);

        const auto list = pb.NewList(strType, {value});
        const auto pgmReturn = pb.Map(list, [&pb, udf](const TRuntimeNode item) {
            return pb.Apply(udf, {item});
        });

        // Create the test setup, using TCurrying implementation
        // for TestModule.Test UDF.
        TVector<TUdfModuleInfo> runModules;
        runModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TCurryingUTModule()}
        );
        TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));

        // Move the graph from the one setup to another as a
        // serialized bytecode sequence.
        const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
        const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

        // Run the graph on the setup with TCurrying implementation.
        const auto graph = runSetup.BuildGraph(root);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT(iterator.Next(result));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is alive");
        UNIT_ASSERT(!iterator.Next(result));
    }

    Y_UNIT_TEST_LLVM(CurryingToRunconfig) {
        // Create the test setup, using TCurrying implementation
        // for TestModule.Test UDF.
        TVector<TUdfModuleInfo> compileModules;
        compileModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TCurryingUTModule()}
        );
        TSetup<LLVM> compileSetup(GetTestFactory(), std::move(compileModules));
        TProgramBuilder& pb = *compileSetup.PgmBuilder;

        // Build the graph on the setup with TRunConfig implementation.
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto upvalue = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary");
        const auto optional = pb.NewOptional(pb.NewDataLiteral(true));
        const auto value = pb.NewDataLiteral<NUdf::EDataSlot::String>("is alive");
        const auto userType = pb.NewTupleType({
            pb.NewTupleType({strType}),
            pb.NewEmptyStructType(),
            pb.NewEmptyTupleType()});

        const auto udfType = TweakUdfType<TCurrying>("Test", userType, *compileSetup.Env);
        const auto udf = pb.TypedUdf("TestModule.Test", udfType, pb.NewVoid(), userType);
        const auto closure = pb.Apply(udf, {upvalue, optional});

        const auto list = pb.NewList(strType, {value});
        const auto pgmReturn = pb.Map(list, [&pb, closure](const TRuntimeNode item) {
            return pb.Apply(closure, {item});
        });

        // Create the test setup, using TRunConfig implementation
        // for TestModule.Test UDF.
        TVector<TUdfModuleInfo> runModules;
        runModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TRunConfigUTModule()}
        );
        TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));
        // Move the graph from the one setup to another as a
        // serialized bytecode sequence.
        const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
        const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

        // Run the graph on the setup with TCurrying implementation.
        const auto graph = runSetup.BuildGraph(root);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT(iterator.Next(result));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is alive");
        UNIT_ASSERT(!iterator.Next(result));
    }

    Y_UNIT_TEST_LLVM(OldToIncremental) {
        // Create the test setup, using the old implementation for
        // TestModule.Test UDF.
        TVector<TUdfModuleInfo> compileModules;
        compileModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TOldUTModule()}
        );
        TSetup<LLVM> compileSetup(GetTestFactory(), std::move(compileModules));
        TProgramBuilder& pb = *compileSetup.PgmBuilder;

        // Build the graph, using the old setup.
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto arg1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary");
        const auto arg2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("is");
        const auto arg3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("alive");

        const auto udf = pb.Udf("TestModule.Test");
        const auto argsType = pb.NewTupleType({strType, strType, strType});
        const auto argList = pb.NewList(argsType, {pb.NewTuple({arg1, arg2, arg3})});
        const auto pgmReturn = pb.Map(argList, [&pb, udf](const TRuntimeNode args) {
            return pb.Apply(udf, {pb.Nth(args, 0), pb.Nth(args, 1), pb.Nth(args, 2)});
        });

        // Create the test setup, using the incremental
        // implementation for TestModule.Test UDF.
        TVector<TUdfModuleInfo> runModules;
        runModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TIncrementalUTModule()}
        );
        TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));
        // Move the graph from the one setup to another as a
        // serialized bytecode sequence.
        const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
        const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

        // Run the graph, using the incremental setup.
        const auto graph = runSetup.BuildGraph(root);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT(iterator.Next(result));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is alive");
        UNIT_ASSERT(!iterator.Next(result));
    }

    Y_UNIT_TEST_LLVM(IncrementalToOld) {
        // Create the test setup, using the incremental
        // implementation for TestModule.Test UDF.
        TVector<TUdfModuleInfo> compileModules;
        compileModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TIncrementalUTModule()}
        );
        TSetup<LLVM> compileSetup(GetTestFactory(), std::move(compileModules));
        TProgramBuilder& pb = *compileSetup.PgmBuilder;

        // Build the graph, using the incremental setup.
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto arg1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary");
        const auto arg2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("is");
        const auto arg3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("alive");

        const auto udf = pb.Udf("TestModule.Test");
        const auto argsType = pb.NewTupleType({strType, strType, strType});
        const auto argList = pb.NewList(argsType, {pb.NewTuple({arg1, arg2, arg3})});
        const auto pgmReturn = pb.Map(argList, [&pb, udf](const TRuntimeNode args) {
            return pb.Apply(udf, {pb.Nth(args, 0), pb.Nth(args, 1), pb.Nth(args, 2)});
        });

        // Create the test setup, using the old implementation for
        // TestModule.Test UDF.
        TVector<TUdfModuleInfo> runModules;
        runModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TOldUTModule()}
        );
        TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));
        // Move the graph from the one setup to another as a
        // serialized bytecode sequence.
        const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
        const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

        // Run the graph, using the old setup.
        const auto graph = runSetup.BuildGraph(root);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT(iterator.Next(result));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is alive");
        UNIT_ASSERT(!iterator.Next(result));
    }

    Y_UNIT_TEST_LLVM(IncrementalToNew) {
        // Create the test setup, using the incremental
        // implementation for TestModule.Test UDF.
        TVector<TUdfModuleInfo> compileModules;
        compileModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TIncrementalUTModule()}
        );
        TSetup<LLVM> compileSetup(GetTestFactory(), std::move(compileModules));
        TProgramBuilder& pb = *compileSetup.PgmBuilder;

        // Build the graph, using the incremental setup.
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto arg1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary");
        const auto arg2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("is");
        const auto arg3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("alive");

        const auto udf = pb.Udf("TestModule.Test");
        const auto argsType = pb.NewTupleType({strType, strType, strType});
        const auto argList = pb.NewList(argsType, {pb.NewTuple({arg1, arg2, arg3})});
        const auto pgmReturn = pb.Map(argList, [&pb, udf](const TRuntimeNode args) {
            return pb.Apply(udf, {pb.Nth(args, 0), pb.Nth(args, 1), pb.Nth(args, 2)});
        });

        // Create the test setup, using the new implementation for
        // TestModule.Test UDF.
        TVector<TUdfModuleInfo> runModules;
        runModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TNewUTModule()}
        );
        TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));
        // Move the graph from the one setup to another as a
        // serialized bytecode sequence.
        const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
        const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

        // Run the graph, using the new setup.
        const auto graph = runSetup.BuildGraph(root);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT(iterator.Next(result));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is alive");
        UNIT_ASSERT(!iterator.Next(result));
    }

    Y_UNIT_TEST_LLVM(NewToIncremental) {
        // Create the test setup, using the new implementation for
        // TestModule.Test UDF.
        TVector<TUdfModuleInfo> compileModules;
        compileModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TNewUTModule()}
        );
        TSetup<LLVM> compileSetup(GetTestFactory(), std::move(compileModules));
        TProgramBuilder& pb = *compileSetup.PgmBuilder;

        // Build the graph, using the new setup.
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto optType = pb.NewOptionalType(strType);
        const auto arg1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary");
        const auto arg2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("is");
        const auto arg3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("alive");
        const auto arg4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("still");
        const auto opt4 = pb.NewOptional(arg4);

        const auto udf = pb.Udf("TestModule.Test");
        const auto argsType = pb.NewTupleType({strType, strType, strType, optType});
        const auto argList = pb.NewList(argsType, {pb.NewTuple({arg1, arg2, arg3, opt4})});
        const auto pgmReturn = pb.Map(argList, [&pb, udf](const TRuntimeNode args) {
            return pb.Apply(udf, {pb.Nth(args, 0), pb.Nth(args, 1), pb.Nth(args, 2), pb.Nth(args, 3)});
        });

        // Create the test setup, using the incremental
        // implementation for TestModule.Test UDF.
        TVector<TUdfModuleInfo> runModules;
        runModules.emplace_back(
            TUdfModuleInfo{"", "TestModule", new TIncrementalUTModule()}
        );
        TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));
        // Move the graph from the one setup to another as a
        // serialized bytecode sequence.
        const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
        const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

        // Run the graph, using the incremental setup.
        const auto graph = runSetup.BuildGraph(root);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT(iterator.Next(result));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is still alive");
        UNIT_ASSERT(!iterator.Next(result));
    }
} // Y_UNIT_TEST_SUITE

// XXX: Test the hack with on-the-fly argument convertion for the
// call of the Datetime::Format UDF with the basic date resource
// parameter against the underline function, expecting the
// extended date resource as an argument.

namespace {

extern const char TMResourceName[] = "DateTime2.TM";
extern const char TM64ResourceName[] = "DateTime2.TM64";

enum class EBuilds {
    A, // DateTime::Format build with the single parameter: basic
       // DateTime resource representing the timestamp argument.
    B, // DateTime::Format build with two parameters:
       // * the required one with the **basic** resource.
       // * the optional one to tweak the Format behaviour.
    C, // DateTime::Format build with two parameters:
       // * the required one with the **extended** resource.
       // * the optional one to tweak the Format behaviour.
};

template<const char* TResourceName, typename TValue,
         typename TStorage = std::conditional_t<TResourceName == TMResourceName,
                                                NYql::DateTime::TTMStorage,
                                                NYql::DateTime::TTM64Storage>>
TStorage& Reference(TValue& value) {
    return *reinterpret_cast<TStorage*>(value.GetRawPtr());
}

template<const char* TResourceName, typename TValue,
         typename TStorage = std::conditional_t<TResourceName == TMResourceName,
                                                NYql::DateTime::TTMStorage,
                                                NYql::DateTime::TTM64Storage>>
const TStorage& Reference(const TValue& value) {
    return *reinterpret_cast<const TStorage*>(value.GetRawPtr());
}

static TRuntimeNode NewDateTimeNode(const NYql::NUdf::TStringRef& dateLiteral,
                                    const TTypeEnvironment& env)
{
    const auto dtval = ValueFromString(NYql::NUdf::EDataSlot::Datetime, dateLiteral);
    return TRuntimeNode(BuildDataLiteral(dtval, NUdf::TDataType<NYql::NUdf::TDatetime>::Id, env), true);
}

template<enum EBuilds Build, bool LoweredRuntimeVersion>
class TTestDateTime2Format : public NYql::NUdf::TBoxedValue {
static_assert(Build != EBuilds::A || LoweredRuntimeVersion,
              "Build 'A' provides only the 'lowered' runtime version");
public:
    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef::Of("Format");
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name,
                                 NYql::NUdf::TType*,
                                 NYql::NUdf::IFunctionTypeInfoBuilder& builder,
                                 bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        // FIXME: The condition below is required to untie the
        // Gordian knot with the upgrade, when two MiniKQL
        // runtimes with different versions are being used.
        // See YQL-19967 for more info.
        if (LoweredRuntimeVersion && typesOnly) {
            builder.SimpleSignature<char*(NYql::NUdf::TAutoMap<NYql::NUdf::TResource<TMResourceName>>)>();
            builder.RunConfig<char*>();
            return true;
        }

        switch (Build) {
        case EBuilds::A:
            builder.SimpleSignature<char*(NYql::NUdf::TAutoMap<NYql::NUdf::TResource<TMResourceName>>)>();
            builder.RunConfig<char*>();
            break;
        case EBuilds::B:
            builder.OptionalArgs(1).Args()->Add<char*>()
                .Add<NYql::NUdf::TOptional<bool>>().Name("AlwaysWriteFractionalSeconds");
            builder.Returns(
                builder.SimpleSignatureType<char*(NYql::NUdf::TAutoMap<NYql::NUdf::TResource<TMResourceName>>)>());
            break;
        case EBuilds::C:
            builder.OptionalArgs(1).Args()->Add<char*>()
                .Add<NYql::NUdf::TOptional<bool>>().Name("AlwaysWriteFractionalSeconds");
            builder.Returns(
                builder.SimpleSignatureType<char*(NYql::NUdf::TAutoMap<NYql::NUdf::TResource<TM64ResourceName>>)>());
            break;
        }

        if (!typesOnly) {
            builder.Implementation(new TTestDateTime2Format);
        }

        return true;
    }

private:
    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder*,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const override {
        bool alwaysWriteFractionalSeconds = false;

        if constexpr (Build != EBuilds::A) {
            if (auto val = args[1]) {
                alwaysWriteFractionalSeconds = val.Get<bool>();
            }
        }

        return NYql::NUdf::TUnboxedValuePod(new TTestDateTime2Formatter(args[0], alwaysWriteFractionalSeconds));
    }

    class TTestDateTime2Formatter : public NYql::NUdf::TBoxedValue {
    public:
        TTestDateTime2Formatter(NYql::NUdf::TUnboxedValue format, bool alwaysWriteFractionalSeconds)
            : Format_(format)
        {
            UNIT_ASSERT(!alwaysWriteFractionalSeconds);
        }

    private:
        NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder* valueBuilder,
                                      const NYql::NUdf::TUnboxedValuePod* args)
        const override {
            TStringBuilder result;
            result << Format_.AsStringRef() << ": ";
            if constexpr (Build == EBuilds::C) {
                const auto storage = Reference<TM64ResourceName>(args[0]);
                result << storage.Day << "/" << storage.Month << "/"
                       << storage.Year << " " << storage.Hour << ":"
                       << storage.Minute << ":" << storage.Second;
            } else {
                const auto storage = Reference<TMResourceName>(args[0]);
                result << storage.Day << "/" << storage.Month << "/"
                       << storage.Year << " " << storage.Hour << ":"
                       << storage.Minute << ":" << storage.Second;
            }
            result << ".";
            return valueBuilder->NewString(result);
        }

        const NYql::NUdf::TUnboxedValue Format_;
    };
};

template<bool LoweredRuntimeVersion>
class TTestDateTime2Convert : public NYql::NUdf::TBoxedValue {
public:
    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef::Of("Convert");
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name,
                                 NYql::NUdf::TType*,
                                 NYql::NUdf::IFunctionTypeInfoBuilder& builder,
                                 bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        if (LoweredRuntimeVersion && typesOnly) {
            builder.SimpleSignature<NYql::NUdf::TResource<TMResourceName>(NYql::NUdf::TAutoMap<NYql::NUdf::TResource<TMResourceName>>)>()
                   .IsStrict();
            return true;
        }

        builder.SimpleSignature<NYql::NUdf::TResource<TM64ResourceName>(NYql::NUdf::TAutoMap<NYql::NUdf::TResource<TMResourceName>>)>()
               .IsStrict();

        if (!typesOnly) {
            builder.Implementation(new TTestDateTime2Convert);
        }

        return true;
    }

private:
    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder*,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const override {
        NYql::NUdf::TUnboxedValuePod result(0);
        auto& arg = Reference<TMResourceName>(args[0]);
        auto& storage = Reference<TM64ResourceName>(result);
        storage.From(arg);
        return result;
    }
};

class TTestDateTime2Split : public NYql::NUdf::TBoxedValue {
public:
    explicit TTestDateTime2Split(NYql::NUdf::TSourcePosition pos)
        : Pos_(pos)
    {}

    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef::Of("Split");
        return name;
    }

    static bool DeclareSignature(const NYql::NUdf::TStringRef& name,
                                 NYql::NUdf::TType*,
                                 NYql::NUdf::IFunctionTypeInfoBuilder& builder,
                                 bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }
        builder.SimpleSignature<NYql::NUdf::TResource<TMResourceName>(NYql::NUdf::TAutoMap<NYql::NUdf::TDatetime>)>()
               .IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TTestDateTime2Split(builder.GetSourcePosition()));
        }

        return true;
    }

private:
    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder* valueBuilder,
                                  const NYql::NUdf::TUnboxedValuePod* args)
    const override try {
        EMPTY_RESULT_ON_EMPTY_ARG(0);

        auto& builder = valueBuilder->GetDateBuilder();
        NYql::NUdf::TUnboxedValuePod result(0);
        auto& storage = Reference<TMResourceName>(result);
        storage.FromDatetime(builder, args[0].Get<ui32>());
        return result;
    } catch (const std::exception& e) {
        UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
    }

    const NYql::NUdf::TSourcePosition Pos_;
};

// Here are the short description for the versions:
// * A is an old version with DateTime::Format function using
//   runconfig signature.
// * B is a new version with DateTime::Format function using
//   currying signature with basic DateTime resource type for
//   closure argument.
// * C is a new version with DateTime::Format function using
//   currying signature with extended DateTime resource type
//   for closure argument.
SIMPLE_MODULE(TTestADateTime2Module, TTestDateTime2Format<EBuilds::A, true>,
                                     TTestDateTime2Split)
SIMPLE_MODULE(TTestB1DateTime2Module, TTestDateTime2Format<EBuilds::B, true>,
                                      TTestDateTime2Convert<true>,
                                      TTestDateTime2Split)
SIMPLE_MODULE(TTestB2DateTime2Module, TTestDateTime2Format<EBuilds::B, false>,
                                      TTestDateTime2Convert<false>,
                                      TTestDateTime2Split)
SIMPLE_MODULE(TTestC1DateTime2Module, TTestDateTime2Format<EBuilds::C, true>,
                                      TTestDateTime2Convert<true>,
                                      TTestDateTime2Split)
SIMPLE_MODULE(TTestC2DateTime2Module, TTestDateTime2Format<EBuilds::C, false>,
                                      TTestDateTime2Convert<false>,
                                      TTestDateTime2Split)

template<bool LLVM, class TCompileModule, class TRunModule>
static void TestDateTimeFormat() {
    // Create the test setup, using compileModule implementation
    // for DateTime2 UDF.
    TVector<TUdfModuleInfo> modules;
    modules.emplace_back(TUdfModuleInfo{"", "DateTime2", new TCompileModule()});
    TSetup<LLVM> compileSetup(GetTestFactory(), std::move(modules));
    TProgramBuilder& pb = *compileSetup.PgmBuilder;

    // Build the graph, using the compileModule setup.
    const auto dttype = pb.NewDataType(NUdf::EDataSlot::Datetime);
    const auto dtnode = NewDateTimeNode("2009-09-01T15:37:19Z", *compileSetup.Env);
    const auto format = pb.NewDataLiteral<NUdf::EDataSlot::String>("Canary is alive");

    // Build the runtime node for formatter (i.e. DateTime2.Format
    // resulting closure), considering its declared signature.
    TRuntimeNode formatter;
    if constexpr (std::is_same_v<TCompileModule, TTestADateTime2Module> ||
                  std::is_same_v<TCompileModule, TTestB1DateTime2Module> ||
                  std::is_same_v<TCompileModule, TTestC1DateTime2Module>)
    {
        formatter = pb.Udf("DateTime2.Format", format);
    } else {
        formatter = pb.Apply(pb.Udf("DateTime2.Format"), {format});
    }

    const auto list = pb.NewList(dttype, {dtnode});
    const auto pgmReturn = pb.Map(list, [&pb, formatter](const TRuntimeNode item) {
        auto resource = pb.Apply(pb.Udf("DateTime2.Split"), {item});
        if constexpr (std::is_same_v<TCompileModule, TTestC2DateTime2Module>) {
            resource = pb.Apply(pb.Udf("DateTime2.Convert"), {resource});
        }
        return pb.Apply(formatter, {resource});
    });

    // Create the test setup, using runModule implementation for
    // DateTime2 UDF.
    TVector<TUdfModuleInfo> runModules;
    runModules.emplace_back(TUdfModuleInfo{"", "DateTime2", new TRunModule()});
    TSetup<LLVM> runSetup(GetTestFactory(), std::move(runModules));

    // Move the graph from the one setup to another as a
    // serialized bytecode sequence.
    const auto bytecode = SerializeRuntimeNode(pgmReturn, *compileSetup.Env);
    const auto root = DeserializeRuntimeNode(bytecode, *runSetup.Env);

    // Run the graph, using the runModule setup.
    const auto graph = runSetup.BuildGraph(root);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue result;
    UNIT_ASSERT(iterator.Next(result));
    UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(result.AsStringRef()), "Canary is alive: 1/9/2009 15:37:19.");
    UNIT_ASSERT(!iterator.Next(result));
}

} // namespace

// The main idea for the test below: check whether all hacks,
// introduced to the core components and DateTime UDF (that is
// partially stubbed above) works fine for the following
// "compile/execute" matrix:
// +-------------------------------------------+
// | compile \ execute | A | B1 | C1 | B2 | C2 |
// +-------------------+---+----+----+----+----+
// |         A         | + | +  | +  | -  | -  |
// +-------------------+---+----+----+----+----+
// |         B1        | + | +  | +  | +  | -  |
// +-------------------+---+----+----+----+----+
// |         C1        | + | +  | +  | -  | +  |
// +-------------------+---+----+----+----+----+
// |         B2        | - | +  | -  | +  | -  |
// +-------------------+---+----+----+----+----+
// |         C2        | - | -  | +  | -  | +  |
// +-------------------+---+----+----+----+----+
Y_UNIT_TEST_SUITE(TMiniKQLDatetimeFormatTest) {
    Y_UNIT_TEST_LLVM(AtoB1) {
        TestDateTimeFormat<LLVM, TTestADateTime2Module, TTestB1DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(B1toA) {
        TestDateTimeFormat<LLVM, TTestB1DateTime2Module, TTestADateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(B1toB2) {
        TestDateTimeFormat<LLVM, TTestB1DateTime2Module, TTestB2DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(B2toB1) {
        TestDateTimeFormat<LLVM, TTestB2DateTime2Module, TTestB1DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(AtoC1) {
        TestDateTimeFormat<LLVM, TTestADateTime2Module, TTestC1DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(C1toA) {
        TestDateTimeFormat<LLVM, TTestC1DateTime2Module, TTestADateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(B1toC1) {
        TestDateTimeFormat<LLVM, TTestB1DateTime2Module, TTestC1DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(C1toB1) {
        TestDateTimeFormat<LLVM, TTestC1DateTime2Module, TTestB1DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(C1toC2) {
        TestDateTimeFormat<LLVM, TTestC1DateTime2Module, TTestC2DateTime2Module>();
    }
    Y_UNIT_TEST_LLVM(C2toC1) {
        TestDateTimeFormat<LLVM, TTestC2DateTime2Module, TTestC1DateTime2Module>();
    }
} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr

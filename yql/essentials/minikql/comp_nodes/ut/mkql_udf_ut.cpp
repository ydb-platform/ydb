#include "mkql_computation_node_ut.h"
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

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
    TFunctionTypeInfoBuilder typeInfoBuilder(NYql::UnknownLangVersion, env,
                                             new TTypeInfoHelper(),
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

} // namespace NMiniKQL
} // namespace NKikimr

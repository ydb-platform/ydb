#include "mkql_computation_node_ut.h"
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

namespace NKikimr {
namespace NMiniKQL {

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
        const auto udf = pb.Udf("TestModule.Test", upvalue, userType);

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
        const auto udf = pb.Udf("TestModule.Test", pb.NewVoid(), userType);
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
} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr

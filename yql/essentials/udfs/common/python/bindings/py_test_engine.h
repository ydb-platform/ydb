#pragma once

#include "py_cast.h"
#include "py_yql_module.h"
#include "py_utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/udfs/common/python/python_udf/python_udf.h>

#include <library/cpp/testing/unittest/registar.h>

#define PYTHON_TEST_TAG "Python2Test"


using namespace NKikimr;
using namespace NMiniKQL;

namespace NPython {

//////////////////////////////////////////////////////////////////////////////
// TPyInitializer
//////////////////////////////////////////////////////////////////////////////
struct TPyInitializer {
    TPyInitializer() {
        PrepareYqlModule();
        Py_Initialize();
        InitYqlModule(NYql::NUdf::EPythonFlavor::Arcadia);
        const auto rc = PyRun_SimpleString(NYql::NUdf::STANDART_STREAM_PROXY_INJECTION_SCRIPT);
        Y_ENSURE(rc >= 0, "Can't setup module");
    }
    ~TPyInitializer() {
        TermYqlModule();
        Py_Finalize();
    }
};

//////////////////////////////////////////////////////////////////////////////
// TPythonTestEngine
//////////////////////////////////////////////////////////////////////////////
class TPythonTestEngine {
public:
    TPythonTestEngine()
        : MemInfo_("Memory")
        , Alloc_(__LOCATION__)
        , Env_(Alloc_)
        , TypeInfoHelper_(new TTypeInfoHelper)
        , FunctionInfoBuilder_(NYql::UnknownLangVersion, Env_, TypeInfoHelper_, "", nullptr, {})
    {
        HolderFactory_ = MakeHolder<THolderFactory>(
                    Alloc_.Ref(),
                    MemInfo_,
                    nullptr);
        ValueBuilder_ = MakeHolder<TDefaultValueBuilder>(*HolderFactory_, NUdf::EValidatePolicy::Exception);
        BindTerminator_ = MakeHolder<TBindTerminator>(ValueBuilder_.Get());
        Singleton<TPyInitializer>();
        CastCtx_ = MakeIntrusive<TPyCastContext>(&GetValueBuilder(),
            MakeIntrusive<TPyContext>(TypeInfoHelper_.Get(), NUdf::TStringRef::Of(PYTHON_TEST_TAG), NUdf::TSourcePosition())
        );
    }

    ~TPythonTestEngine() {
        PyCleanup();
    }

    NUdf::IFunctionTypeInfoBuilder& GetTypeBuilder() {
        return FunctionInfoBuilder_;
    }

    const NUdf::IValueBuilder& GetValueBuilder() const {
        return *ValueBuilder_;
    }

    template <typename TChecker>
    void ToMiniKQL(NUdf::TType* udfType, const TStringBuf& script, TChecker&& checker) {
        TPyObjectPtr result = RunPythonFunction(script);
        UNIT_ASSERT_C(!!result, script);

        TType* type = static_cast<TType*>(udfType);
        auto value = FromPyObject(CastCtx_, type, result.Get());
        checker(value);
    }

    template <typename TExpectedType, typename TChecker>
    void ToMiniKQL(const TStringBuf& script, TChecker&& checker) {
        auto type = GetTypeBuilder().SimpleType<TExpectedType>();
        ToMiniKQL<TChecker>(type, script, std::move(checker));
    }

    template <typename TChecker>
    void ToMiniKQLWithArg(
            NUdf::TType* udfType, PyObject* argValue,
            const TStringBuf& script, TChecker&& checker)
    {
        TPyObjectPtr args = Py_BuildValue("(O)", argValue);

        auto result = RunPythonFunction(script, args.Get());
        if (!result || PyErr_Occurred()) {
            PyErr_Print();
            UNIT_FAIL("function execution error");
        }

        TType* type = static_cast<TType*>(udfType);
        auto value = FromPyObject(CastCtx_, type, result.Get());
        checker(value);
    }

    template <typename TExpectedType, typename TChecker>
    void ToMiniKQLWithArg(
            PyObject* argValue,
            const TStringBuf& script, TChecker&& checker)
    {
        auto type = GetTypeBuilder().SimpleType<TExpectedType>();
        ToMiniKQLWithArg<TChecker>(type, argValue, script, std::move(checker));
    }

    template <typename FunctionType,
              typename TMiniKQLValueBuilder,
              typename TChecker>
    void UnsafeCall(TMiniKQLValueBuilder&& builder,
                    const TStringBuf& script,
                    TChecker&& checker)
    {
        TPyObjectPtr function = CompilePythonFunction(script);
        const auto functionType = GetTypeBuilder().SimpleSignatureType<FunctionType>();
        NUdf::TCallableTypeInspector inspector(*CastCtx_->PyCtx->TypeInfoHelper, functionType);
        Y_ENSURE(inspector.GetArgsCount() == 1);
        const TType* argType = static_cast<const TType*>(inspector.GetArgType(0));
        NUdf::TUnboxedValue value = builder(argType, GetValueBuilder());
        TPyObjectPtr pyArgs = ToPyArgs(CastCtx_, functionType, &value, inspector);
        TPyObjectPtr resultObj = PyObject_CallObject(function.Get(), pyArgs.Get());

        if (!resultObj) {
            return checker(NUdf::TUnboxedValuePod::Invalid());
        }

        const auto returnType = inspector.GetReturnType();
        Y_ENSURE(CastCtx_->PyCtx->TypeInfoHelper->GetTypeKind(returnType) != NUdf::ETypeKind::Stream);

        checker(FromPyObject(CastCtx_, returnType, resultObj.Get()));
    }

    template <typename TMiniKQLValueBuilder>
    TPyObjectPtr ToPython(
                NUdf::TType* udfType,
                TMiniKQLValueBuilder&& builder,
                const TStringBuf& script)
    {
        try {
            TType* type = static_cast<TType*>(udfType);
            NUdf::TUnboxedValue value = builder(type, GetValueBuilder());
            TPyObjectPtr pyValue = ToPyObject(CastCtx_, type, value);
            if (!pyValue || PyErr_Occurred()) {
                PyErr_Print();
                UNIT_FAIL("object execution error");
            }
            TPyObjectPtr args = Py_BuildValue("(O)", pyValue.Get());

            auto result = RunPythonFunction(script, args.Get());
            if (!result || PyErr_Occurred()) {
                PyErr_Print();
                UNIT_FAIL("function execution error");
            }
            return result;
        } catch (const yexception& e) {
            Cerr << e << Endl;
            UNIT_FAIL("cast error");
        }

        Py_RETURN_NONE;
    }

    template <typename TExpectedType, typename TMiniKQLValueBuilder>
    TPyObjectPtr ToPython(TMiniKQLValueBuilder&& builder, const TStringBuf& script) {
        auto type = GetTypeBuilder().SimpleType<TExpectedType>();
        return ToPython<TMiniKQLValueBuilder>(type, std::move(builder), script);
    }

    NUdf::TUnboxedValue FromPython(NUdf::TType* udfType, const TStringBuf& script) {
        auto result = RunPythonFunction(script);
        if (!result || PyErr_Occurred()) {
            PyErr_Print();
            UNIT_FAIL("function execution error");
        }

        TType* type = static_cast<TType*>(udfType);
        return FromPyObject(CastCtx_, type, result.Get());
    }

    template <typename TExpectedType>
    NUdf::TUnboxedValue FromPython(const TStringBuf& script) {
        auto type = GetTypeBuilder().SimpleType<TExpectedType>();
        return FromPython(type, script);
    }

    template <typename TArgumentType, typename TReturnType = TArgumentType, typename TMiniKQLValueBuilder>
    NUdf::TUnboxedValue ToPythonAndBack(TMiniKQLValueBuilder&& builder, const TStringBuf& script) {
        const auto aType = GetTypeBuilder().SimpleType<TArgumentType>();
        const auto result = ToPython<TMiniKQLValueBuilder>(aType, std::move(builder), script);

        if (!result || PyErr_Occurred()) {
            PyErr_Print();
            UNIT_FAIL("function execution error");
        }

        const auto rType = static_cast<TType*>(GetTypeBuilder().SimpleType<TReturnType>());
        return FromPyObject(CastCtx_, rType, result.Get());
    }

    template <typename TArgumentType, typename TReturnType = TArgumentType, typename TMiniKQLValueBuilder, typename TChecker>
    void ToPythonAndBack(TMiniKQLValueBuilder&& builder, const TStringBuf& script, TChecker&& checker) {
        const auto result = ToPythonAndBack<TArgumentType, TReturnType, TMiniKQLValueBuilder>(std::move(builder), script);
        checker(result);
    }

private:
    TPyObjectPtr CompilePythonFunction(const TStringBuf& script) {
        TString filename(TStringBuf("embedded:test.py"));
        TPyObjectPtr code(Py_CompileString(script.data(), filename.data(), Py_file_input));
        if (!code) {
            PyErr_Print();
            UNIT_FAIL("can't compile python script");
        }

        TString moduleName(TStringBuf("py_cast_ut"));
        TPyObjectPtr module(PyImport_ExecCodeModule(moduleName.begin(), code.Get()));
        if (!module) {
            PyErr_Print();
            UNIT_FAIL("can't create python module");
        }

        TPyObjectPtr function(PyObject_GetAttrString(module.Get(), "Test"));
        if (!function) {
            PyErr_Print();
            UNIT_FAIL("function 'Test' is not found in module");
        }
        return function;
    }

    TPyObjectPtr RunPythonFunction(
            const TStringBuf& script, PyObject* args = nullptr)
    {
        TPyObjectPtr function(CompilePythonFunction(script));
        return PyObject_CallObject(function.Get(), args);
    }

private:
    TMemoryUsageInfo MemInfo_;
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    const NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    TFunctionTypeInfoBuilder FunctionInfoBuilder_;
    THolder<THolderFactory> HolderFactory_;
    THolder<TDefaultValueBuilder> ValueBuilder_;
    THolder<TBindTerminator> BindTerminator_;
    TPyCastContext::TPtr CastCtx_;
};

} // namespace NPython

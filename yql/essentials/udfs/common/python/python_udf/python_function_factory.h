#pragma once

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_terminator.h>
#include <yql/essentials/udfs/common/python/bindings/py_ptr.h>
#include <yql/essentials/udfs/common/python/bindings/py_callable.h>
#include <yql/essentials/udfs/common/python/bindings/py_cast.h>
#include <yql/essentials/udfs/common/python/bindings/py_errors.h>
#include <yql/essentials/udfs/common/python/bindings/py_gil.h>
#include <yql/essentials/udfs/common/python/bindings/py_utils.h>
#include <yql/essentials/udfs/common/python/bindings/py_yql_module.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/stream/printf.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

using namespace NYql::NUdf;
using namespace NPython;

//////////////////////////////////////////////////////////////////////////////
// TPythonFunctionFactory
//////////////////////////////////////////////////////////////////////////////
class TPythonFunctionFactory: public TBoxedValue
{
public:
    TPythonFunctionFactory(
            const TStringRef& name,
            const TStringRef& tag,
            const TType* functionType,
            ITypeInfoHelper::TPtr&& helper,
            const NYql::NUdf::TSourcePosition& pos)
        : Ctx_(new TPyContext(helper, tag, pos))
        , FunctionName_(name)
        , FunctionType_(functionType)
    {
    }

    ~TPythonFunctionFactory() {
        Ctx_->Cleanup();
        PyCleanup();
    }

private:
    TUnboxedValue Run(
            const IValueBuilder* valueBuilder,
            const TUnboxedValuePod* args) const override
    {
        TPyCastContext::TPtr castCtx = MakeIntrusive<TPyCastContext>(valueBuilder, Ctx_);

        // for get propper c-compatible null-terminating string
        TString source(args[0].AsStringRef());

        TPyGilLocker lock;
        TPyObjectPtr module = CompileModule(FunctionName_, source);
        if (!module) {
            UdfTerminate((TStringBuilder() << Ctx_->Pos << "Failed to compile module: " << GetLastErrorAsString()).data());
        }

        TPyObjectPtr function(PyObject_GetAttrString(module.Get(), FunctionName_.data()));
        if (!function) {
            UdfTerminate((TStringBuilder() << Ctx_->Pos << "Failed to find entry point: " << GetLastErrorAsString()).data());
        }

        if (!PyCallable_Check(function.Get())) {
            UdfTerminate((TStringBuilder() << Ctx_->Pos << "Entry point is not a callable").data());
        }

        try {
            SetupCallableSettings(castCtx, function.Get());
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << Ctx_->Pos << "Failed to setup callable settings: "
                                           << e.what()).data());
        }
        return FromPyCallable(castCtx, FunctionType_, function.Release());
    }

    static TPyObjectPtr CompileModule(const TString& name, const TString& source) {
        unsigned int moduleNum = AtomicCounter++;
        TString filename(TStringBuf("embedded:"));
        filename += name;

        TPyObjectPtr module, code;
        if (HasEncodingCookie(source)) {
            code.ResetSteal(Py_CompileString(source.data(), filename.data(), Py_file_input));
        } else {
            PyCompilerFlags cflags;
            cflags.cf_flags = PyCF_SOURCE_IS_UTF8;

            code.ResetSteal(Py_CompileStringFlags(
                    source.data(), filename.data(), Py_file_input, &cflags));
        }

        if (code) {
            TString nameWithNum = name + ToString(moduleNum);
            char* moduleName = const_cast<char*>(nameWithNum.data());
            module.ResetSteal(PyImport_ExecCodeModule(moduleName, code.Get()));
        }

        return module;
    }

    const TPyContext::TPtr Ctx_;
    const TString FunctionName_;
    const TType* FunctionType_;
    inline static std::atomic_uint AtomicCounter = 0;
};

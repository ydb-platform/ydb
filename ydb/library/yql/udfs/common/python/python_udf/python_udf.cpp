#include "python_udf.h"
#include "python_function_factory.h"

#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/udfs/common/python/bindings/py_utils.h>

#include <util/generic/vector.h>
#include <util/system/execpath.h>

namespace {

#if PY_MAJOR_VERSION >= 3
#define PYTHON_PROGRAMM_NAME L"YQL::Python3"
#else
#define PYTHON_PROGRAMM_NAME "YQL::Python2"
#endif

int AddToPythonPath(const TVector<TStringBuf>& pathVals)
{
    char pathVar[] = "path"; // PySys_{Get,Set}Object take a non-const char* arg

    TPyObjectPtr sysPath(PySys_GetObject(pathVar), TPyObjectPtr::ADD_REF);
    if (!sysPath) return -1;

    for (const auto& val: pathVals) {
        TPyObjectPtr pyStr = PyRepr(val.data());
        int rc = PyList_Append(sysPath.Get(), pyStr.Get());
        if (rc != 0) {
            return rc;
        }
    }

    return PySys_SetObject(pathVar, sysPath.Get());
}

void InitArcadiaPythonRuntime()
{
    // Arcadia static python import hook resides in __res module
    // It modifies sys.meta_path upon import

    TPyObjectPtr mod(PyImport_ImportModule("__res"));
    Y_ABORT_UNLESS(mod, "Can't import arcadia python runtime");
}

//////////////////////////////////////////////////////////////////////////////
// TPythonModule
//////////////////////////////////////////////////////////////////////////////
class TPythonModule: public IUdfModule
{
public:
    TPythonModule(const TString& resourceName, EPythonFlavor pythonFlavor, bool standalone = true)
        : ResourceName(resourceName), Standalone(standalone)
    {
        if (Standalone) {
            Py_SetProgramName(PYTHON_PROGRAMM_NAME);
            PrepareYqlModule();
            Py_Initialize();
        }

        InitYqlModule(pythonFlavor, standalone);

        const auto rc = PyRun_SimpleString(R"(
# numpy on import may find installed openblas library and load it,
# which in turn causes it to start CPUCOUNT threads
# with approx. 40Mb memory reserved for each thread;
#
# See more detailed explanation here: https://st.yandex-team.ru/STATLIBS-1715#5bfc68ecbbc039001cec572a
#
# Thus, we reduce negative effects as much as possible
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'


# Following part allows us later to format tracebacks via sys.excepthook
# in thread-safe manner
import sys
import threading
if sys.version_info >= (3, 0):
    from io import StringIO, TextIOWrapper as SysStderrType
else:
    from cStringIO import StringIO
    SysStderrType = file

class StderrLocal(threading.local):

    def __init__(self):
        self.is_real_mode = True
        self.buffer = StringIO()


class StderrProxy(object):
    def __init__(self, stderr):
        self._stderr = stderr
        self._tls = StderrLocal()

    def _toggle_real_mode(self):
        self._tls.is_real_mode = not self._tls.is_real_mode
        if not self._tls.is_real_mode:
            self._tls.buffer.clear()

    def _get_value(self):
        assert not self._tls.is_real_mode
        return self._tls.buffer.getvalue()

    def __getattr__(self, attr):
        target = self._stderr
        if not self._tls.is_real_mode:
            target = self._tls.buffer

        return getattr(target, attr)

if isinstance(sys.stderr, SysStderrType):
    sys.stderr = StderrProxy(sys.stderr)
)");
        Y_ABORT_UNLESS(rc >= 0, "Can't setup module");

        if (pythonFlavor == EPythonFlavor::Arcadia) {
            InitArcadiaPythonRuntime();
        }

#ifndef _win_
        if (Standalone) {
            TVector<TStringBuf> paths;
            if (pythonFlavor == EPythonFlavor::System) {
                paths.push_back(TStringBuf("/usr/lib/python2.7/dist-packages"));
            }
            paths.push_back(TStringBuf("."));
            const auto r = AddToPythonPath(paths);
            Y_ABORT_UNLESS(r >= 0, "Can't add dist-packages into sys.path");
        }
#endif

        char executableVar[] = "executable"; // PySys_{Get,Set}Object take a non-const char* arg
        TPyObjectPtr pyExecutableStr = PyRepr(GetExecPath().data());
        Y_ABORT_UNLESS(PySys_SetObject(executableVar, pyExecutableStr.Get()) >= 0, "Can't set sys.executable");

        if (Standalone) {
            PyEval_InitThreads();
            MainThreadState_ = PyEval_SaveThread();
        }
    }

    ~TPythonModule() {
        if (Standalone) {
            PyEval_RestoreThread(MainThreadState_);
            Py_Finalize();
        }
    }

    void CleanupOnTerminate() const final {
        PyCleanup();
    }

    void GetAllFunctions(IFunctionsSink&) const final {}

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final
    {
        Y_UNUSED(typeConfig);

        if (flags & TFlags::TypesOnly) {
            return;
        }

        try {
            auto typeHelper = builder.TypeInfoHelper();
            if (ETypeKind::Callable != typeHelper->GetTypeKind(userType)) {
                return builder.SetError(TStringRef::Of("Expected callable type"));
            }

            const auto pos = builder.GetSourcePosition();
            builder.Implementation(new TPythonFunctionFactory(name, ResourceName, userType, std::move(typeHelper), pos));
        } catch (const yexception& e) {
            builder.SetError(TStringBuf(e.what()));
        }
    }

private:
    TString ResourceName;
    bool Standalone;
    PyThreadState* MainThreadState_;
};

//////////////////////////////////////////////////////////////////////////////
// TStubModule
//////////////////////////////////////////////////////////////////////////////
class TStubModule: public IUdfModule {
    void GetAllFunctions(IFunctionsSink&) const final {}

    void BuildFunctionTypeInfo(
            const TStringRef& /*name*/,
            TType* /*userType*/,
            const TStringRef& /*typeConfig*/,
            ui32 flags,
            IFunctionTypeInfoBuilder& /*builder*/) const final
    {
        Y_DEBUG_ABORT_UNLESS(flags & TFlags::TypesOnly,
                "in stub module this function can be called only for types loading");
    }

    void CleanupOnTerminate() const final {}
};

} // namespace

void NKikimr::NUdf::RegisterYqlPythonUdf(
        IRegistrator& registrator,
        ui32 flags,
        TStringBuf moduleName,
        TStringBuf resourceName,
        EPythonFlavor pythonFlavor)
{
    if (flags & IRegistrator::TFlags::TypesOnly) {
        registrator.AddModule(moduleName, new TStubModule);
    } else {
        registrator.AddModule(
            moduleName,
            NKikimr::NUdf::GetYqlPythonUdfModule(resourceName, pythonFlavor, true)
        );
    }
}

TUniquePtr<NKikimr::NUdf::IUdfModule> NKikimr::NUdf::GetYqlPythonUdfModule(
    TStringBuf resourceName, NKikimr::NUdf::EPythonFlavor pythonFlavor,
    bool standalone
) {
    return new TPythonModule(TString(resourceName), pythonFlavor, standalone);
}

#include "py_yql_module.h"

#include "py_void.h"
#include "py_iterator.h"
#include "py_list.h"
#include "py_dict.h"
#include "py_stream.h"
#include "py_utils.h"
#include "py_callable.h"

#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/udfs/common/python/python_udf/python_udf.h>

namespace NPython {

static PyMethodDef ModuleMethods[] = {
    { nullptr, nullptr, 0, nullptr }      /* sentinel */
};

#define MODULE_NAME "yql"

#if PY_MAJOR_VERSION >= 3
#define MODULE_NAME_TYPING "yql.typing"
#endif

#define MODULE_INITIALIZED_ATTRIBUTE "_initialized"

PyDoc_STRVAR(ModuleDoc,
    "This module provides YQL specific types for Python.");

#if PY_MAJOR_VERSION >= 3
PyDoc_STRVAR(ModuleDocTyping,
    "This module provides annotations for YQL types for Python.");
#endif

PyDoc_STRVAR(StopIterationException_doc,
    "Can be throwed to yield stream iteration.");

#define PREPARE_TYPE(Name, Type) \
    do { \
        if (PyType_Ready(Type) < 0) { \
            throw yexception() << "Can't prepare type: " << (Name); \
        } \
    } while (0)

#define REGISTER_TYPE(Name, Type) \
    do { \
        PREPARE_TYPE(Name, Type); \
        Py_INCREF(Type); \
        if (PyModule_AddObject(module, (Name), (PyObject*) Type) < 0) { \
            throw yexception() << "Can't add type: " << (Name); \
        } \
    } while (0)

#define REGISTER_OBJECT(Name, Object) \
    do { \
        if (PyDict_SetItemString(dict, (Name), (PyObject *) (Object)) < 0) \
            throw yexception() << "Can't register object: " << (Name); \
    } while (0)

#define REGISTER_EXCEPTION(Name, Object, Doc) \
    do { \
        if (!Object) { \
            Object = PyErr_NewExceptionWithDoc((char*) MODULE_NAME "." Name, Doc, nullptr, nullptr); \
            if (!Object) { \
                throw yexception() << "Can't register exception: " << (Name); \
            } \
            REGISTER_OBJECT(Name, Object); \
        } \
    } while (0)

#if PY_MAJOR_VERSION >= 3
static PyModuleDef ModuleDefinition = {
        PyModuleDef_HEAD_INIT,
        INIT_MEMBER(m_name, MODULE_NAME),
        INIT_MEMBER(m_doc, ModuleDoc),
        INIT_MEMBER(m_size, -1),
        INIT_MEMBER(m_methods, ModuleMethods),
        INIT_MEMBER(m_slots, nullptr),
        INIT_MEMBER(m_traverse, nullptr),
        INIT_MEMBER(m_clear, nullptr),
        INIT_MEMBER(m_free, nullptr),
};

static PyModuleDef ModuleDefinitionTyping = {
        PyModuleDef_HEAD_INIT,
        INIT_MEMBER(m_name, MODULE_NAME_TYPING),
        INIT_MEMBER(m_doc, ModuleDocTyping),
        INIT_MEMBER(m_size, -1),
        INIT_MEMBER(m_methods, nullptr),
        INIT_MEMBER(m_slots, nullptr),
        INIT_MEMBER(m_traverse, nullptr),
        INIT_MEMBER(m_clear, nullptr),
        INIT_MEMBER(m_free, nullptr),
};

PyMODINIT_FUNC PyInit_YQL(void)
{
    auto mod = PyModule_Create(&ModuleDefinition);
    PyModule_AddObject(mod, "__path__", Py_BuildValue("()"));
    return mod;
}

void go_throw();

PyMODINIT_FUNC PyInit_YQLTyping(void)
{
    return PyModule_Create(&ModuleDefinitionTyping);
}
#else
PyMODINIT_FUNC PyInit_YQL(void)
{
    Py_InitModule3(MODULE_NAME, ModuleMethods, ModuleDoc);
}
#endif

void PrepareYqlModule() {
    PyImport_AppendInittab(MODULE_NAME, &PyInit_YQL);
#if PY_MAJOR_VERSION >= 3
    PyImport_AppendInittab(MODULE_NAME_TYPING, &PyInit_YQLTyping);
#endif
}

#if PY_MAJOR_VERSION >= 3
void RegisterRuntimeModule(const char* name, PyObject* module) {
    if (!module || !PyModule_Check(module)) {
        throw yexception() << "Invalid object for module " << name;
    }

    // borrowed reference
    PyObject* modules = PyImport_GetModuleDict();
    if (!modules || !PyDict_CheckExact(modules)) {
        throw yexception() << "Can't get sys.modules dictionary";
    }

    if (PyDict_SetItemString(modules, name, module) < 0) {
        throw yexception() << "Can't register module " << name;
    }
}
#endif

void InitYqlModule(NYql::NUdf::EPythonFlavor pythonFlavor, bool standalone) {
    TPyObjectPtr m = PyImport_ImportModule(MODULE_NAME);
    if (!standalone && !m) {
        PyErr_Clear();
#if PY_MAJOR_VERSION >= 3
        m = PyInit_YQL();
        RegisterRuntimeModule(MODULE_NAME, m.Get());
#else
        PyInit_YQL();
#endif
        m = PyImport_ImportModule(MODULE_NAME);
    }

    PyObject* module = m.Get();

    if (!module) {
        throw yexception() << "Can't get YQL module.";
    }

    TPyObjectPtr initialized = PyObject_GetAttrString(module, MODULE_INITIALIZED_ATTRIBUTE);
    if (!initialized) {
        PyErr_Clear();
    } else if (initialized.Get() == Py_True) {
        return;
    }

    PyObject* dict = PyModule_GetDict(module);

    REGISTER_TYPE("TVoid", &PyVoidType);
    REGISTER_OBJECT("Void", &PyVoidObject);

    PREPARE_TYPE("TIterator", &PyIteratorType);
    PREPARE_TYPE("TPairIterator", &PyPairIteratorType);

    PREPARE_TYPE("TDict", &PyLazyDictType);
    PREPARE_TYPE("TSet", &PyLazySetType);

    PREPARE_TYPE("TLazyListIterator", &PyLazyListIteratorType);
    PREPARE_TYPE("TLazyList", &PyLazyListType);
    PREPARE_TYPE("TThinListIterator", &PyThinListIteratorType);
    PREPARE_TYPE("TThinList", &PyThinListType);

    PREPARE_TYPE("TStream", &PyStreamType);
    PREPARE_TYPE("TCallable", &PyCallableType);

    REGISTER_EXCEPTION("TYieldIteration", PyYieldIterationException, StopIterationException_doc);

#if PY_MAJOR_VERSION >= 3
    if (pythonFlavor == NYql::NUdf::EPythonFlavor::Arcadia) {
        if (!standalone) {
            TPyObjectPtr typingModule = PyImport_ImportModule(MODULE_NAME_TYPING);
            if (!typingModule) {
                PyErr_Clear();
                typingModule = PyInit_YQLTyping();
                RegisterRuntimeModule(MODULE_NAME_TYPING, typingModule.Get());
            }
        }

        const auto typing = NResource::Find(TStringBuf("typing.py"));
        const auto rc = PyRun_SimpleStringFlags(typing.c_str(), nullptr);

        if (rc < 0) {
            // Not sure if PyErr_Print() works after PyRun_SimpleStringFlags,
            // but just in case...
            PyErr_Print();
            ythrow yexception() << "Can't parse YQL type annotations module";
        }

        auto processError = [&] (PyObject* obj, TStringBuf message) {
            if (obj) {
                return;
            }
            PyObject *ptype, *pvalue, *ptraceback;
            PyErr_Fetch(&ptype, &pvalue, &ptraceback);
            if (pvalue) {
                auto pstr = PyObject_Str(pvalue);
                if (pstr) {
                    if (auto err_msg = PyUnicode_AsUTF8(pstr)) {
                        Cerr << err_msg << Endl;
                    }
                }
                PyErr_Restore(ptype, pvalue, ptraceback);
            }
            ythrow yexception() << "Can't setup YQL type annotations module: " << message;
        };

        auto main = PyImport_ImportModule("__main__");
        processError(main, "PyImport_ImportModule");
        auto function = PyObject_GetAttrString(main, "main");
        processError(function, "PyObject_GetAttrString");
        auto args = PyTuple_New(0);
        processError(args, "PyTuple_New");
        auto result = PyObject_CallObject(function, args);
        processError(result, "PyObject_CallObject");

        Py_DECREF(result);
        Py_DECREF(args);
        Py_DECREF(function);
        Py_DECREF(main);
    }
#endif

    REGISTER_OBJECT(MODULE_INITIALIZED_ATTRIBUTE, Py_True);
}

void TermYqlModule() {
    PyYieldIterationException = nullptr;
}

} // namspace NPython

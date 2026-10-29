#include <Python.h>
#include <librsvg/rsvg.h>

namespace {
PyObject* SvgSize(PyObject*, PyObject*) {
    const char svg[] = "<svg xmlns='http://www.w3.org/2000/svg' width='17' height='23'/>";
    GError* error = nullptr;
    RsvgHandle* handle = rsvg_handle_new_from_data(reinterpret_cast<const guint8*>(svg), sizeof(svg) - 1, &error);
    if (!handle) {
        PyErr_SetString(PyExc_RuntimeError, error ? error->message : "failed to load SVG");
        g_clear_error(&error);
        return nullptr;
    }
    double width = 0;
    double height = 0;
    const bool sized = rsvg_handle_get_intrinsic_size_in_pixels(handle, &width, &height);
    g_object_unref(handle);
    if (!sized) {
        PyErr_SetString(PyExc_RuntimeError, "SVG has no intrinsic size");
        return nullptr;
    }
    return Py_BuildValue("(dd)", width, height);
}

PyMethodDef Methods[] = {{"svg_size", SvgSize, METH_NOARGS, nullptr}, {nullptr, nullptr, 0, nullptr}};
PyModuleDef Module = {PyModuleDef_HEAD_INIT, "_rust_runtime_test", nullptr, -1, Methods,
                      nullptr, nullptr, nullptr, nullptr};
}

extern "C" PyObject* PyInit__rust_runtime_test() {
    return PyModule_Create(&Module);
}

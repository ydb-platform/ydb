struct PyObject;
extern "C" int PyImport_AppendInittab(const char* name, PyObject* (*initfunc)());
extern "C" PyObject* PyInit_temporal_sdk_bridge();

namespace {
    struct TRegistrar {
        inline TRegistrar() {
            // TODO Collect all modules and call PyImport_ExtendInittab once
            PyImport_AppendInittab("temporalio.bridge.temporal_sdk_bridge", PyInit_temporal_sdk_bridge);
        }
    } REG;
}

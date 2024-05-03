#include "main.h"

extern "C"
int RunPythonImpl(int argc, char** argv);

extern "C"
int RunPython(int argc, char** argv) {
    return RunPythonImpl(argc, argv);
}

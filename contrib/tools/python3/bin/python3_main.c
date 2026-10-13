/* Entry point for the self-contained python3 binary.
 *
 * The entire standard library is embedded as frozen modules (see the
 * generated frozen_index.c alongside this file), and we explicitly disable
 * any filesystem lookup for imports. That gives a binary which cannot
 * reach out to host paths for stdlib at runtime — useful as a hermetic
 * build tool.
 *
 * The configuration used here:
 *   - isolated mode: no PYTHONPATH/PYTHONHOME, no .pth files, no user site
 *   - parse_argv = 1: behave like a real python3 CLI for -c, -m, script.py
 *   - empty module_search_paths: sys.path is [] (no disk lookups)
 *   - site_import = 0: site.py would walk prefixes looking for site-packages
 */

#include <Python.h>

int main(int argc, char** argv) {
    PyConfig config;
    PyConfig_InitIsolatedConfig(&config);

    config.parse_argv = 1;
    config.site_import = 0;
    config.use_environment = 0;
    config.module_search_paths_set = 1;
    // PyConfig_InitIsolatedConfig() defaults use_frozen_modules to 0, which
    // makes look_up_frozen() in Python/import.c walk only _PyImport_FrozenBootstrap
    // and skip _PyImport_FrozenStdlib entirely. Since our entire stdlib lives
    // in the stdlib table (see gen_frozen_index.py), we must opt in explicitly
    // or every import past _frozen_importlib_external fails with ModuleNotFoundError.
    config.use_frozen_modules = 1;

    PyStatus status = PyConfig_SetBytesArgv(&config, argc, argv);
    if (PyStatus_Exception(status)) {
        goto fail;
    }

    status = Py_InitializeFromConfig(&config);
    PyConfig_Clear(&config);
    if (PyStatus_Exception(status)) {
        Py_ExitStatusException(status);
    }

    return Py_RunMain();

fail:
    PyConfig_Clear(&config);
    Py_ExitStatusException(status);
    return 1;
}

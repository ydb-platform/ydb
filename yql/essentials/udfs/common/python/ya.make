# This module should not be exported under CMake since it requires Python build
NO_BUILD_IF(STRICT EXPORT_CMAKE)

RECURSE(
    bindings
    main_py3
    python3_small
    python_udf
    system_python
)

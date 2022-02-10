PY23_NATIVE_LIBRARY()

LICENSE(BSL-1.0) 
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

CFLAGS(
    GLOBAL -DBOOST_PYTHON_STATIC_LIB
)

SRCS(
    src/list.cpp
    src/long.cpp
    src/dict.cpp
    src/tuple.cpp
    src/str.cpp
    src/slice.cpp
    src/converter/from_python.cpp
    src/converter/registry.cpp
    src/converter/type_id.cpp
    src/object/enum.cpp
    src/object/class.cpp
    src/object/function.cpp
    src/object/inheritance.cpp
    src/object/life_support.cpp
    src/object/pickle_support.cpp
    src/errors.cpp
    src/module.cpp
    src/converter/builtin_converters.cpp
    src/converter/arg_to_python_base.cpp
    src/object/iterator.cpp
    src/object/stl_iterator.cpp
    src/object_protocol.cpp
    src/object_operators.cpp
    src/wrapper.cpp
    src/import.cpp
    src/exec.cpp
    src/object/function_doc_signature.cpp
)

END()

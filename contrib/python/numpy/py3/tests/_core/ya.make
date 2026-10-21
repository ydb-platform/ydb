PY3TEST()

REQUIREMENTS(ram:25)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/python/numpy/py3/tests
)

DATA(
    arcadia/contrib/python/numpy/py3/numpy
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/_core/tests/test__exceptions.py
    numpy/_core/tests/test_abc.py
    numpy/_core/tests/test_api.py
    numpy/_core/tests/test_argparse.py
    numpy/_core/tests/test_array_api_info.py
    numpy/_core/tests/test_array_coercion.py
    #numpy/_core/tests/test_array_interface.py
    numpy/_core/tests/test_arraymethod.py
    numpy/_core/tests/test_arrayobject.py
    numpy/_core/tests/test_arrayprint.py
    numpy/_core/tests/test_casting_floatingpoint_errors.py
    numpy/_core/tests/test_casting_unittests.py
    numpy/_core/tests/test_conversion_utils.py
    numpy/_core/tests/test_cpu_dispatcher.py
    numpy/_core/tests/test_cpu_features.py
    numpy/_core/tests/test_custom_dtypes.py
    numpy/_core/tests/test_cython.py
    numpy/_core/tests/test_datetime.py
    numpy/_core/tests/test_defchararray.py
    numpy/_core/tests/test_deprecations.py
    numpy/_core/tests/test_dlpack.py
    numpy/_core/tests/test_dtype.py
    numpy/_core/tests/test_einsum.py
    numpy/_core/tests/test_errstate.py
    numpy/_core/tests/test_extint128.py
    numpy/_core/tests/test_function_base.py
    numpy/_core/tests/test_getlimits.py
    numpy/_core/tests/test_half.py
    numpy/_core/tests/test_hashtable.py
    numpy/_core/tests/test_indexerrors.py
    numpy/_core/tests/test_indexing.py
    numpy/_core/tests/test_item_selection.py
    numpy/_core/tests/test_limited_api.py
    numpy/_core/tests/test_longdouble.py
    numpy/_core/tests/test_machar.py
    numpy/_core/tests/test_mem_overlap.py
    #numpy/_core/tests/test_mem_policy.py
    numpy/_core/tests/test_memmap.py
    numpy/_core/tests/test_multithreading.py
    numpy/_core/tests/test_multiarray.py
    numpy/_core/tests/test_nditer.py
    numpy/_core/tests/test_nep50_promotions.py
    numpy/_core/tests/test_numeric.py
    numpy/_core/tests/test_numerictypes.py
    numpy/_core/tests/test_overrides.py
    numpy/_core/tests/test_print.py
    numpy/_core/tests/test_protocols.py
    numpy/_core/tests/test_records.py
    numpy/_core/tests/test_regression.py
    numpy/_core/tests/test_scalar_ctors.py
    numpy/_core/tests/test_scalar_methods.py
    numpy/_core/tests/test_scalarbuffer.py
    numpy/_core/tests/test_scalarinherit.py
    numpy/_core/tests/test_scalarmath.py
    numpy/_core/tests/test_scalarprint.py
    numpy/_core/tests/test_shape_base.py
    numpy/_core/tests/test_simd.py
    numpy/_core/tests/test_simd_module.py
    numpy/_core/tests/test_stringdtype.py
    numpy/_core/tests/test_strings.py
    numpy/_core/tests/test_ufunc.py
    numpy/_core/tests/test_umath.py
    numpy/_core/tests/test_umath_accuracy.py
    numpy/_core/tests/test_umath_complex.py
    numpy/_core/tests/test_unicode.py
)

END()

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
    numpy/core/tests/__init__.py
    numpy/core/tests/_locales.py
    numpy/core/tests/test__exceptions.py
    numpy/core/tests/test_abc.py
    numpy/core/tests/test_api.py
    numpy/core/tests/test_argparse.py
    numpy/core/tests/test_array_coercion.py
    #numpy/core/tests/test_array_interface.py
    numpy/core/tests/test_arraymethod.py
    numpy/core/tests/test_arrayprint.py
    numpy/core/tests/test_casting_floatingpoint_errors.py
    numpy/core/tests/test_casting_unittests.py
    numpy/core/tests/test_conversion_utils.py
    numpy/core/tests/test_cpu_dispatcher.py
    numpy/core/tests/test_cpu_features.py
    numpy/core/tests/test_custom_dtypes.py
    numpy/core/tests/test_cython.py
    numpy/core/tests/test_datetime.py
    numpy/core/tests/test_defchararray.py
    numpy/core/tests/test_deprecations.py
    numpy/core/tests/test_dlpack.py
    numpy/core/tests/test_dtype.py
    numpy/core/tests/test_einsum.py
    numpy/core/tests/test_errstate.py
    numpy/core/tests/test_extint128.py
    numpy/core/tests/test_function_base.py
    numpy/core/tests/test_getlimits.py
    numpy/core/tests/test_half.py
    numpy/core/tests/test_hashtable.py
    numpy/core/tests/test_indexerrors.py
    numpy/core/tests/test_indexing.py
    numpy/core/tests/test_item_selection.py
    numpy/core/tests/test_limited_api.py
    numpy/core/tests/test_longdouble.py
    numpy/core/tests/test_machar.py
    numpy/core/tests/test_mem_overlap.py
    #numpy/core/tests/test_mem_policy.py
    numpy/core/tests/test_memmap.py
    numpy/core/tests/test_multiarray.py
    numpy/core/tests/test_nditer.py
    numpy/core/tests/test_nep50_promotions.py
    numpy/core/tests/test_numeric.py
    numpy/core/tests/test_numerictypes.py
    numpy/core/tests/test_numpy_2_0_compat.py
    numpy/core/tests/test_overrides.py
    numpy/core/tests/test_print.py
    numpy/core/tests/test_protocols.py
    numpy/core/tests/test_records.py
    numpy/core/tests/test_regression.py
    numpy/core/tests/test_scalar_ctors.py
    numpy/core/tests/test_scalar_methods.py
    numpy/core/tests/test_scalarbuffer.py
    numpy/core/tests/test_scalarinherit.py
    numpy/core/tests/test_scalarmath.py
    numpy/core/tests/test_scalarprint.py
    numpy/core/tests/test_shape_base.py
    numpy/core/tests/test_simd.py
    numpy/core/tests/test_simd_module.py
    numpy/core/tests/test_strings.py
    numpy/core/tests/test_ufunc.py
    numpy/core/tests/test_umath.py
    numpy/core/tests/test_umath_accuracy.py
    numpy/core/tests/test_umath_complex.py
    numpy/core/tests/test_unicode.py
)

END()

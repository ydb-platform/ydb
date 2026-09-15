PY3_LIBRARY()

PROVIDES(numpy)

VERSION(2.2.6)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/libs/clapack
    contrib/python/numpy/py3/numpy/random
)

ADDINCL(
    contrib/python/numpy/include
    contrib/python/numpy/include/numpy/core
    contrib/python/numpy/include/numpy/core/include
    FOR cython contrib/python/numpy/include/numpy/core/include
    contrib/python/numpy/include/numpy/core/include/numpy
    contrib/python/numpy/include/numpy/core/src
    contrib/python/numpy/include/numpy/core/src/common
    contrib/python/numpy/include/numpy/core/src/highway
    contrib/python/numpy/include/numpy/core/src/multiarray
    contrib/python/numpy/include/numpy/core/src/npymath
    contrib/python/numpy/include/numpy/core/src/npysort
    contrib/python/numpy/include/numpy/core/src/umath
    contrib/python/numpy/include/numpy/distutils/include
)
ADDINCL(
    GLOBAL FOR cython contrib/python/numpy/py3
)

NO_COMPILER_WARNINGS()

NO_EXTENDED_SOURCE_SEARCH()

NO_LINT()

NO_CHECK_IMPORTS(
    numpy._pyinstaller.*
    numpy._core.umath_tests
    numpy._core.cversions
    numpy.distutils.*
    numpy.f2py.*
)

CFLAGS(
    -DHAVE_CBLAS
    -DHAVE_NPY_CONFIG_H=1
    -D_FILE_OFFSET_BITS=64
    -D_LARGEFILE64_SOURCE=1
    -D_LARGEFILE_SOURCE=1
    -DNPY_INTERNAL_BUILD=1
    -DNPY_MTARGETS_BASELINE
    -Dintern_strings=_numpy_intern_strings
)

IF (ARCH_PPC64LE)
    CFLAGS(-DNPY_DISABLE_OPTIMIZATION=1)
ENDIF()

IF (CLANG)
    CFLAGS(
        -ftrapping-math
        -DNPY_HAVE_CLANG_FPSTRICT
    )
ENDIF()

SRCS(
    numpy/_core/src/_simd/_simd.c
    numpy/_core/src/_simd/_simd.dispatch.c
    numpy/_core/src/common/array_assign.c
    numpy/_core/src/common/cblasfuncs.c
    numpy/_core/src/common/gil_utils.c
    numpy/_core/src/common/mem_overlap.c
    numpy/_core/src/common/npy_argparse.c
    numpy/_core/src/common/npy_cpu_dispatch.c
    numpy/_core/src/common/npy_cpu_features.c
    numpy/_core/src/common/npy_hashtable.cpp
    numpy/_core/src/common/npy_import.c
    numpy/_core/src/common/npy_longdouble.c
    numpy/_core/src/common/numpyos.c
    # numpy/_core/src/common/python_xerbla.c is defined in blas.
    numpy/_core/src/common/ucsnarrow.c
    numpy/_core/src/common/ufunc_override.c
    numpy/_core/src/dummymodule.c
    numpy/_core/src/multiarray/_multiarray_tests.c
    numpy/_core/src/multiarray/abstractdtypes.c
    numpy/_core/src/multiarray/alloc.c
    numpy/_core/src/multiarray/argfunc.dispatch.c
    numpy/_core/src/multiarray/array_api_standard.c
    numpy/_core/src/multiarray/array_assign_array.c
    numpy/_core/src/multiarray/array_assign_scalar.c
    numpy/_core/src/multiarray/array_coercion.c
    numpy/_core/src/multiarray/array_converter.c
    numpy/_core/src/multiarray/array_method.c
    numpy/_core/src/multiarray/arrayfunction_override.c
    numpy/_core/src/multiarray/arrayobject.c
    numpy/_core/src/multiarray/arraytypes.c
    numpy/_core/src/multiarray/arraywrap.c
    numpy/_core/src/multiarray/buffer.c
    numpy/_core/src/multiarray/calculation.c
    numpy/_core/src/multiarray/common.c
    numpy/_core/src/multiarray/common_dtype.c
    numpy/_core/src/multiarray/compiled_base.c
    numpy/_core/src/multiarray/conversion_utils.c
    numpy/_core/src/multiarray/convert.c
    numpy/_core/src/multiarray/convert_datatype.c
    numpy/_core/src/multiarray/ctors.c
    numpy/_core/src/multiarray/datetime.c
    numpy/_core/src/multiarray/datetime_busday.c
    numpy/_core/src/multiarray/datetime_busdaycal.c
    numpy/_core/src/multiarray/datetime_strings.c
    numpy/_core/src/multiarray/descriptor.c
    numpy/_core/src/multiarray/dlpack.c
    numpy/_core/src/multiarray/dragon4.c
    numpy/_core/src/multiarray/dtype_transfer.c
    numpy/_core/src/multiarray/dtype_traversal.c
    numpy/_core/src/multiarray/dtypemeta.c
    numpy/_core/src/multiarray/einsum.c
    numpy/_core/src/multiarray/einsum_sumprod.c
    numpy/_core/src/multiarray/flagsobject.c
    numpy/_core/src/multiarray/getset.c
    numpy/_core/src/multiarray/hashdescr.c
    numpy/_core/src/multiarray/item_selection.c
    numpy/_core/src/multiarray/iterators.c
    numpy/_core/src/multiarray/legacy_dtype_implementation.c
    numpy/_core/src/multiarray/lowlevel_strided_loops.c
    numpy/_core/src/multiarray/mapping.c
    numpy/_core/src/multiarray/methods.c
    numpy/_core/src/multiarray/multiarraymodule.c
    numpy/_core/src/multiarray/nditer_api.c
    numpy/_core/src/multiarray/nditer_constr.c
    numpy/_core/src/multiarray/nditer_pywrap.c
    numpy/_core/src/multiarray/nditer_templ.c
    numpy/_core/src/multiarray/npy_static_data.c
    numpy/_core/src/multiarray/number.c
    numpy/_core/src/multiarray/public_dtype_api.c
    numpy/_core/src/multiarray/refcount.c
    numpy/_core/src/multiarray/scalarapi.c
    numpy/_core/src/multiarray/scalartypes.c
    numpy/_core/src/multiarray/sequence.c
    numpy/_core/src/multiarray/shape.c
    numpy/_core/src/multiarray/strfuncs.c
    numpy/_core/src/multiarray/stringdtype/casts.c
    numpy/_core/src/multiarray/stringdtype/dtype.c
    numpy/_core/src/multiarray/stringdtype/static_string.c
    numpy/_core/src/multiarray/stringdtype/utf8_utils.c
    numpy/_core/src/multiarray/temp_elide.c
    numpy/_core/src/multiarray/textreading/conversions.c
    numpy/_core/src/multiarray/textreading/field_types.c
    numpy/_core/src/multiarray/textreading/growth.c
    numpy/_core/src/multiarray/textreading/readtext.c
    numpy/_core/src/multiarray/textreading/rows.c
    numpy/_core/src/multiarray/textreading/str_to_int.c
    numpy/_core/src/multiarray/textreading/stream_pyobject.c
    numpy/_core/src/multiarray/textreading/tokenize.cpp
    numpy/_core/src/multiarray/usertypes.c
    numpy/_core/src/multiarray/vdot.c
    numpy/_core/src/npymath/arm64_exports.c
    numpy/_core/src/npymath/halffloat.cpp
    numpy/_core/src/npymath/ieee754.c
    numpy/_core/src/npymath/ieee754.cpp
    numpy/_core/src/npymath/npy_math.c
    numpy/_core/src/npymath/npy_math_complex.c
    numpy/_core/src/npysort/binsearch.cpp
    numpy/_core/src/npysort/heapsort.cpp
    numpy/_core/src/npysort/mergesort.cpp
    numpy/_core/src/npysort/quicksort.cpp
    numpy/_core/src/npysort/radixsort.cpp
    numpy/_core/src/npysort/selection.cpp
    numpy/_core/src/npysort/timsort.cpp
    numpy/_core/src/umath/_operand_flag_tests.c
    numpy/_core/src/umath/_rational_tests.c
    numpy/_core/src/umath/_scaled_float_dtype.c
    numpy/_core/src/umath/_struct_ufunc_tests.c
    numpy/_core/src/umath/_umath_tests.c
    numpy/_core/src/umath/_umath_tests.dispatch.c
    numpy/_core/src/umath/clip.cpp
    numpy/_core/src/umath/dispatching.cpp
    numpy/_core/src/umath/extobj.c
    numpy/_core/src/umath/legacy_array_method.c
    numpy/_core/src/umath/loops.c
    numpy/_core/src/umath/loops_arithm_fp.dispatch.c
    numpy/_core/src/umath/loops_arithmetic.dispatch.c
    numpy/_core/src/umath/loops_autovec.dispatch.c
    numpy/_core/src/umath/loops_comparison.dispatch.c
    numpy/_core/src/umath/loops_exponent_log.dispatch.c
    numpy/_core/src/umath/loops_hyperbolic.dispatch.c
    numpy/_core/src/umath/loops_logical.dispatch.c
    numpy/_core/src/umath/loops_minmax.dispatch.c
    numpy/_core/src/umath/loops_modulo.dispatch.c
    numpy/_core/src/umath/loops_trigonometric.dispatch.cpp
    numpy/_core/src/umath/loops_umath_fp.dispatch.c
    numpy/_core/src/umath/loops_unary.dispatch.c
    numpy/_core/src/umath/loops_unary_complex.dispatch.c
    numpy/_core/src/umath/loops_unary_fp.dispatch.c
    numpy/_core/src/umath/loops_unary_fp_le.dispatch.c
    numpy/_core/src/umath/matmul.c
    numpy/_core/src/umath/override.c
    numpy/_core/src/umath/reduction.c
    numpy/_core/src/umath/scalarmath.c
    numpy/_core/src/umath/special_integer_comparisons.cpp
    numpy/_core/src/umath/string_ufuncs.cpp
    numpy/_core/src/umath/stringdtype_ufuncs.cpp
    numpy/_core/src/umath/ufunc_object.c
    numpy/_core/src/umath/ufunc_type_resolution.c
    numpy/_core/src/umath/umathmodule.c
    numpy/_core/src/umath/wrapping_array_method.c
    numpy/f2py/src/fortranobject.c
    numpy/fft/_pocketfft_umath.cpp
    numpy/linalg/lapack_litemodule.c
    numpy/linalg/umath_linalg.cpp
)

IF (CLANG OR CLANG_CL)
    SET(F16C_FLAGS -mf16c)
    SET(FMA3_FLAGS -mfma)
ELSE()
    SET(F16C_FLAGS)
    SET(FMA3_FLAGS)
ENDIF()

IF (ARCH_X86_64)
    CFLAGS(
        -DNPY_HAVE_SSE
        -DNPY_HAVE_SSE2
        -DNPY_HAVE_SSE3
    )

    SRC_C_AVX2(numpy/_core/src/_simd/_simd.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/_simd/_simd.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/_simd/_simd.dispatch.avx512f.c $F16C_FLAGS)
    SRC(numpy/_core/src/_simd/_simd.dispatch.fma3.c $AVX_CFLAGS $F16C_FLAGS $FMA3_FLAGS)
    SRC_C_SSE4(numpy/_core/src/_simd/_simd.dispatch.sse42.c)
    SRC_C_AVX2(numpy/_core/src/multiarray/argfunc.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/multiarray/argfunc.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_SSE4(numpy/_core/src/multiarray/argfunc.dispatch.sse42.c)
    SRC_C_AVX2(numpy/_core/src/npysort/x86_simd_argsort.dispatch.avx2.cpp $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/npysort/x86_simd_argsort.dispatch.avx512_skx.cpp $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/npysort/x86_simd_qsort.dispatch.avx2.cpp $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/npysort/x86_simd_qsort.dispatch.avx512_skx.cpp $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/npysort/x86_simd_qsort_16bit.dispatch.avx512_icl.cpp $F16C_FLAGS -mavx512vbmi2)
    SRC_C_AVX2(numpy/_core/src/umath/_umath_tests.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_SSE4(numpy/_core/src/umath/_umath_tests.dispatch.sse41.c)
    SRC_C_AVX2(numpy/_core/src/umath/loops_arithm_fp.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_arithmetic.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_arithmetic.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_arithmetic.dispatch.avx512f.c $F16C_FLAGS)
    SRC_C_SSE4(numpy/_core/src/umath/loops_arithmetic.dispatch.sse41.c)
    SRC_C_AVX2(numpy/_core/src/umath/loops_autovec.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_comparison.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_comparison.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_comparison.dispatch.avx512f.c $F16C_FLAGS)
    SRC_C_SSE4(numpy/_core/src/umath/loops_comparison.dispatch.sse42.c)
    SRC_C_AVX2(numpy/_core/src/umath/loops_exponent_log.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_exponent_log.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_exponent_log.dispatch.avx512f.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_hyperbolic.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_hyperbolic.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_logical.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_logical.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_minmax.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_minmax.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_trigonometric.dispatch.avx2.cpp $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_trigonometric.dispatch.avx512_skx.cpp $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_umath_fp.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_unary.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_unary.dispatch.avx512_skx.c $F16C_FLAGS)
    SRC_C_AVX2(numpy/_core/src/umath/loops_unary_complex.dispatch.avx2.c $F16C_FLAGS)
    SRC_C_AVX512(numpy/_core/src/umath/loops_unary_complex.dispatch.avx512f.c $F16C_FLAGS)
    SRC_C_SSE4(numpy/_core/src/umath/loops_unary_fp.dispatch.sse41.c)
    SRC_C_SSE4(numpy/_core/src/umath/loops_unary_fp_le.dispatch.sse41.c)
ELSEIF (ARCH_ARM64)
    CFLAGS(
        -DNPY_HAVE_NEON_VFPV4
        -DNPY_HAVE_NEON_FP16
        -DNPY_HAVE_NEON
        -DNPY_HAVE_ASIMD
        -DTOOLCHAIN_MISS_ASM_HWCAP_H
    )

    SRC(numpy/_core/src/highway/hwy/abort.cc)
    SRC(numpy/_core/src/umath/_umath_tests.dispatch.asimdhp.c)
    SRC(numpy/_core/src/npysort/highway_qsort.dispatch.asimd.cpp)
    SRC(numpy/_core/src/npysort/highway_qsort_16bit.dispatch.asimdhp.cpp)
ENDIF()

PY_REGISTER(
    numpy._core._multiarray_tests
    numpy._core._multiarray_umath
    numpy._core._operand_flag_tests
    numpy._core._rational_tests
    numpy._core._simd
    numpy._core._struct_ufunc_tests
    numpy._core._umath_tests
    numpy.fft._pocketfft_umath
    numpy.linalg._umath_linalg
    numpy.linalg.lapack_lite
)

PY_SRCS(
    TOP_LEVEL
    numpy/__config__.py
    numpy/__config__.pyi
    numpy/__init__.py
    numpy/__init__.pyi
    numpy/_array_api_info.py
    numpy/_array_api_info.pyi
    numpy/_configtool.py
    numpy/_configtool.pyi
    numpy/_core/__init__.py
    numpy/_core/__init__.pyi
    numpy/_core/_add_newdocs.py
    numpy/_core/_add_newdocs.pyi
    numpy/_core/_add_newdocs_scalars.py
    numpy/_core/_add_newdocs_scalars.pyi
    numpy/_core/_asarray.py
    numpy/_core/_asarray.pyi
    numpy/_core/_dtype.py
    numpy/_core/_dtype.pyi
    numpy/_core/_dtype_ctypes.py
    numpy/_core/_dtype_ctypes.pyi
    numpy/_core/_exceptions.py
    numpy/_core/_exceptions.pyi
    numpy/_core/_internal.py
    numpy/_core/_internal.pyi
    numpy/_core/_machar.py
    numpy/_core/_machar.pyi
    numpy/_core/_methods.py
    numpy/_core/_methods.pyi
    numpy/_core/_simd.pyi
    numpy/_core/_string_helpers.py
    numpy/_core/_string_helpers.pyi
    numpy/_core/_type_aliases.py
    numpy/_core/_type_aliases.pyi
    numpy/_core/_ufunc_config.py
    numpy/_core/_ufunc_config.pyi
    numpy/_core/arrayprint.py
    numpy/_core/arrayprint.pyi
    numpy/_core/cversions.py
    numpy/_core/defchararray.py
    numpy/_core/defchararray.pyi
    numpy/_core/einsumfunc.py
    numpy/_core/einsumfunc.pyi
    numpy/_core/fromnumeric.py
    numpy/_core/fromnumeric.pyi
    numpy/_core/function_base.py
    numpy/_core/function_base.pyi
    numpy/_core/getlimits.py
    numpy/_core/getlimits.pyi
    numpy/_core/memmap.py
    numpy/_core/memmap.pyi
    numpy/_core/multiarray.py
    numpy/_core/multiarray.pyi
    numpy/_core/numeric.py
    numpy/_core/numeric.pyi
    numpy/_core/numerictypes.py
    numpy/_core/numerictypes.pyi
    numpy/_core/overrides.py
    numpy/_core/overrides.pyi
    numpy/_core/printoptions.py
    numpy/_core/printoptions.pyi
    numpy/_core/records.py
    numpy/_core/records.pyi
    numpy/_core/shape_base.py
    numpy/_core/shape_base.pyi
    numpy/_core/strings.py
    numpy/_core/strings.pyi
    numpy/_core/tests/_natype.py
    numpy/_core/umath.py
    numpy/_core/umath.pyi
    numpy/_distributor_init.py
    numpy/_distributor_init.pyi
    numpy/_expired_attrs_2_0.py
    numpy/_expired_attrs_2_0.pyi
    numpy/_globals.py
    numpy/_globals.pyi
    numpy/_pyinstaller/__init__.py
    numpy/_pyinstaller/__init__.pyi
    numpy/_pyinstaller/hook-numpy.py
    numpy/_pyinstaller/hook-numpy.pyi
    numpy/_pytesttester.py
    numpy/_pytesttester.pyi
    numpy/_typing/__init__.py
    numpy/_typing/_add_docstring.py
    numpy/_typing/_array_like.py
    numpy/_typing/_callable.pyi
    numpy/_typing/_char_codes.py
    numpy/_typing/_dtype_like.py
    numpy/_typing/_extended_precision.py
    numpy/_typing/_nbit.py
    numpy/_typing/_nbit_base.py
    numpy/_typing/_nested_sequence.py
    numpy/_typing/_scalars.py
    numpy/_typing/_shape.py
    numpy/_typing/_ufunc.py
    numpy/_typing/_ufunc.pyi
    numpy/_utils/__init__.py
    numpy/_utils/__init__.pyi
    numpy/_utils/_convertions.py
    numpy/_utils/_convertions.pyi
    numpy/_utils/_inspect.py
    numpy/_utils/_inspect.pyi
    numpy/_utils/_pep440.py
    numpy/_utils/_pep440.pyi
    numpy/char/__init__.py
    numpy/char/__init__.pyi
    numpy/compat/__init__.py
    numpy/compat/py3k.py
    numpy/core/__init__.py
    numpy/core/__init__.pyi
    numpy/core/_dtype.py
    numpy/core/_dtype.pyi
    numpy/core/_dtype_ctypes.py
    numpy/core/_dtype_ctypes.pyi
    numpy/core/_internal.py
    numpy/core/_multiarray_umath.py
    numpy/core/_utils.py
    numpy/core/arrayprint.py
    numpy/core/defchararray.py
    numpy/core/einsumfunc.py
    numpy/core/fromnumeric.py
    numpy/core/function_base.py
    numpy/core/getlimits.py
    numpy/core/multiarray.py
    numpy/core/numeric.py
    numpy/core/numerictypes.py
    numpy/core/overrides.py
    numpy/core/overrides.pyi
    numpy/core/records.py
    numpy/core/shape_base.py
    numpy/core/umath.py
    numpy/ctypeslib.py
    numpy/ctypeslib.pyi
    numpy/distutils/__config__.py
    numpy/distutils/__init__.py
    numpy/distutils/__init__.pyi
    numpy/distutils/_shell_utils.py
    numpy/distutils/armccompiler.py
    numpy/distutils/ccompiler.py
    numpy/distutils/ccompiler_opt.py
    numpy/distutils/command/__init__.py
    numpy/distutils/command/autodist.py
    numpy/distutils/command/bdist_rpm.py
    numpy/distutils/command/build.py
    numpy/distutils/command/build_clib.py
    numpy/distutils/command/build_ext.py
    numpy/distutils/command/build_py.py
    numpy/distutils/command/build_scripts.py
    numpy/distutils/command/build_src.py
    numpy/distutils/command/config.py
    numpy/distutils/command/config_compiler.py
    numpy/distutils/command/develop.py
    numpy/distutils/command/egg_info.py
    numpy/distutils/command/install.py
    numpy/distutils/command/install_clib.py
    numpy/distutils/command/install_data.py
    numpy/distutils/command/install_headers.py
    numpy/distutils/command/sdist.py
    numpy/distutils/conv_template.py
    numpy/distutils/core.py
    numpy/distutils/cpuinfo.py
    numpy/distutils/exec_command.py
    numpy/distutils/extension.py
    numpy/distutils/fcompiler/__init__.py
    numpy/distutils/fcompiler/absoft.py
    numpy/distutils/fcompiler/arm.py
    numpy/distutils/fcompiler/compaq.py
    numpy/distutils/fcompiler/environment.py
    numpy/distutils/fcompiler/fujitsu.py
    numpy/distutils/fcompiler/g95.py
    numpy/distutils/fcompiler/gnu.py
    numpy/distutils/fcompiler/hpux.py
    numpy/distutils/fcompiler/ibm.py
    numpy/distutils/fcompiler/intel.py
    numpy/distutils/fcompiler/lahey.py
    numpy/distutils/fcompiler/mips.py
    numpy/distutils/fcompiler/nag.py
    numpy/distutils/fcompiler/none.py
    numpy/distutils/fcompiler/nv.py
    numpy/distutils/fcompiler/pathf95.py
    numpy/distutils/fcompiler/pg.py
    numpy/distutils/fcompiler/sun.py
    numpy/distutils/fcompiler/vast.py
    numpy/distutils/from_template.py
    numpy/distutils/fujitsuccompiler.py
    numpy/distutils/intelccompiler.py
    numpy/distutils/lib2def.py
    numpy/distutils/line_endings.py
    numpy/distutils/log.py
    numpy/distutils/mingw32ccompiler.py
    numpy/distutils/misc_util.py
    numpy/distutils/msvc9compiler.py
    numpy/distutils/msvccompiler.py
    numpy/distutils/npy_pkg_config.py
    numpy/distutils/numpy_distribution.py
    numpy/distutils/pathccompiler.py
    numpy/distutils/system_info.py
    numpy/distutils/unixccompiler.py
    numpy/doc/ufuncs.py
    numpy/dtypes.py
    numpy/dtypes.pyi
    numpy/exceptions.py
    numpy/exceptions.pyi
    numpy/f2py/__init__.py
    numpy/f2py/__init__.pyi
    numpy/f2py/__main__.py
    numpy/f2py/__version__.py
    numpy/f2py/_backends/__init__.py
    numpy/f2py/_backends/_backend.py
    numpy/f2py/_backends/_distutils.py
    numpy/f2py/_backends/_meson.py
    numpy/f2py/_isocbind.py
    numpy/f2py/_src_pyf.py
    numpy/f2py/auxfuncs.py
    numpy/f2py/capi_maps.py
    numpy/f2py/cb_rules.py
    numpy/f2py/cfuncs.py
    numpy/f2py/common_rules.py
    numpy/f2py/crackfortran.py
    numpy/f2py/diagnose.py
    numpy/f2py/f2py2e.py
    numpy/f2py/f90mod_rules.py
    numpy/f2py/func2subr.py
    numpy/f2py/rules.py
    numpy/f2py/symbolic.py
    numpy/f2py/use_rules.py
    numpy/fft/__init__.py
    numpy/fft/__init__.pyi
    numpy/fft/_helper.py
    numpy/fft/_helper.pyi
    numpy/fft/_pocketfft.py
    numpy/fft/_pocketfft.pyi
    numpy/fft/helper.py
    numpy/fft/helper.pyi
    numpy/lib/__init__.py
    numpy/lib/__init__.pyi
    numpy/lib/_array_utils_impl.py
    numpy/lib/_array_utils_impl.pyi
    numpy/lib/_arraypad_impl.py
    numpy/lib/_arraypad_impl.pyi
    numpy/lib/_arraysetops_impl.py
    numpy/lib/_arraysetops_impl.pyi
    numpy/lib/_arrayterator_impl.py
    numpy/lib/_arrayterator_impl.pyi
    numpy/lib/_datasource.py
    numpy/lib/_datasource.pyi
    numpy/lib/_function_base_impl.py
    numpy/lib/_function_base_impl.pyi
    numpy/lib/_histograms_impl.py
    numpy/lib/_histograms_impl.pyi
    numpy/lib/_index_tricks_impl.py
    numpy/lib/_index_tricks_impl.pyi
    numpy/lib/_iotools.py
    numpy/lib/_iotools.pyi
    numpy/lib/_nanfunctions_impl.py
    numpy/lib/_nanfunctions_impl.pyi
    numpy/lib/_npyio_impl.py
    numpy/lib/_npyio_impl.pyi
    numpy/lib/_polynomial_impl.py
    numpy/lib/_polynomial_impl.pyi
    numpy/lib/_scimath_impl.py
    numpy/lib/_scimath_impl.pyi
    numpy/lib/_shape_base_impl.py
    numpy/lib/_shape_base_impl.pyi
    numpy/lib/_stride_tricks_impl.py
    numpy/lib/_stride_tricks_impl.pyi
    numpy/lib/_twodim_base_impl.py
    numpy/lib/_twodim_base_impl.pyi
    numpy/lib/_type_check_impl.py
    numpy/lib/_type_check_impl.pyi
    numpy/lib/_ufunclike_impl.py
    numpy/lib/_ufunclike_impl.pyi
    numpy/lib/_user_array_impl.py
    numpy/lib/_user_array_impl.pyi
    numpy/lib/_utils_impl.py
    numpy/lib/_utils_impl.pyi
    numpy/lib/_version.py
    numpy/lib/_version.pyi
    numpy/lib/array_utils.py
    numpy/lib/array_utils.pyi
    numpy/lib/format.py
    numpy/lib/format.pyi
    numpy/lib/introspect.py
    numpy/lib/introspect.pyi
    numpy/lib/mixins.py
    numpy/lib/mixins.pyi
    numpy/lib/npyio.py
    numpy/lib/npyio.pyi
    numpy/lib/recfunctions.py
    numpy/lib/recfunctions.pyi
    numpy/lib/scimath.py
    numpy/lib/scimath.pyi
    numpy/lib/stride_tricks.py
    numpy/lib/stride_tricks.pyi
    numpy/lib/user_array.py
    numpy/lib/user_array.pyi
    numpy/linalg/__init__.py
    numpy/linalg/__init__.pyi
    numpy/linalg/_linalg.py
    numpy/linalg/_linalg.pyi
    numpy/linalg/_umath_linalg.pyi
    numpy/linalg/lapack_lite.pyi
    numpy/linalg/linalg.py
    numpy/linalg/linalg.pyi
    numpy/ma/__init__.py
    numpy/ma/__init__.pyi
    numpy/ma/core.py
    numpy/ma/core.pyi
    numpy/ma/extras.py
    numpy/ma/extras.pyi
    numpy/ma/mrecords.py
    numpy/ma/mrecords.pyi
    numpy/ma/testutils.py
    numpy/ma/timer_comparison.py
    numpy/matlib.py
    numpy/matlib.pyi
    numpy/matrixlib/__init__.py
    numpy/matrixlib/__init__.pyi
    numpy/matrixlib/defmatrix.py
    numpy/matrixlib/defmatrix.pyi
    numpy/polynomial/__init__.py
    numpy/polynomial/__init__.pyi
    numpy/polynomial/_polybase.py
    numpy/polynomial/_polybase.pyi
    numpy/polynomial/_polytypes.pyi
    numpy/polynomial/chebyshev.py
    numpy/polynomial/chebyshev.pyi
    numpy/polynomial/hermite.py
    numpy/polynomial/hermite.pyi
    numpy/polynomial/hermite_e.py
    numpy/polynomial/hermite_e.pyi
    numpy/polynomial/laguerre.py
    numpy/polynomial/laguerre.pyi
    numpy/polynomial/legendre.py
    numpy/polynomial/legendre.pyi
    numpy/polynomial/polynomial.py
    numpy/polynomial/polynomial.pyi
    numpy/polynomial/polyutils.py
    numpy/polynomial/polyutils.pyi
    numpy/random/__init__.py
    numpy/random/__init__.pyi
    numpy/random/_generator.pyi
    numpy/random/_mt19937.pyi
    numpy/random/_pcg64.pyi
    numpy/random/_philox.pyi
    numpy/random/_pickle.py
    numpy/random/_pickle.pyi
    numpy/random/_sfc64.pyi
    numpy/random/bit_generator.pyi
    numpy/random/mtrand.pyi
    numpy/rec/__init__.py
    numpy/rec/__init__.pyi
    numpy/strings/__init__.py
    numpy/strings/__init__.pyi
    numpy/testing/__init__.py
    numpy/testing/__init__.pyi
    numpy/testing/_private/__init__.py
    numpy/testing/_private/__init__.pyi
    numpy/testing/_private/extbuild.py
    numpy/testing/_private/extbuild.pyi
    numpy/testing/_private/utils.py
    numpy/testing/_private/utils.pyi
    numpy/testing/overrides.py
    numpy/testing/overrides.pyi
    numpy/testing/print_coercion_tables.py
    numpy/testing/print_coercion_tables.pyi
    numpy/typing/__init__.py
    numpy/typing/mypy_plugin.py
    numpy/version.py
    numpy/version.pyi
)

RESOURCE_FILES(
    PREFIX contrib/python/numpy/py3/
    .dist-info/METADATA
    .dist-info/entry_points.txt
    .dist-info/top_level.txt
    numpy/py.typed
)

END()

RECURSE(
    numpy/random
)

RECURSE_FOR_TESTS(
    tests
)

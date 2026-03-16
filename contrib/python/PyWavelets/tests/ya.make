PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/pytest
    contrib/python/numpy
    contrib/python/future
    contrib/python/PyWavelets
)

DATA(
    arcadia/contrib/python/PyWavelets/pywt/tests/data
)

SRCDIR(contrib/python/PyWavelets/pywt/tests)

TEST_SRCS(
    test__pywt.py
    test_concurrent.py
    test_cwt_wavelets.py
    test_data.py
    test_deprecations.py
    test_doc.py
    test_dwt_idwt.py
    test_functions.py
    test_mra.py
    test_matlab_compatibility.py
    test_matlab_compatibility_cwt.py
    test_modes.py
    test_multidim.py
    test_multilevel.py
    test_perfect_reconstruction.py
    test_swt.py
    test_thresholding.py
    test_wavelet.py
    test_wp.py
    test_wp2d.py
    test_wpnd.py
)

NO_LINT()

END()

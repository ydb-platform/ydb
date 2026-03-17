PY3TEST()

PEERDIR(
    contrib/python/h3
    contrib/python/numpy
    contrib/python/packaging
)

TEST_SRCS(
    test_api_bindings_match.py
    test_basic_int.py
    test_basic_str.py
    test_cells_and_edges.py
    test_collection_inputs.py
    test_h3.py
    test_length_area.py
    test_memview_int.py
    test_numpy_int.py
    test_polyfill.py
    test_to_multipoly.py
    test_unstable_vect.py
)

NO_LINT()

END()

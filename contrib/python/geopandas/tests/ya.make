PY3TEST()

FORK_TESTS()

PEERDIR(
    contrib/python/geopandas
)

NO_LINT()

SRCDIR(contrib/python/geopandas)

PY_SRCS(
    TOP_LEVEL
    geopandas/tests/util.py
)

TEST_SRCS(
    geopandas/conftest.py
    # geopandas/io/tests/generate_legacy_storage_files.py  # not a test
    geopandas/io/tests/__init__.py
    # geopandas/io/tests/test_arrow.py  # no data in package for tests
    geopandas/io/tests/test_file_geom_types_drivers.py
    geopandas/io/tests/test_file.py
    geopandas/io/tests/test_geoarrow.py
    geopandas/io/tests/test_infer_schema.py
    # geopandas/io/tests/test_pickle.py  # work with file system
    # geopandas/io/tests/test_sql.py  # need PG base
    geopandas/testing.py
    geopandas/tests/__init__.py
    # geopandas/tests/test_api.py  # run subprocess `python -c some_code`
    geopandas/tests/test_array.py
    geopandas/tests/test_compat.py
    geopandas/tests/test_config.py
    geopandas/tests/test_crs.py
    geopandas/tests/test_datasets.py
    # geopandas/tests/test_decorator.py  # idkw
    geopandas/tests/test_dissolve.py
    geopandas/tests/test_explore.py
    # geopandas/tests/test_extension_array.py  # need pandas.tests
    geopandas/tests/test_geocode.py
    geopandas/tests/test_geodataframe.py
    # geopandas/tests/test_geom_methods.py  # need geos 3.11
    geopandas/tests/test_geoseries.py
    geopandas/tests/test_merge.py
    geopandas/tests/test_op_output_types.py
    # geopandas/tests/test_overlay.py  # no data in package for tests
    # geopandas/tests/test_pandas_methods.py  # test unexpectedly passed
    geopandas/tests/test_plotting.py
    geopandas/tests/test_show_versions.py
    geopandas/tests/test_sindex.py
    geopandas/tests/test_testing.py
    geopandas/tests/test_types.py
    geopandas/tools/tests/__init__.py
    geopandas/tools/tests/test_clip.py
    geopandas/tools/tests/test_hilbert_curve.py
    geopandas/tools/tests/test_random.py
    geopandas/tools/tests/test_sjoin.py
    geopandas/tools/tests/test_tools.py
)

DATA(
    arcadia/contrib/python/geopandas/geopandas/tests/data
)

END()

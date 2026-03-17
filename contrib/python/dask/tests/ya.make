PY3TEST()

SIZE(MEDIUM)

FORK_TESTS()

PEERDIR(
    contrib/python/dask
    contrib/python/pytest-localserver
    contrib/python/pytest-mock
    contrib/python/ipython
    contrib/python/scikit-image
    contrib/python/scipy
    contrib/python/cytoolz
    contrib/python/cloudpickle
    contrib/python/partd
    contrib/python/pandas
    contrib/python/psutil
    contrib/python/PyYAML
    contrib/python/xarray
    contrib/python/fastavro
    contrib/python/zarr
)

SRCDIR(contrib/python/dask)

TEST_SRCS(
    dask/array/tests/__init__.py
    dask/array/tests/test_array_core.py
    dask/array/tests/test_array_function.py
    dask/array/tests/test_array_utils.py
    dask/array/tests/test_atop.py
    dask/array/tests/test_chunk.py
    dask/array/tests/test_creation.py
    # dask/array/tests/test_cupy_core.py
    # dask/array/tests/test_cupy_creation.py
    # dask/array/tests/test_cupy_gufunc.py
    # dask/array/tests/test_cupy_linalg.py
    # dask/array/tests/test_cupy_overlap.py
    # dask/array/tests/test_cupy_percentile.py
    # dask/array/tests/test_cupy_random.py
    # dask/array/tests/test_cupy_reductions.py
    # dask/array/tests/test_cupy_routines.py
    # dask/array/tests/test_cupy_slicing.py
    # dask/array/tests/test_cupy_sparse.py
    dask/array/tests/test_dispatch.py
    dask/array/tests/test_fft.py
    dask/array/tests/test_gufunc.py
    dask/array/tests/test_image.py
    dask/array/tests/test_linalg.py
    dask/array/tests/test_masked.py
    dask/array/tests/test_numpy_compat.py
    dask/array/tests/test_optimization.py
    dask/array/tests/test_overlap.py
    dask/array/tests/test_percentiles.py
    dask/array/tests/test_random.py
    dask/array/tests/test_rechunk.py
    dask/array/tests/test_reductions.py
    dask/array/tests/test_reshape.py
    dask/array/tests/test_routines.py
    dask/array/tests/test_slicing.py
    dask/array/tests/test_sparse.py
    dask/array/tests/test_stats.py
    dask/array/tests/test_svg.py
    dask/array/tests/test_testing.py
    dask/array/tests/test_ufunc.py
    dask/array/tests/test_wrap.py
    dask/array/tests/test_xarray.py
    dask/bag/tests/__init__.py
    dask/bag/tests/test_avro.py
    dask/bag/tests/test_bag.py
    dask/bag/tests/test_random.py
    dask/bag/tests/test_text.py
    dask/bytes/tests/__init__.py
    dask/bytes/tests/test_bytes_utils.py
    dask/bytes/tests/test_compression.py
    dask/bytes/tests/test_http.py
    dask/bytes/tests/test_local.py
    dask/bytes/tests/test_s3.py
    # dask/dataframe/io/tests/__init__.py
    # dask/dataframe/io/tests/test_csv.py
    # dask/dataframe/io/tests/test_demo.py
    # dask/dataframe/io/tests/test_hdf.py
    # dask/dataframe/io/tests/test_io.py
    # dask/dataframe/io/tests/test_json.py
    # dask/dataframe/io/tests/test_orc.py
    # dask/dataframe/io/tests/test_parquet.py
    # dask/dataframe/io/tests/test_sql.py
    # dask/dataframe/tests/__init__.py
    # dask/dataframe/tests/test_accessors.py
    # dask/dataframe/tests/test_arithmetics_reduction.py
    # dask/dataframe/tests/test_boolean.py
    # dask/dataframe/tests/test_categorical.py
    # dask/dataframe/tests/test_dataframe.py
    # dask/dataframe/tests/test_extensions.py
    # dask/dataframe/tests/test_format.py
    # dask/dataframe/tests/test_groupby.py
    # dask/dataframe/tests/test_hashing.py
    # dask/dataframe/tests/test_hyperloglog.py
    # dask/dataframe/tests/test_indexing.py
    # dask/dataframe/tests/test_merge_column_and_index.py
    # dask/dataframe/tests/test_methods.py
    # dask/dataframe/tests/test_multi.py
    # dask/dataframe/tests/test_numeric.py
    # dask/dataframe/tests/test_optimize_dataframe.py
    # dask/dataframe/tests/test_pyarrow.py
    # dask/dataframe/tests/test_pyarrow_compat.py
    # dask/dataframe/tests/test_reshape.py
    # dask/dataframe/tests/test_rolling.py
    # dask/dataframe/tests/test_shuffle.py
    # dask/dataframe/tests/test_ufunc.py
    # dask/dataframe/tests/test_utils_dataframe.py
    # dask/dataframe/tseries/tests/__init__.py
    # dask/dataframe/tseries/tests/test_resample.py
    dask/diagnostics/tests/__init__.py
    dask/diagnostics/tests/test_profiler.py
    dask/diagnostics/tests/test_progress.py
    dask/tests/__init__.py
    dask/tests/test_backends.py
    dask/tests/test_base.py
    dask/tests/test_cache.py
    dask/tests/test_callbacks.py
    dask/tests/test_ci.py
    dask/tests/test_cli.py
    dask/tests/test_config.py
    dask/tests/test_context.py
    dask/tests/test_core.py
    dask/tests/test_datasets.py
    dask/tests/test_delayed.py
    dask/tests/test_distributed.py
    # dask/tests/test_docs.py
    dask/tests/test_dot.py
    dask/tests/test_graph_manipulation.py
    dask/tests/test_hashing.py
    dask/tests/test_highgraph.py
    dask/tests/test_layers.py
    dask/tests/test_local.py
    dask/tests/test_ml.py
    dask/tests/test_multiprocessing.py
    dask/tests/test_optimization.py
    dask/tests/test_order.py
    dask/tests/test_rewrite.py
    dask/tests/test_sizeof.py
    dask/tests/test_spark_compat.py
    dask/tests/test_system.py
    dask/tests/test_threaded.py
    # dask/tests/test_tokenize.py
    dask/tests/test_traceback.py
    dask/tests/test_typing.py
    dask/tests/test_utils.py
    dask/tests/test_utils_test.py
    dask/tests/warning_aliases.py
    dask/widgets/tests/test_widgets.py
)

NO_LINT()

END()

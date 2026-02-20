PY3_PROGRAM(ydb-skip_indexes_heaven-dataset_generator)

PY_MAIN(ydb.tests.functional.skip_indexes_heaven.generate.main)

PY_SRCS(
    main.py
)

PEERDIR(
    contrib/python/pyarrow
    contrib/python/numpy
    
)

END()
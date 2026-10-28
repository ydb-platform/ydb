PACKAGE()
RUN_PYTHON3(${ARCADIA_ROOT}/tools/ya_archive_gil_repro/task.py before ${BINDIR}/before.txt ${PROBE_OUT} OUT before.txt)
RUN_PYTHON3(${ARCADIA_ROOT}/tools/ya_archive_gil_repro/task.py after ${BINDIR}/after.txt ${BINDIR}/before.txt IN before.txt OUT after.txt)
END()

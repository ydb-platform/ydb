# ya make: archive extraction holds GIL

Three real build tasks: extract a 16 GiB zstd entry, finish a short task during
extraction, then start its dependent task. The dependent task stalls until
extraction ends. The control calls the same native function through
`ctypes.CFUNCTYPE`, releasing GIL. No artificial delay in extraction.

From the repository root (Linux x86_64, unstripped non-PIE ya-bin, `nm`, ~32 GiB free):

```bash
Y_PYTHON_ENTRY_POINT=:main ./ya tools/ya_archive_gil_repro/repro.py
```

Prints extraction time and the delay before the dependent task starts.
Use `--mib 1024` for a smaller example. Logs and outputs stay in the printed
temporary directory. Build results are not cached; CI is unchanged.

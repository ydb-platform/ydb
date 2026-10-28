# ya make: archive extraction holds GIL

Ordinary `ya.make` targets: an `EXECTEST` writes 16 GiB of logs; two dependent
`RUN_PYTHON3` tasks measure the scheduling delay during result extraction.
No custom graph. Linux, `/usr/bin/python3`, ~48 GiB free space for both runs.

Measured next-task delay: **12.77 s**, or **49 ms** with GIL released.

```bash
out=$(mktemp -d)
./ya make --build relwithdebinfo -tA --build-all tools/ya_archive_gil_repro \
  --no-dir-outputs --no-src-links --no-bazel-remote-store \
  --output "$out" -DPROBE_OUT="$out"
cat "$out/tools/ya_archive_gil_repro/chain/after.txt"
```

For the control, use a fresh `out` and replace `./ya make` with
`Y_PYTHON_ENTRY_POINT=:main ./ya tools/ya_archive_gil_repro/release_gil.py make`.
It calls the same native function through `ctypes.CFUNCTYPE`, releasing GIL
(requires an unstripped non-PIE x86_64 ya-bin and `nm`).
Add `-DPROBE_MIB=1024` for a smaller run.

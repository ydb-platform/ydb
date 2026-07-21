# YDB Z3 command-line solver

This directory vendors the upstream `z3-4.16.0` release at commit
`ddb49568d3520e99799e364fb22f35fc67d887b1` from
<https://github.com/Z3Prover/z3>. The release is licensed under MIT; the
unaltered upstream notice is in `LICENSE.txt`. The tag archive used for the
import is 6,136,396 bytes with SHA-256
`c68c3e5e4810b16126b8cb4c47eee85c1ac3e24a81914c8e371b40de9dd33ac7`.

YDB builds only the standalone `z3` program. There is deliberately no Z3
library target, so production targets such as `ydbd` cannot acquire a Z3 link
dependency. The new-RBO bounded-equivalence tests declare the program through
`DEPENDS` and resolve its build output with the test framework's binary-path
API.

Upstream uses Python during configuration to generate parameter headers, API
logging sources, and tactic-registration sources. Those platform-independent
outputs are committed under `generated/`; normal YDB builds do not execute an
ambient Python generator. `generated/FILES.txt` records the complete generated
inventory. They were produced by the same legacy generator that defines the
source graph below. To refresh them, unpack the pinned archive into a
disposable directory and run, from that directory:

```text
python3 scripts/mk_make.py --staticbin
```

The command writes generated sources into that copy's `src/` tree (and creates
`build/`). Copy only the paths listed in this repository's
`generated/FILES.txt` from the disposable `src/` tree to `generated/src/`, then
recompute the inventory count, byte count, and aggregate hash recorded at the
top of `generated/FILES.txt`. CMake is not interchangeable here: its version
header differs byte-for-byte even for the same release.

The explicit source list in `ya.make` is the upstream `mk_make.py --staticbin`
shell graph with tests, language bindings, and `api_dll` excluded. It contains
831 translation units; after normalizing the five `generated/src/...` paths to
`src/...`, its sorted newline-terminated SHA-256 is
`a2a5359feeca6c5dffc71674a259196d3bfe69405388d880388271642a1eecb5`.

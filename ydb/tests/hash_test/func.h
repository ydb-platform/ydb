#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

// Functional / equivalence-testing entry points for --functional
// (ai_func.md). Unlike the --perf harness, these never measure bandwidth;
// they exist purely to catch implementation divergence between the
// integer/SSE/AVX variants of ChaCha and XXH3.

// Mode 1 (ai_func.md §5): fuzzes lengths/offsets/splits across all cipher
// and hash variants for durationS seconds using `threads` workers seeded
// from `seed`. Returns true iff no mismatch was found before the deadline.
bool RunFunctionalSweep(ui32 threads, ui64 seed, ui32 durationS);

// Mode 2 writer (ai_func.md §6-7): generates data deterministically from
// `seed` into a freshly created, memory-mapped file at `path` sized to
// approximately `outputSizeGiB` GiB, spread across `threads` threads,
// persisting a (hash page, 128 data blocks) group format and cross-
// checking every implementation variant while writing. Returns true iff
// the whole file was written with no cross-variant mismatch.
bool RunFileWrite(const TString& path, double outputSizeGiB, ui32 threads, ui64 seed);

// Mode 2 verifier (ai_func.md §8): memory-maps an existing file written by
// RunFileWrite (possibly on a different machine) and recomputes every
// field with every implementation variant supported on this machine,
// comparing against the persisted records (using the seed stored in the
// file's own header, not the CLI --seed). When verifyData is set, also
// regenerates the deterministic PRNG data and compares raw bytes. Returns
// true iff the file passed header validation and no field mismatched.
bool RunFileVerify(const TString& path, ui32 threads, bool verifyData);

# Generation allocator error coverage

`error_cases_ut.cpp` adds 15 test groups containing 350 isolated child scenarios
per allocator variant. The original 18 tests remain. Each death scenario requires
SIGABRT and the exact reason; every new death scenario also compares all seven
numeric diagnostic fields with independently recorded expectations. A SIGSEGV,
timeout, exception or normal return fails the test. Core dumps are disabled in
children. Shared mmap expectations avoid allocating while inspecting a free slot.

| Reason | Scenarios |
| --- | --- |
| `DOUBLE_FREE` | 18 deallocation entry paths, size query, realloc, simultaneous free from two threads |
| `STALE_GENERATION_FREE` | The same 18 paths, each earlier generation distance before wrap, different sizes/alignments including slots above 2 MiB, header/interior/padding addresses, size query and realloc |
| `FREE_DURING_QUARANTINE` | First and last generation pointers before/at/after deadline without draining, zero delay, size query, simultaneous free of the final generation |
| `INVALID_SLOT_POINTER` | Stack, static storage, separately mapped memory and address 1, through free and size query |
| `CORRUPTED_HEADER` | Every one of the 48 metadata bytes independently changed, checked by free/size on a live slot, allocation from the free list, quarantine drain; corrupted quarantine tail when appending another slot |
| `WRITE_AFTER_FREE` | First/last checked bytes and inner edges of the two poison zones, sizes 1/64/65/128/129/256, overlapping and disjoint zones, delays 0 and 100 injected clock ticks |

The 18 deallocation paths cover free/cfree, sized/aligned-sized free, sdallocx
with default/explicit alignment, scalar/array delete and their sized, aligned,
sized-aligned, nothrow and aligned-nothrow forms. Tests call the allocator's
explicit C entry points so compiler optimization of invalid C++ delete expressions
does not remove the operation under test.

Positive controls document limits: intact poison, read after free, a write in the
unchecked middle, restored poison, disabled poison and a pointer whose address
matches a new allocation after a full generation cycle with zero delay. These are
expected to succeed, not claimed as detected errors. A zero delay still uses the
quarantine queue and drains on the next allocation. The original tests additionally
cover deadline expiry, budget exhaustion, normal APIs, cross-thread free and fork.

Validation on 2026-09-17: build-host-004, Linux x86-64, relwithdebinfo,
**66 GOOD** (33 with `TCMALLOC_TC`, 33 with `TCMALLOC_256K`). Production allocator
sources were unchanged. This is focused functional coverage, not an exhaustive
proof of race freedom or arbitrary memory-corruption detection.

```sh
ssh_ya_pool --host build-host-004 ya make --build relwithdebinfo -tA \
  library/cpp/malloc/tcmalloc/free_check_ut \
  library/cpp/malloc/tcmalloc/free_check_ut_percpu
```

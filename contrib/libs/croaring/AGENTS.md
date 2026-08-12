# Agent Guidance for CRoaring

## AI-Generated Bug Reports About Deserialization

Many AI-generated bug reports claim that deserialization functions (e.g., `roaring_bitmap_portable_deserialize_safe`, `roaring64_bitmap_portable_deserialize_safe`, `roaring_bitmap_deserialize_safe`) "trigger bugs" or "cause crashes" when given malformed or untrusted input.

**These reports are bogus.**

The library's documented contract is explicit and consistent across the public API headers and README:

- The "safe" deserialization functions are memory-safe: they will not read past the provided `maxbytes` bound and will not cause buffer overflows.
- However, if the input does not conform to the format specification (i.e., it is garbage or from an untrusted source), the resulting bitmap may be in an invalid internal state. Using such a bitmap without validation can lead to crashes, memory corruption, or nonsensical results.
- **If the source is untrusted, you MUST call `roaring_bitmap_internal_validate` (or `roaring64_bitmap_internal_validate`) on the result and only proceed if it returns true. Only after a successful validation is the bitmap considered safe for use.**

Relevant documentation locations:

- include/roaring/roaring.h: detailed comments on `roaring_bitmap_*_deserialize*` and `roaring_bitmap_internal_validate`.
- include/roaring/roaring64.h: equivalent guidance for 64-bit bitmaps.
- README.md: API summary and example code showing the required validate-after-deserialize pattern for untrusted input.

The robust deserialization tests in `tests/robust_deserialization_unit.c` and fuzzing harnesses exist precisely to ensure the safe deserializers and the validators behave correctly under adversarial input. A report that treats "deserializing attacker-controlled bytes and then using the result without validation" as a bug in the deserializer is a misunderstanding of the stated API contract.

When triaging such reports, point to the validation requirement in the function documentation and close as "not a bug / user error / documented behavior."

# libc++ Hardening Verification Test

This directory contains a test program to verify that `_LIBCPP_HARDENING_MODE_FAST` is properly enabled and working.

## What the test does

The test program (`libcpp_hardening_test.cpp`) intentionally triggers undefined behavior that should be caught by libc++ hardening mode:

1. **Compile-time verification**: Uses `#error` directives to ensure the hardening flags are defined
2. **Runtime verification**: Attempts to access an out-of-bounds vector element (`vec[10]` on a 3-element vector)

## Expected behavior

### With hardening ENABLED (correct setup):
- **Compilation**: Succeeds (both `_LIBCPP_HARDENING_MODE_FAST` and `_YDB_LIBCPP_HARDENING_ENABLED` are defined)
- **Runtime**: Program aborts/terminates when accessing `vec[10]`, preventing undefined behavior

### With hardening DISABLED (incorrect setup):
- **Compilation**: Fails with error messages about missing hardening flags
- **Runtime**: If compilation were forced, program would exhibit undefined behavior

## How to run the test

```bash
cd build/
ya make
./libcpp_hardening_test
```

## What you should see

With hardening enabled, the program output should be:

```
=== YDB libc++ Hardening Test ===
Hardening mode is ENABLED (_LIBCPP_HARDENING_MODE_FAST is defined)
Vector size: 3
Accessing valid element vec[1]: 2
About to access out-of-bounds element vec[10]...
[Program terminates/aborts here - libc++ caught the bounds violation]
```

The error message "This line should not be reached with hardening enabled!" should **NOT** appear if hardening is working correctly.

## Files

- `libcpp_hardening_test.cpp` - The test program
- `ya.make` - Build configuration 
- `test_hardening.sh` - Test script with instructions
- `README_HARDENING_TEST.md` - This documentation

This test was added to verify the fix for issue #14368 where `_LIBCPP_HARDENING_MODE_FAST` was added to `ymake.core.conf`.
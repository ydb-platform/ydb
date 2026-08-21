# Unit Test Context Helpers

This library provides helpers for passing test context through helper functions in unit tests.

## Problem

When using helper functions in unit tests that contain assertions, if an assertion fails, the error message usually points to the line inside the helper function. This makes it difficult to know which call to the helper function caused the failure, especially if the helper is called multiple times.

## Solution

`TTestContext` captures the source location of the call site. By passing `TTestContext` to your helper functions and using `CTX_UNIT_*` macros, you can report errors with the location of the call site.

## Usage

1. Include the header:
   ```cpp
   #include <ydb/library/ut/ut.h>
   ```

2. Define your helper function to accept `TTestContext` as a default argument:
   ```cpp
   void MyHelper(int expected, int actual, NYdb::NUt::TTestContext testCtx = NYdb::NUt::TTestContext()) {
   ```

3. Call the helper as usual:
   ```cpp
   Y_UNIT_TEST(MyTest) {
       MyHelper(1, 1); // Success
       MyHelper(1, 2); // Failure reported at this line
   }
   ```

## Future Improvements

After `library/cpp/testing/unittest` extension we won't need any custom `CTX_UNIT` redefinition (we just use customization point and replace original `UNIT_FAIL_IMPL` with context aware version).

## Macros

- `CTX_UNIT_ASSERT(condition)`
- `CTX_UNIT_ASSERT_C(condition, message)`
- `CTX_UNIT_ASSERT_VALUES_EQUAL(a, b)`
- `CTX_UNIT_ASSERT_VALUES_EQUAL_C(a, b, message)`
- `CTX_UNIT_ASSERT_VALUES_UNEQUAL(a, b)`
- `CTX_UNIT_ASSERT_VALUES_UNEQUAL_C(a, b, message)`

# YDB Testing Helpers

This directory contains helper macros for YDB unit tests.

## Y_UNIT_TEST_TWIN

Similar to Y_UNIT_TEST, but runs the test twice - once with a boolean template parameter set to `true`, and once set to `false`. The test can access the parameter value and branch accordingly.

### Usage

```cpp
Y_UNIT_TEST_SUITE(MySuite) {
    Y_UNIT_TEST_TWIN(TestWithFeatureFlag, UseNewAlgorithm) {
        if (UseNewAlgorithm) {
            // Test with new algorithm
            UNIT_ASSERT(newAlgorithmResult());
        } else {
            // Test with old algorithm
            UNIT_ASSERT(oldAlgorithmResult());
        }
    }
}
```

This creates two test instances:
- `TestWithFeatureFlag+UseNewAlgorithm` (with `UseNewAlgorithm=true`)
- `TestWithFeatureFlag-UseNewAlgorithm` (with `UseNewAlgorithm=false`)

## Y_UNIT_TEST_SUITE_TWIN

Runs an entire test suite twice - once with a boolean flag set to `true`, and once set to `false`. All tests in the suite can access the flag value via `TCurrentTest::FlagName`.

### Usage

```cpp
// Define test body as a macro
#define MY_SUITE_TESTS(FlagName) \
    Y_UNIT_TEST(TestFeatureBehavior) { \
        if (TCurrentTest::FlagName) { \
            /* Test with feature enabled */ \
            UNIT_ASSERT(newFeatureWorks()); \
        } else { \
            /* Test with feature disabled */ \
            UNIT_ASSERT(oldBehaviorWorks()); \
        } \
    } \
    Y_UNIT_TEST(TestConfiguration) { \
        /* All tests can access TCurrentTest::FlagName */ \
        auto config = getConfig(TCurrentTest::FlagName); \
        UNIT_ASSERT(config.isValid()); \
    }

// Create twin test suite
Y_UNIT_TEST_SUITE_TWIN(FeatureTestSuite, UseNewAlgorithm, MY_SUITE_TESTS)
```

This creates two test suites:
- `FeatureTestSuite+UseNewAlgorithm` (with `UseNewAlgorithm=true`)
- `FeatureTestSuite-UseNewAlgorithm` (with `UseNewAlgorithm=false`)

Each suite contains all the tests defined in the `MY_SUITE_TESTS` macro.

### Why Use a Macro for Test Definitions?

Due to C preprocessor limitations, test definitions must be provided as a macro that can be expanded twice - once for each suite variant. This allows the preprocessor to duplicate the test code in both suite namespaces.

### Benefits

- **Comprehensive testing**: Ensures code works correctly with feature flags in both states
- **Avoids duplication**: Write test logic once, run it in both configurations
- **Clear test names**: Test names clearly indicate which variant is running
- **Compile-time flag**: The flag is `constexpr`, allowing it to be used in compile-time expressions

### Example Real-World Scenario

```cpp
#define COMPRESSION_TESTS(EnableCompression) \
    Y_UNIT_TEST(TestDataTransfer) { \
        auto config = MakeConfig(TCurrentTest::EnableCompression); \
        auto result = transferData(config); \
        UNIT_ASSERT(result.success); \
        if (TCurrentTest::EnableCompression) { \
            UNIT_ASSERT(result.bytesTransferred < uncompressedSize); \
        } \
    } \
    Y_UNIT_TEST(TestPerformance) { \
        auto startTime = Now(); \
        processWithConfig(TCurrentTest::EnableCompression); \
        auto duration = Now() - startTime; \
        UNIT_ASSERT(duration < MAX_DURATION); \
    }

Y_UNIT_TEST_SUITE_TWIN(CompressionTests, EnableCompression, COMPRESSION_TESTS)
```

## Y_UNIT_TEST_QUAD

Similar to Y_UNIT_TEST_TWIN, but with two boolean parameters, creating four test instances (all combinations of true/false for both parameters).

### Usage

```cpp
Y_UNIT_TEST_SUITE(MySuite) {
    Y_UNIT_TEST_QUAD(TestWithTwoFlags, UseCompression, UseEncryption) {
        auto data = prepareData(UseCompression, UseEncryption);
        UNIT_ASSERT(data.isValid());
    }
}
```

This creates four test instances:
- `TestWithTwoFlags-UseCompression-UseEncryption`
- `TestWithTwoFlags+UseCompression-UseEncryption`
- `TestWithTwoFlags-UseCompression+UseEncryption`
- `TestWithTwoFlags+UseCompression+UseEncryption`

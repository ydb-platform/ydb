# GWP-ASan Integration for YDB

This module provides GWP-ASan (Guarded Pointer ASan) integration for YDB applications.

## What is GWP-ASan?

GWP-ASan is a memory error detector that uses guard pages to detect use-after-free and buffer overflow bugs. It's designed to be used in production with minimal performance overhead by sampling a small percentage of allocations.

## Usage

GWP-ASan is automatically initialized in both `ydb` and `ydbd` applications. No code changes are required to use it.

## Configuration

GWP-ASan can be configured using the `GWP_ASAN_OPTIONS` environment variable:

```bash
# Enable GWP-ASan with custom settings
export GWP_ASAN_OPTIONS="Enabled=1:SampleRate=5000:MaxSimultaneousAllocations=16"

# Disable GWP-ASan
export GWP_ASAN_OPTIONS="Enabled=0"

# Enable debug output
export GWP_ASAN_DEBUG=1
```

### Available Options

- **Enabled**: Enable/disable GWP-ASan (default: true)
- **SampleRate**: Probability of sampling an allocation (1/N) (default: 10000)
- **MaxSimultaneousAllocations**: Number of simultaneous guarded allocations (default: 8)
- **InstallSignalHandlers**: Install signal handlers for better error reporting (default: true)
- **Recoverable**: Continue execution after detecting an error (default: false)

## Production Settings

The default settings are optimized for production use:
- Conservative sample rate (1/10000) to minimize performance impact
- Small pool size (8 simultaneous allocations) to reduce memory overhead
- Signal handlers enabled for better error reporting
- Non-recoverable mode to catch bugs early

## Platform Support

GWP-ASan is only available on:
- Non-Windows platforms
- Clang compiler
- x86_64 and aarch64 architectures

On unsupported platforms, the initialization is a no-op.

## Performance Impact

GWP-ASan has minimal performance impact in production:
- Memory overhead: ~8 guard pages (typically 32KB on x86_64)
- CPU overhead: Negligible with default sample rate of 1/10000
- No impact on non-sampled allocations
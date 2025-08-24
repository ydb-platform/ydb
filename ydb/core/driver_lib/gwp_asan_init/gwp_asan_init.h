#pragma once

namespace NKikimr {

// Initialize GWP-ASan memory error detection.
// Should be called early in main() before any significant memory allocations.
// Uses environment variables for configuration:
// - GWP_ASAN_OPTIONS: Standard GWP-ASan options string
// Safe to call multiple times (will only initialize once).
void InitializeGwpAsan();

} // namespace NKikimr
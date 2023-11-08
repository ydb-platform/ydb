// Automatically sets GOMAXPROCS to match Yandex clouds container CPU quota.
//
// This package always adjust GOMAXPROCS to some "safe" value.
// "safe" values are:
//   - 2 or more
//   - no more than logical cores
//   - no moore than container guarantees
//   - no more than 8
package maxprocs

<p align="center">
  <img src="z-order.png" alt="Z-Order Curve" height="250">
</p>

<h1 align="center">Morton ND</h1>

A header-only Morton encode/decode library (C++14) capable of encoding from and decoding to N-dimensional space.

All algorithms are **generated** at compile-time for the number of dimensions and field width used. This way, loops and branches are not required.

Includes a hardware-based approach (using Intel BMI2) for most Intel CPUs, as well as another fast approach based on Lookup Table (LUT) methods for other CPU variants. 

### Status
[![Build Status](https://github.com/kevinhartman/morton-nd/actions/workflows/cmake.yml/badge.svg)](https://github.com/kevinhartman/morton-nd/actions/workflows/cmake.yml) [![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://opensource.org/licenses/MIT)

## Features

### Encode/Decode Support
- any number of dimensions (e.g. `2D, 3D, 4D ... ND`).
- built-in support for up to 128-bit native results (`__uint128_t`). Unlimited using a user-supplied "big integer" class (not yet tested).
- `constexpr` encoding and decoding, allowing Morton coding to be expressed at compile-time.

## Encoders and Decoders

### Hardware (Intel BMI2)
Supports encoding and decoding in N dimensions, using Intel's BMI2 ISA extension (available in Haswell (Intel), Excavator (AMD) and newer).

See the [Morton ND BMI2 Usage Guide](docs/MortonND_BMI2.md) for details.

```c++
using MortonND_4D = mortonnd::MortonNDBmi<4, uint64_t>;

// Encodes 4 fields into a uint64_t result.
auto encoding = MortonND_4D::Encode(f1, f2, f3, f4);

// Decodes 4 fields.
std::tie(f1, f2, f3, f4) = MortonND_4D::Decode(encoding);
```

### Lookup Table (LUT)
Supports encoding and decoding in N dimensions, using compiler-generated LUTs.

LUTs are defined with constant expressions and thus can be generated (and even used) at compile-time.

Both the encoder and decoder support chunking, allowing a LUT smaller than the input field width when encoding, or smaller than the Morton code width when decoding, to be used internally. This is useful for applications which require faster compilation times and smaller binaries (at the expense of extra bit manipulation operations required to combine chunks at runtime).

See the [Morton ND LUT Usage Guide](docs/MortonND_LUT.md) for details.

#### Encoding
```c++
// Generates a 4D LUT encoder (4 fields, 16 bits each, 8-bit LUT) using the compiler.
constexpr auto MortonND_4D_Enc = mortonnd::MortonNDLutEncoder<4, 16, 8>();

// Encodes 4 fields. Can be done at run-time or compile-time.
auto encoding = MortonND_4D_Enc.Encode(f1, f2, f3, f4);
```

#### Decoding
```c++
// Generates a 4D LUT decoder (4 fields, 16 bits each, 8-bit LUT) using the compiler.
constexpr auto MortonND_4D_Dec = mortonnd::MortonNDLutDecoder<4, 16, 8>();

// Decodes 4 fields. Can be done at run-time or compile-time (just not with std::tie in C++14).
std::tie(f1, f2, f3, f4) = MortonND_4D_Dec.Decode(encoding);
```

## Testing and Performance
Validation testing specific to MortonND is located in the `/tests` folder, covering N-dimensional configurations where `N ∈ { 1, 2, 3, 4, 5, 8, 16, 32, 64 }` for common field sizes, and is run as part of Travis CI.

Performance benchmark tests (and additional validation) for 2D and 3D use cases are located in a separate repository. See [this fork](https://github.com/kevinhartman/libmorton#fork-changes) of @Forceflow's [Libmorton](https://github.com/Forceflow/libmorton), which integrates Morton ND into Libmorton's existing test framework.

### Benchmarks
The snippets below show performance comparisons between various 3D configurations of Morton ND. Comparisons to the 3D algorithms found in Libmorton are also included to demonstrate that Morton ND's generated algorithms are as efficient as hand-coded algorithms.

To run these tests (and more!) on your own machine, clone the fork linked above.

The following metrics (sorted by random access time, ascending) were collected on an I9-9980HK, compiled with GCC 11.1.0 on macOS 11.1 using `-O3 -DNDEBUG`. Results include data from both linearly increasing and random inputs to demonstrate the performance impact of cache (hit or miss) under each algorithm / configuration. Results are averaged over 5 runs (each algorithm is run 5 times consecutively before moving on to the next).

#### 32-bit
```
++ Running each performance test 5 times and averaging results
++ Encoding 512^3 morton codes (134217728 in total)

    Linear      Random
    ======      ======
    524.178 ms  515.768 ms : 32-bit (MortonND)    LUT: 1 chunks, 10 bit LUT
    530.421 ms  536.726 ms : 32-bit (lib-morton)  BMI2 instruction set
    539.061 ms  530.954 ms : 32-bit (MortonND)    BMI2
    629.888 ms  628.029 ms : 32-bit (lib-morton)  LUT Shifted
    647.075 ms  646.281 ms : 32-bit (MortonND)    LUT: 2 chunks, 8 bit LUT
    658.859 ms  667.313 ms : 32-bit (MortonND)    LUT: 2 chunks, 5 bit LUT
    668.371 ms  665.673 ms : 32-bit (lib-morton)  LUT
    
++ Decoding 512^3 morton codes (134217728 in total)

    Linear      Random
    ======      ======
    560.478 ms  3159.143 ms : 32-bit (lib-morton)  BMI2 Instruction set
    569.632 ms  3188.762 ms : 32-bit (MortonND)    MortonND: BMI2
    807.765 ms  3386.069 ms : 32-bit (MortonND)    LUT: 3 chunks, 10 bit LUT
    870.854 ms  3479.404 ms : 32-bit (lib-morton)  LUT Shifted
    902.272 ms  3516.063 ms : 32-bit (MortonND)    LUT: 4 chunks, 8 bit LUT
    1033.880 ms 3605.081 ms : 32-bit (lib-morton)  LUT
    1162.954 ms 3755.352 ms : 32-bit (MortonND)    LUT: 6 chunks, 5 bit LUT
```

#### 64-bit
```
++ Running each performance test 5 times and averaging results
++ Encoding 512^3 morton codes (134217728 in total)

    Linear      Random
    ======      ======
    524.044 ms  563.434 ms : 64-bit (lib-morton)  BMI2 instruction set
    536.608 ms  587.632 ms : 64-bit (MortonND)    BMI2
    632.564 ms  639.866 ms : 64-bit (MortonND)    LUT: 2 chunks, 11 bit LUT
    812.262 ms  817.026 ms : 64-bit (MortonND)    LUT: 3 chunks, 8 bit LUT
    812.683 ms  823.962 ms : 64-bit (MortonND)    LUT: 3 chunks, 7 bit LUT
    828.591 ms  845.098 ms : 64-bit (lib-morton)  LUT Shifted
    612.973 ms  863.919 ms : 64-bit (MortonND)    LUT: 2 chunks, 16 bit LUT
    909.336 ms  929.829 ms : 64-bit (lib-morton)  LUT
    516.114 ms 1003.164 ms : 64-bit (MortonND)    LUT: 1 chunks, 21 bit LUT
    
++ Decoding 512^3 morton codes (134217728 in total)

    Linear      Random
    ======      ======
    575.827 ms  3195.522 ms : 64-bit (MortonND)    BMI2
    560.200 ms  3204.435 ms : 64-bit (lib-morton)  BMI2 Instruction set
    932.061 ms  3600.856 ms : 64-bit (MortonND)    LUT: 4 chunks, 16 bit LUT
    1164.335 ms 3758.155 ms : 64-bit (MortonND)    LUT: 6 chunks, 11 bit LUT
    1300.058 ms 3912.560 ms : 64-bit (lib-morton)  LUT Shifted
    1518.749 ms 4144.474 ms : 64-bit (MortonND)    LUT: 9 chunks, 7 bit LUT
    1387.119 ms 4039.352 ms : 64-bit (MortonND)    LUT: 8 chunks, 8 bit LUT
    1564.913 ms 4150.859 ms : 64-bit (lib-morton)  LUT
    833.821 ms  4496.993 ms : 64-bit (MortonND)    LUT: 3 chunks, 21 bit LUT
```

## Installation
### CMake
Morton ND provides a CMake integration, making it easy to use from your CMake project.

#### Adding the dependency
If you've installed Morton ND to your `CMAKE_MODULE_PATH` (e.g. with [vcpkg](https://github.com/microsoft/vcpkg)), find and link it like this:

```cmake
find_package(morton-nd CONFIG REQUIRED)
target_link_libraries(main PRIVATE morton-nd::MortonND)
```

Otherwise, if you're using Morton ND as a Git submodule:
```cmake
add_subdirectory(morton-nd)
target_link_libraries(main PRIVATE morton-nd::MortonND)
```

#### Including library headers
By using `target_link_libraries(...)` as above, Morton ND's headers will be automatically available for use by your target:

```c++
// main.cpp
#include <morton-nd/mortonND_BMI2.h>
#include <morton-nd/mortonND_LUT.h>
```

## Thanks
* Jeroen Baert (@Forceflow)
  - [Morton encoding/decoding through bit interleaving: Implementations](https://www.forceflow.be/2013/10/07/morton-encodingdecoding-through-bit-interleaving-implementations/)
  - [libmorton](https://github.com/Forceflow/libmorton), a C++11 header-only library for 2D and 3D Morton encoding and decoding.

## License
This project is licensed under the MIT license.

Attribution is appreciated where applicable, and as such, a NOTICE file is included which may be distributed in the credits of derivative software.

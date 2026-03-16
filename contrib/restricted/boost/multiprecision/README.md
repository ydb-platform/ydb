Boost Multiprecision Library
============================

|                  |  Master  |   Develop   |
|------------------|----------|-------------|
| Drone            | [![Build Status](https://drone.cpp.al/api/badges/boostorg/multiprecision/status.svg?ref=refs/heads/master)](https://drone.cpp.al/boostorg/multiprecision)          | [![Build Status](https://drone.cpp.al/api/badges/boostorg/multiprecision/status.svg)](https://drone.cpp.al/boostorg/multiprecision) |
| Github Actions   | [![Build Status](https://github.com/boostorg/multiprecision/actions/workflows/multiprecision.yml/badge.svg?branch=master)](https://github.com/boostorg/multiprecision/actions?query=branch%3Amaster) | [![Build Status](https://github.com/boostorg/multiprecision/actions/workflows/multiprecision.yml/badge.svg?branch=develop)](https://github.com/boostorg/multiprecision/actions?query=branch%3Adevelop) |
| Codecov          | [![codecov](https://codecov.io/gh/boostorg/multiprecision/branch/master/graph/badge.svg)](https://codecov.io/gh/boostorg/multiprecision/branch/master)             | [![codecov](https://codecov.io/gh/boostorg/multiprecision/branch/develop/graph/badge.svg)](https://codecov.io/gh/boostorg/multiprecision/branch/develop) |


`Boost.Multiprecision` is a C++ library that provides integer,
rational, floating-point, complex and interval number types
having more range and precision than the language's ordinary built-in types.

Language adherence:
  - `Boost.Multiprecision` requires a compliant C++14 compiler.
  - It is compatible with C++14, 17, 20, 23 and beyond.

The big number types in `Boost.Multiprecision` can be used with a wide selection of basic
mathematical operations, elementary transcendental functions as well as the functions in
[`Boost.Math`](https://github.com/boostorg/math).
The Multiprecision types also interoperate with built-in types in C++.
The big number types adhere to clearly defined conversion rules. This allows `Boost.Multiprecision` to be
used for all kinds of mathematical calculations involving integer, rational and floating-point types
requiring extended range and precision.

Multiprecision consists of a generic interface to the mathematics
of large numbers as well as a selection of big number backends.
These include interfaces to GMP, MPFR, MPIR and TomMath
and also Multiprecision's own collection of Boost-licensed,
header-only backends for integers, rationals, floats and complex-floats.

In addition, user-defined backends can be created and used with the interface of Multiprecision,
presuming that the class implementation adheres to the necessary concepts.

Depending upon the multiprecision type, precision may be arbitrarily large (limited only by available memory),
fixed at compile time (for example $50$ or $100$ decimal digits),
or variable controlled at run-time by member functions.
Expression templates can be enabled or disabled when configuring the `number` type with its backend.
Most of the multiprecision types are expression-template-enabled by default.
This usually provides better performance than using types configured without expression templates.

The full documentation is available on [boost.org](http://www.boost.org/doc/libs/release/libs/multiprecision/index.html).

A practical, comprehensive, instructive, clear and very helpful video regarding the use of Multiprecision
can be found [here](https://www.youtube.com/watch?v=mK4WjpvLj4c).

## Using Multiprecision

<p align="center">
  <a href="https://godbolt.org/z/hd95P3ovK" alt="godbolt">
    <img src="https://img.shields.io/badge/try%20it%20on-godbolt-green" /></a>
</p>

In the following example, we use Multiprecision's Boost-licensed binary
floating-point backend type `cpp_bin_float` to compute ${\sim}100$ decimal digits of

$$\sqrt{\pi} = \Gamma \left( \frac{1}{2} \right)~{\approx}~1.772453850905516027298{\ldots}\text{,}$$

where we also observe that Multiprecision can seamlesly interoperate with
[`Boost.Math`](https://github.com/boostorg/math).

```cpp
// ------------------------------------------------------------------------------
// Copyright Christopher Kormanyos 2024 - 2025.
// Distributed under the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt
// or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <boost/math/special_functions/gamma.hpp>
#include <boost/multiprecision/cpp_bin_float.hpp>

#include <iomanip>
#include <iostream>
#include <sstream>

auto main() -> int
{
  // Configure a multiprecision binary floating-point type with approximately
  // 100 decimal digits of precision and expression templates enabled.
  // Note that the popular type cpp_bin_float_100 has already been preconfigured
  // and aliased in the multiprecision headers.

  using big_float_type = boost::multiprecision::cpp_bin_float_100;

  // In the next few lines, compute and compare sqrt(pi) with tgamma(1/2)
  // using the 100-digit multiprecision type.

  const big_float_type sqrt_pi { sqrt(boost::math::constants::pi<big_float_type>()) };

  const big_float_type half { big_float_type(1) / 2 };

  const big_float_type gamma_half { boost::math::tgamma(half) }; 

  std::stringstream strm { };

  // 1.772453850905516027298167483341145182797549456122387128213807789852911284591032181374950656738544665

  strm << std::setprecision(std::numeric_limits<big_float_type>::digits10)
       << "sqrt_pi   : "
       << sqrt_pi
       << "\ngamma_half: "
       << gamma_half;

  std::cout << strm.str() << std::endl;
}
```

## Standalone

Defining `BOOST_MP_STANDALONE` allows `Boost.Multiprecision`
to be used with the only dependency being [Boost.Config](https://github.com/boostorg/config).
Our [package on this page](https://github.com/boostorg/multiprecision/releases)
already includes a copy of [Boost.Config](https://github.com/boostorg/config).
So no other downloads are required.

Some functionality is reduced in this mode.
A `static_assert` message will alert you if a particular feature has been disabled by standalone mode.
[`Boost.Math`](https://github.com/boostorg/math) standalone mode is compatiable,
and recommended if special functions are required for the floating point types.

## Support, bugs and feature requests

Bugs and feature requests can be reported through the [Gitub issue tracker](https://github.com/boostorg/multiprecision/issues)
(see [open issues](https://github.com/boostorg/multiprecision/issues) and
[closed issues](https://github.com/boostorg/multiprecision/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aclosed)).

You can submit your changes through a [pull request](https://github.com/boostorg/multiprecision/pulls).

There is no mailing-list specific to `Boost Multiprecision`,
although you can use the general-purpose Boost [mailing-list](http://lists.boost.org/mailman/listinfo.cgi/boost-users)
using the tag `[multiprecision]`.

## Development

Clone the whole boost project, which includes the individual Boost projects as submodules
([see boost+git doc](https://github.com/boostorg/boost/wiki/Getting-Started)):

```bash
  git clone https://github.com/boostorg/boost
  cd boost
  git submodule update --init
```

The `Boost.Multiprecision` Library is located in `libs/multiprecision/`.

### Running tests

First, build the `b2` engine by running `bootstrap.sh` in the root of the boost directory. This will generate `b2` configuration in `project-config.jam`.

```bash
  ./bootstrap.sh
```

Now make sure you are in `libs/multiprecision/test`. You can either run all the tests listed in `Jamfile.v2` or run a single test:

```bash
  ../../../b2                        <- run all tests
  ../../../b2 test_complex           <- single test
```

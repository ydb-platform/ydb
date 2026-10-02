# expected lite: expected objects for C++11 and later

[![Language](https://img.shields.io/badge/C%2B%2B-11-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B#Standardization) [![License](https://img.shields.io/badge/license-BSL-blue.svg)](https://opensource.org/licenses/BSL-1.0) [![Build Status](https://github.com/martinmoene/expected-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/martinmoene/expected-lite/actions/workflows/ci.yml) [![Build status](https://ci.appveyor.com/api/projects/status/sle31w7obrm8lhe1?svg=true)](https://ci.appveyor.com/project/martinmoene/expected-lite) [![Version](https://badge.fury.io/gh/martinmoene%2Fexpected-lite.svg)](https://github.com/martinmoene/expected-lite/releases) [![download](https://img.shields.io/badge/latest-download-blue.svg)](https://raw.githubusercontent.com/martinmoene/expected-lite/master/include/nonstd/expected.hpp) [![Conan](https://img.shields.io/badge/on-conan-blue.svg)](https://conan.io/center/expected-lite) [![Vcpkg](https://img.shields.io/badge/on-vcpkg-blue.svg)](https://vcpkg.link/ports/expected-lite) [![Try it online](https://img.shields.io/badge/on-wandbox-blue.svg)](https://wandbox.org/permlink/MnnwqOtE8ZQ4rRsv) [![Try it on godbolt online](https://img.shields.io/badge/on-godbolt-blue.svg)](https://godbolt.org/z/9BuMZx)

*expected lite* is a single-file header-only library for objects that either represent a valid value or an error that you can pass by value. It is intended for use with C++11 and later. The library is based on the [std:&#58;expected](http://wg21.link/p0323) proposal [1] .

**Contents**  
- [Example usage](#example-usage)
- [In a nutshell](#in-a-nutshell)
- [License](#license)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [Synopsis](#synopsis)
- [Comparison with like types](#comparison)
- [Reported to work with](#reported-to-work-with)
- [Implementation notes](#implementation-notes)
- [Other implementations of expected](#other-implementations-of-expected)
- [Notes and references](#notes-and-references)
- [Appendix](#appendix)

## Example usage

```Cpp
#include "nonstd/expected.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

using namespace nonstd;
using namespace std::literals;

auto to_int( char const * const text ) -> expected<int, std::string> 
{
    char * pos = nullptr;
    auto value = strtol( text, &pos, 0 );

    if ( pos != text ) return value;
    else               return make_unexpected( "'"s + text + "' isn't a number" );
}

int main( int argc, char * argv[] )
{
    auto text = argc > 1 ? argv[1] : "42";

    auto ei = to_int( text );

    if ( ei ) std::cout << "'" << text << "' is " << *ei << ", ";
    else      std::cout << "Error: " << ei.error();
}
```

### Compile and run

```
prompt> g++ -std=c++14 -Wall -I../include -o 01-basic.exe 01-basic.cpp && 01-basic.exe 123 && 01-basic.exe abc
'123' is 123, Error: 'abc' isn't a number
```

## In a nutshell

**expected lite** is a single-file header-only library to represent value objects that either contain a valid value or an error. The library is a partly implementation of the  proposal for [std:&#58;expected](http://wg21.link/p0323) [1,2,3] for use with C++11 and later.

**Some Features and properties of expected lite** are ease of installation (single header), default and explicit construction of an expected, construction and assignment from a value that is convertible to the underlying type, copy- and move-construction and copy- and move-assignment from another expected of the same type, testing for the presence of a value, operators for unchecked access to the value or the error (pointer or reference), value() and value_or() for checked access to the value, relational operators, swap() and various factory functions.

Version 0.7.0 introduces the monadic operations of propsoal [p2505](http://wg21.link/p2505).

*expected lite* shares the approach to in-place tags with [any-lite](https://github.com/martinmoene/any-lite), [optional-lite](https://github.com/martinmoene/optional-lite) and with [variant-lite](https://github.com/martinmoene/variant-lite) and these libraries can be used together.

**Not provided** are reference-type expecteds. *expected lite* doesn't honour triviality of value and error types. *expected lite* doesn't handle overloaded *address of* operators.

For more examples, see [1].

## License

*expected lite* is distributed under the [Boost Software License](LICENSE.txt).

## Dependencies

*expected lite* has no other dependencies than the [C++ standard library](http://en.cppreference.com/w/cpp/header).

## Installation

*expected lite* is a single-file header-only library. Put `expected.hpp` directly into the project source tree or somewhere reachable from your project.

## Synopsis

**Contents**  
- [Configuration](#configuration)
- [Types in namespace nonstd](#types-in-namespace-nonstd)  
- [Interface of expected](#interface-of-expected)  
- [Algorithms for expected](#algorithms-for-expected)  
- [Interface of unexpected_type](#interface-of-unexpected_type)  
- [Algorithms for unexpected_type](#algorithms-for-unexpected_type)  

### Configuration

#### Tweak header

If the compiler supports [`__has_include()`](https://en.cppreference.com/w/cpp/preprocessor/include), *expected lite* supports the [tweak header](https://vector-of-bool.github.io/2020/10/04/lib-configuration.html) mechanism. Provide your *tweak header* as `nonstd/expected.tweak.hpp` in a folder in the include-search-path. In the tweak header, provide definitions as documented below, like `#define expected_CPLUSPLUS 201103L`.

#### Standard selection macro

\-D<b>nsel\_CPLUSPLUS</b>=199711L  
Define this macro to override the auto-detection of the supported C++ standard, or if your compiler does not set the `__cplusplus` macro correctly.

#### Select `std::expected` or `nonstd::expected`

At default, *expected lite* uses `std::expected` if it is available and lets you use it via namespace `nonstd`. You can however override this default and explicitly request to use `std::expected` or expected lite's `nonstd::expected` as `nonstd::expected` via the following macros.

-D<b>nsel\_CONFIG\_SELECT\_EXPECTED</b>=nsel_EXPECTED_DEFAULT  
Define this to `nsel_EXPECTED_STD` to select `std::expected` as `nonstd::expected`. Define this to `nsel_EXPECTED_NONSTD` to select `nonstd::expected` as `nonstd::expected`. Default is undefined, which has the same effect as defining to `nsel_EXPECTED_DEFAULT`.

-D<b>nsel\_P0323R</b>=7  *(default)*  
Define this to the proposal revision number to control the presence and behavior of features (see tables). Default is 7 for the latest revision.

#### Define `WIN32_LEAN_AND_MEAN`

-D<b>nsel\_CONFIG\_WIN32\_LEAN\_AND\_MEAN</b>=1  
Define this to 0 if you want to omit automatic definition of `WIN32_LEAN_AND_MEAN`. Default is 1 when `_MSC_VER` is present.

#### Disable C++ exceptions

-D<b>nsel\_CONFIG\_NO\_EXCEPTIONS</b>=0
Define this to 1 if you want to compile without exceptions. If not defined, the header tries and detect if exceptions have been disabled (e.g. via `-fno-exceptions` or `/kernel`). Default determined in header.

#### Enable SEH exceptions

-D<b>nsel\_CONFIG\_NO\_EXCEPTIONS\_SEH</b>=0
Define this to 1 or 0 to control the use of SEH when C++ exceptions are disabled (see above). If not defined, the header tries and detect if SEH is available if C++ exceptions have been disabled (e.g. via `-fno-exceptions` or `/kernel`). Default determined in header.

#### Disable \[\[nodiscard\]\]

-D<b>nsel\_CONFIG\_NO\_NODISCARD</b>=0
Define this to 1 if you want to compile without \[\[nodiscard\]\]. Note that the default of marking `class expected` with \[\[nodiscard\]\] is not part of the C++23 standard. The rationale to use \[\[nodiscard\]\] is that unnoticed discarded expected error values may break the error handling flow.

#### Enable compilation errors

\-D<b>nsel\_CONFIG\_CONFIRMS\_COMPILATION\_ERRORS</b>=0  
Define this macro to 1 to experience the by-design compile-time errors of the library in the test suite. Default is 0.

#### Configure P2505 monadic operations

By default, *expected lite* provides monadic operations as described in [P2505R5](http://wg21.link/p2505r5). You can disable these operations by defining the following macro.

-D<b>nsel\_P2505R</b>=0

You can use the R3 revision of P2505, which lacks `error_or`, and uses `remove_cvref` for transforms, by defining the following macro.

-D<b>nsel\_P2505R</b>=3

### Types in namespace nonstd

| Purpose         | Type | Note / Object |
|-----------------|------|---------------|
| Expected        | template&lt;typename T, typename E = std::exception_ptr><br>class **expected**; | nsel_P0323 <= 2 |
| Expected        | template&lt;typename T, typename E><br>class **expected**; | nsel_P0323 > 2 |
| Error type      | template&lt;typename E><br>class **unexpected_type**; | &nbsp; |
| Error type      | template&lt;><br>class **unexpected_type**&lt;std::exception_ptr>; | nsel_P0323 <= 2 |
| Error type      | template&lt;typename E><br>class **unexpected**; | >= C++17 |
| Traits          | template&lt;typename E><br>struct **is_unexpected**;  | nsel_P0323 <= 3 |
| In-place value construction | struct **in_place_t**;            | in_place_t in_place{}; |
| In-place error construction | struct **in_place_unexpected_t**; | in_place_unexpected_t<br>unexpect{}; |
| In-place error construction | struct **in_place_unexpected_t**; | in_place_unexpected_t<br>in_place_unexpected{}; |
| Error reporting             | class **bad_expected_access**;    |&nbsp; |

### Interface of expected

| Kind         | Method                                                                  | Result |
|--------------|-------------------------------------------------------------------------|--------|
| Construction | [constexpr] **expected**() noexcept(...)                                | an object with default value |
| &nbsp;       | [constexpr] **expected**( expected const & other )                      | initialize to contents of other |
| &nbsp;       | [constexpr] **expected**( expected && other )                           | move contents from other |
| &nbsp;       | [constexpr] **expected**( value_type const & value )                    | initialize to value |
| &nbsp;       | [constexpr] **expected**( value_type && value ) noexcept(...)           | move from value |
| &nbsp;       | [constexpr] explicit **expected**( in_place_t, Args&&... args )         | construct value in-place from args |
| &nbsp;       | [constexpr] explicit **expected**( in_place_t,<br>&emsp;std::initializer_list&lt;U> il, Args&&... args ) | construct value in-place from args |
| &nbsp;       | [constexpr] **expected**( unexpected_type<E> const & error )            | initialize to error |
| &nbsp;       | [constexpr] **expected**( unexpected_type<E> && error )                 | move from error |
| &nbsp;       | [constexpr] explicit **expected**( in_place_unexpected_t,<br>&emsp;Args&&... args ) | construct error in-place from args |
| &nbsp;       | [constexpr] explicit **expected**( in_place_unexpected_t,<br>&emsp;std::initializer_list&lt;U> il, Args&&... args )| construct error in-place from args |
| Destruction  | ~**expected**()                                                         | destruct current content |
| Assignment   | expected **operator=**( expected const & other )                        | assign contents of other;<br>destruct current content, if any |
| &nbsp;       | expected & **operator=**( expected && other ) noexcept(...)             | move contents of other |
| &nbsp;       | expected & **operator=**( U && v )                                      | move value from v |
| &nbsp;       | expected & **operator=**( unexpected_type<E> const & u )                | initialize to unexpected |
| &nbsp;       | expected & **operator=**( unexpected_type<E> && u )                     | move from unexpected |
| &nbsp;       | template&lt;typename... Args><br>void **emplace**( Args &&... args )     | emplace from args |
| &nbsp;       | template&lt;typename U, typename... Args><br>void **emplace**( std::initializer_list&lt;U> il, Args &&... args )  | emplace from args |
| Swap         | void **swap**( expected & other ) noexcept                              | swap with other  |
| Observers    | constexpr value_type const \* **operator->**() const                    | pointer to current content (const);<br>must contain value |
| &nbsp;       | value_type \* **operator->**()                                          | pointer to current content (non-const);<br>must contain value |
| &nbsp;       | constexpr value_type const & **operator \***() const &                   | the current content (const ref);<br>must contain value |
| &nbsp;       | constexpr value_type && **operator \***() &&                             | the current content (non-const ref);<br>must contain value |
| &nbsp;       | constexpr explicit operator **bool**() const noexcept                   | true if contains value |
| &nbsp;       | constexpr **has_value**() const noexcept                                | true if contains value |
| &nbsp;       | constexpr value_type const & **value**() const &                        | current content (const ref);<br>see [note 1](#note1) |
| &nbsp;       | value_type & **value**() &                                              | current content (non-const ref);<br>see [note 1](#note1) |
| &nbsp;       | constexpr value_type && **value**() &&                                  | move from current content;<br>see [note 1](#note1) |
| &nbsp;       | constexpr error_type const & **error**() const &                        | current error (const ref);<br>must contain error |
| &nbsp;       | error_type & **error**() &                                              | current error (non-const ref);<br>must contain error |
| &nbsp;       | constexpr error_type && **error**() &&                                  | move from current error;<br>must contain error |
| &nbsp;       | constexpr unexpected_type<E> **get_unexpected**() const                 | the error as unexpected&lt;>;<br>must contain error |
| &nbsp;       | template&lt;typename Ex><br>bool **has_exception**() const               | true of contains exception (as base) |
| &nbsp;       | value_type **value_or**( U && v ) const &                               | value or move from v |
| &nbsp;       | value_type **value_or**( U && v ) &&                                    | move from value or move from v |
| &nbsp;       | constexpr error_type **error_or**( G && e ) const &                     | return current error or v [requires nsel_P2505R >= 4] |
| &nbsp;       | constexpr error_type **error_or**( G && e ) &&                          | move from current error or from v [requires nsel_P2505R >=4] |
| Monadic operations<br>(requires nsel_P2505R >= 3) | constexpr auto **and_then**( F && f ) & G| return f(value()) if has value, otherwise the error |
| &nbsp;       | constexpr auto **and_then**( F && f ) const &                           | return f(value()) if has value, otherwise the error |
| &nbsp;       | constexpr auto **and_then**( F && f ) &&                                | return f(std::move(value())) if has value, otherwise the error |
| &nbsp;       | constexpr auto **and_then**( F && f ) const &&                          | return f(std::move(value())) if has value, otherwise the error |
| &nbsp;       | constexpr auto **or_else**( F && f ) &                                  | return the value, or f(error()) if there is no value |
| &nbsp;       | constexpr auto **or_else**( F && f ) const &                            | return the value, or f(error()) if there is no value |
| &nbsp;       | constexpr auto **or_else**( F && f ) &&                                 | return the value, or f(std::move(error())) if there is no value |
| &nbsp;       | constexpr auto **or_else**( F && f ) const &&                           | return the value, or f(std::move(error())) if there is no value |
| &nbsp;       | constexpr auto **transform**( F && f ) &                                | return f(value()) wrapped if has value, otherwise the error |
| &nbsp;       | constexpr auto **transform**( F && f ) const &                          | return f(value()) wrapped if has value, otherwise the error |
| &nbsp;       | constexpr auto **transform**( F && f ) &&                               | return f(std::move(value())) wrapped if has value, otherwise the error |
| &nbsp;       | constexpr auto **transform**( F && f ) const &&                         | return f(std::move(value())) wrapped if has value, otherwise the error |
| &nbsp;       | constexpr auto **transform_error**( F && f ) &                          | return the value if has value, or f(error()) otherwise |
| &nbsp;       | constexpr auto **transform_error**( F && f ) const &                    | return the value if has value, or f(error()) otherwise |
| &nbsp;       | constexpr auto **transform_error**( F && f ) &&                         | return the value if has value, or f(std::move(error())) otherwise |
| &nbsp;       | constexpr auto **transform_error**( F && f ) const &&                   | return the value if has value, or f(std::move(error())) otherwise |
| &nbsp;       | ... | &nbsp; |

<a id="note1"></a>Note 1: checked access: if no content, for std::exception_ptr rethrows error(), otherwise throws bad_expected_access(error()).

### Algorithms for expected

| Kind                            | Function |
|---------------------------------|----------|
| Comparison with expected        | &nbsp;   | 
| ==&ensp;!=                      | template&lt;typename T1, typename E1, typename T2, typename E2><br>constexpr bool operator ***op***(<br>&emsp;expected&lt;T1,E1> const & x,<br>&emsp;expected&lt;T2,E2> const & y ) |
| Comparison with expected        | nsel_P0323R <= 2 | 
| <&ensp;>&ensp;<=&ensp;>=        | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;expected&lt;T,E> const & x,<br>&emsp;expected&lt;T,E> const & y ) |
| Comparison with unexpected_type | &nbsp; | 
| ==&ensp;!=                      | template&lt;typename T1, typename E1, typename E2><br>constexpr bool operator ***op***(<br>&emsp;expected&lt;T1,E1> const & x,<br>&emsp;unexpected_type&lt;E2> const & u ) | 
| &nbsp;                          | template&lt;typename T1, typename E1, typename E2><br>constexpr bool operator ***op***(<br>&emsp;unexpected_type&lt;E2> const & u,<br>&emsp;expected&lt;T1,E1> const & x ) | 
| Comparison with unexpected_type | nsel_P0323R <= 2 | 
| <&ensp;>&ensp;<=&ensp;>=        | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;expected&lt;T,E> const & x,<br>&emsp;unexpected_type&lt;E> const & u ) | 
| &nbsp;                          | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;unexpected_type&lt;E> const & u,<br>&emsp;expected&lt;T,E> const & x ) | 
| Comparison with T               | &nbsp;   | 
| ==&ensp;!=                      | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;expected&lt;T,E> const & x,<br>&emsp;T const & v ) | 
| &nbsp;                          | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;T const & v,<br>&emsp;expected&lt;T,E> const & x ) | 
| Comparison with T               | nsel_P0323R <= 2 | 
| <&ensp;>&ensp;<=&ensp;>=        | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;expected&lt;T,E> const & x,<br>&emsp;T const & v ) | 
| &nbsp;                          | template&lt;typename T, typename E><br>constexpr bool operator ***op***(<br>&emsp;T const & v,<br>&emsp;expected&lt;T,E> const & x ) | 
| Specialized algorithms          | &nbsp;   | 
| Swap                            | template&lt;typename T, typename E><br>void **swap**(<br>&emsp;expected&lt;T,E> & x,<br>&emsp;expected&lt;T,E> & y )&emsp;noexcept( noexcept( x.swap(y) ) ) | 
| Make expected from              | nsel_P0323R <= 3 | 
| &emsp;Value                     | template&lt;typename T><br>constexpr auto **make_expected**( T && v ) -><br>&emsp;expected< typename std::decay&lt;T>::type> | 
| &emsp;Nothing                   | auto **make_expected**() -> expected&lt;void> | 
| &emsp;Current exception         | template&lt;typename T><br>constexpr auto **make_expected_from_current_exception**() -> expected&lt;T> | 
| &emsp;Exception                 | template&lt;typename T><br>auto **make_expected_from_exception**( std::exception_ptr v ) -> expected&lt;T>| 
| &emsp;Error                     | template&lt;typename T, typename E><br>constexpr auto **make_expected_from_error**( E e ) -><br>&emsp;expected&lt;T, typename std::decay&lt;E>::type> | 
| &emsp;Call                      | template&lt;typename F><br>auto **make_expected_from_call**( F f ) -><br>&emsp;expected< typename std::result_of&lt;F()>::type>| 
| &emsp;Call, void specialization | template&lt;typename F><br>auto **make_expected_from_call**( F f ) -> expected&lt;void> | 

### Interface of unexpected_type

| Kind         | Method                                                    | Result |
|--------------|-----------------------------------------------------------|--------|
| Construction | **unexpected_type**() = delete;                           | no default construction |
| &nbsp;       | constexpr explicit **unexpected_type**( E const & error ) | copy-constructed from an E |
| &nbsp;       | constexpr explicit **unexpected_type**( E && error )      | move-constructed from an E |
| Observers    | constexpr error_type const & **error**() const            | can observe contained error |
| &nbsp;       | error_type & **error**()                                  | can modify contained error |
| deprecated   | constexpr error_type const & **value**() const            | can observe contained error |
| deprecated   | error_type & **value**()                                  | can modify contained error |

### Algorithms for unexpected_type

| Kind                          | Function |
|-------------------------------|----------|
| Comparison with unexpected    | &nbsp;   | 
| ==&ensp;!=                    | template&lt;typename E><br>constexpr bool operator ***op***(<br>&emsp;unexpected_type&lt;E> const & x,<br>&emsp;unexpected_type&lt;E> const & y ) |
| Comparison with unexpected    | nsel_P0323R <= 2 | 
| <&ensp;>&ensp;<=&ensp;>=      | template&lt;typename E><br>constexpr bool operator ***op***(<br>&emsp;unexpected_type&lt;E> const & x,<br>&emsp;unexpected_type&lt;E> const & y ) |
| Comparison with exception_ptr | &nbsp;   | 
| ==&ensp;!=                    | constexpr bool operator ***op***(<br>&emsp;unexpected_type&lt;std::exception_ptr> const & x,<br>&emsp;unexpected_type&lt;std::exception_ptr> const & y ) |
| Comparison with exception_ptr | nsel_P0323R <= 2 | 
| <&ensp;>&ensp;<=&ensp;>=      | constexpr bool operator ***op***(<br>&emsp;unexpected_type&lt;std::exception_ptr> const & x,<br>&emsp;unexpected_type&lt;std::exception_ptr> const & y ) |
| Specialized algorithms        | &nbsp;   | 
| Make unexpected from          | &nbsp;   | 
| &emsp;Error                   | template&lt;typename E><br>[constexpr] auto **make_unexpected**(E && v) -><br>&emsp;unexpected_type< typename std::decay&lt;E>::type>| 
| &emsp;Arguments (in-place)    | template&lt;typename E, typename... Args><br>[constexpr] auto **make_unexpected**(in_place_t, Args &&... args) -><br>&emsp;unexpected_type< typename std::decay&lt;E>::type>| 
| Make unexpected from          | nsel_P0323R <= 3 | 
| &emsp;Current exception       | [constexpr] auto **make_unexpected_from_current_exception**() -><br>&emsp;unexpected_type< std::exception_ptr>| 

<a id="comparison"></a>
## Comparison with like types

|Feature               |<br>std::pair|std:: optional |std:: expected |nonstd:: expected |Boost. Expected |Nonco expected |Andrei Expected |Hagan required |
|----------------------|-------------|---------------|---------------|------------------|----------------|---------------|----------------|---------------|
|More information      | see [14]    | see [5]       | see [1]       | this work        | see [4]        | see [7]       | see [8]        | see [13]      |
|                      |             |               |               |                  |                |               |                |               |
| C++03                | yes         | no            | no            | no/not yet       | no (union)     | no            | no             | yes           |
| C++11                | yes         | no            | no            | yes              | yes            | yes           | yes            | yes           |
| C++14                | yes         | no            | no            | yes              | yes            | yes           | yes            | yes           |
| C++17                | yes         | yes           | no            | yes              | yes            | yes           | yes            | yes           |
|                      |             |               |               |                  |                |               |                |               |
|DefaultConstructible  | T param     | yes           | yes           | yes              | yes            | no            | no             | no            |
|In-place construction | no          | yes           | yes           | yes              | yes            | yes           | no             | no            |
|Literal type          | yes         | yes           | yes           | yes              | yes            | no            | no             | no            |
|                      |             |               |               |                  |                |               |                |               |
|Disengaged information| possible    | no            | yes           | yes              | yes            | yes           | yes            | no            |
|Vary disengaged type  | yes         | no            | yes           | yes              | yes            | no            | no             | no            |
|Engaged nonuse throws | no          | no            | no            | no               | error_traits   | no            | no             | yes           |
|Disengaged use throws | no          | yes, value()  | yes, value()  | yes, value()     | yes,<br>value()| yes,<br>get() | yes,<br>get()  | n/a           |
|                      |             |               |               |                  |                |               |                |               |
|Proxy (rel.ops)       | no          | yes           | yes           | yes              | yes            | no            | no             | no            |
|References            | no          | yes           | no/not yet    | no/not yet       | no/not yet     | yes           | no             | no            |
|Chained visitor(s)    | no          | no            | yes           | yes              | yes            | no            | no             | no            |

Note 1: std:&#58;*experimental*:&#58;expected

Note 2: sources for [Nonco expected](https://github.com/martinmoene/spike-expected/tree/master/nonco), [Andrei Expected](https://github.com/martinmoene/spike-expected/tree/master/alexandrescu) and [Hagan required](https://github.com/martinmoene/spike-expected/tree/master/hagan) can befound in the [spike-expected](https://github.com/martinmoene/spike-expected) repository.

## Reported to work with

TBD

## Implementation notes

TBD

## Other implementations of expected

- Simon Brand. [C++11/14/17 std::expected with functional-style extensions](https://github.com/TartanLlama/expected). Single-header.
- Isabella Muerte. [MNMLSTC Core](https://github.com/mnmlstc/core) (C++11).
- Vicente J. Botet Escriba. [stdmake's expected](https://github.com/viboes/std-make/tree/master/include/experimental/fundamental/v3/expected) (C++17).
- Facebook. [ Folly's Expected.h](https://github.com/facebook/folly/blob/master/folly/Expected.h) (C++14).

## Notes and references

[1] Vicente J. Botet Escriba. [p0323 - A proposal to add a utility class to represent expected object (latest)](http://wg21.link/p0323) (HTML). ([r12](http://wg21.link/p0323r12), [r11](http://wg21.link/p0323r11), [r10](http://wg21.link/p0323r10), [r9](http://wg21.link/p0323r9), [r8](http://wg21.link/p0323r8), [r7](http://wg21.link/p0323r7), [r6](http://wg21.link/p0323r6), [r5](http://wg21.link/p0323r5), [r4](http://wg21.link/p0323r4), [r3](http://wg21.link/p0323r3), [r2](http://wg21.link/p0323r2), [r1](http://wg21.link/n4109), [r0](http://wg21.link/n4015), [draft](https://github.com/viboes/std-make/blob/master/doc/proposal/expected/DXXXXR0_expected.pdf)).

[2] Vicente J. Botet Escriba. [JASEL: Just a simple experimental library for C++](https://github.com/viboes/std-make). Reference implementation of [expected](https://github.com/viboes/std-make/tree/master/include/experimental/fundamental/v3/expected).

[3] Vicente J. Botet Escriba. [Expected - An exception-friendly Error Monad](https://www.youtube.com/watch?v=Zdlt1rgYdMQ). C++Now 2014. 24 September 2014.  

[4] Pierre Talbot. [Boost.Expected. Unofficial Boost candidate](http://www.google-melange.com/gsoc/proposal/review/google/gsoc2013/trademark/25002). 5 May 2013. [GitHub](https://github.com/TrademarkPewPew/Boost.Expected), [GSoC 2013 Proposal](http://www.google-melange.com/gsoc/proposal/review/google/gsoc2013/trademark/25002), [boost@lists.boost.org](http://permalink.gmane.org/gmane.comp.lib.boost.devel/240056 ).  

[5] Fernando Cacciola and Andrzej Krzemieński. [A proposal to add a utility class to represent optional objects (Revision 4)](http://isocpp.org/files/papers/N3672.html). ISO/IEC JTC1 SC22 WG21 N3672 2013-04-19.  

[6] Andrzej Krzemieński, [Optional library implementation in C++11](https://github.com/akrzemi1/Optional/).  

[7] Anto Nonco. [Extending expected<T> to deal with references](http://anto-nonco.blogspot.nl/2013/03/extending-expected-to-deal-with.html). 27 May 2013.  

[8] Andrei Alexandrescu. Systematic Error Handling in C++. Prepared for The C++and Beyond Seminar 2012. [Video](http://channel9.msdn.com/Shows/Going+Deep/C-and-Beyond-2012-Andrei-Alexandrescu-Systematic-Error-Handling-in-C). [Slides](http://sdrv.ms/RXjNPR).  

[9] Andrei Alexandrescu. [Choose your Poison: Exceptions or Error Codes? (PDF)](http://accu.org/content/conf2007/Alexandrescu-Choose_Your_Poison.pdf). ACCU Conference 2007.  

[10] Andrei Alexandrescu. [The Power of None (PPT)](http://nwcpp.org/static/talks/2006/The_Power_of_None.ppt). Northwest C++ Users' Group. [May 17th, 2006](http://nwcpp.org/may-2006.html).  

[11] Jon Jagger. [A Return Type That Doesn't Like Being Ignored](http://accu.org/var/uploads/journals/overload53-FINAL.pdf#page=18). Overload issue 53, February 2003.  

[12] Andrei Alexandrescu. [Error Handling in C++: Are we inching towards a total solution?](http://accu.org/index.php/conferences/2002/speakers2002). ACCU Conference 2002.  

[13] Ken Hagan et al. [Exploding return codes](https://groups.google.com/d/msg/comp.lang.c++.moderated/BkZqPfoq3ys/H_PMR8Sat4oJ). comp.lang.c++.moderated. 11 February 2000.  

[14] [std::pair](http://en.cppreference.com/w/cpp/utility/pair). cppreference.com

[15] Niall Douglas. [Outcome](https://ned14.github.io/outcome/). Very lightweight outcome&lt;T> and result&lt;T> (non-Boost edition). 

[16] Niall Douglas. [p0762 - Concerns about expected&lt;T, E> from the Boost.Outcome peer review](http://wg21.link/p0762). 15 October 2017.

[17] Jeff Garland. [p2505 - Monadic Functions for `std::expected`](http://wg21.link/p2505) (HTML). ([r0](http://wg21.link/p2505r0), [r1](http://wg21.link/p2505r1), [r2](http://wg21.link/p2505r2), [r3](http://wg21.link/p2505r3), [r4](http://wg21.link/p2505r4), [r5](http://wg21.link/p2505r5)).

## Appendix

### A.1 Compile-time information

The version of *expected lite* is available via tag `[.version]`. The following tags are available for information on the compiler and on the C++ standard library used: `[.compiler]`, `[.stdc++]`, `[.stdlanguage]` and `[.stdlibrary]`.

### A.2 Expected lite test specification

<details>
<summary>click to expand</summary>
<p>

```Text
unexpected_type: Disallows default construction
unexpected_type: Allows to copy-construct from unexpected_type, default
unexpected_type: Allows to move-construct from unexpected_type, default
unexpected_type: Allows to in-place-construct
unexpected_type: Allows to in-place-construct from initializer_list
unexpected_type: Allows to copy-construct from error_type
unexpected_type: Allows to move-construct from error_type
unexpected_type: Allows to copy-construct from unexpected_type, explicit converting
unexpected_type: Allows to copy-construct from unexpected_type, non-explicit converting
unexpected_type: Allows to move-construct from unexpected_type, explicit converting
unexpected_type: Allows to move-construct from unexpected_type, non-explicit converting
unexpected_type: Allows to copy-assign from unexpected_type, default
unexpected_type: Allows to move-assign from unexpected_type, default
unexpected_type: Allows to copy-assign from unexpected_type, converting
unexpected_type: Allows to move-assign from unexpected, converting
unexpected_type: Allows to observe its value via a l-value reference
unexpected_type: Allows to observe its value via a r-value reference
unexpected_type: Allows to modify its value via a l-value reference
unexpected_type: Allows to be swapped
unexpected_type<std::exception_ptr>: Disallows default construction
unexpected_type<std::exception_ptr>: Allows to copy-construct from error_type
unexpected_type<std::exception_ptr>: Allows to move-construct from error_type
unexpected_type<std::exception_ptr>: Allows to copy-construct from an exception
unexpected_type<std::exception_ptr>: Allows to observe its value
unexpected_type<std::exception_ptr>: Allows to modify its value
unexpected_type: Provides relational operators
unexpected_type: Provides relational operators, std::exception_ptr specialization
make_unexpected(): Allows to create an unexpected_type<E> from an E
make_unexpected(): Allows to in-place create an unexpected_type<E> from an E
unexpected: C++17 and later provide unexpected_type as unexpected
bad_expected_access: Disallows default construction
bad_expected_access: Allows construction from error_type
bad_expected_access: Allows to observe its error
bad_expected_access: Allows to change its error
bad_expected_access: Provides non-empty what()
expected: Allows to default construct
expected: Allows to default construct from noncopyable, noncopyable value type
expected: Allows to default construct from noncopyable, noncopyable error type
expected: Allows to copy-construct from expected: value
expected: Allows to copy-construct from expected: error
expected: Allows to move-construct from expected: value
expected: Allows to move-construct from expected: error
expected: Allows to copy-construct from expected; value, explicit converting
expected: Allows to copy-construct from expected; error, explicit converting
expected: Allows to copy-construct from expected; value, non-explicit converting
expected: Allows to copy-construct from expected; error, non-explicit converting
expected: Allows to move-construct from expected; value, explicit converting
expected: Allows to move-construct from expected; error, explicit converting
expected: Allows to move-construct from expected; value, non-explicit converting
expected: Allows to move-construct from expected; error, non-explicit converting
expected: Allows to forward-construct from value, explicit converting
expected: Allows to forward-construct from value, non-explicit converting
expected: Allows to in-place-construct value
expected: Allows to in-place-construct value from initializer_list
expected: Allows to copy-construct from unexpected, explicit converting
expected: Allows to copy-construct from unexpected, non-explicit converting
expected: Allows to move-construct from unexpected, explicit converting
expected: Allows to move-construct from unexpected, non-explicit converting
expected: Allows to in-place-construct error
expected: Allows to in-place-construct error from initializer_list
expected: Allows to copy-assign from expected, value
expected: Allows to copy-assign from expected, error
expected: Allows to move-assign from expected, value
expected: Allows to move-assign from expected, error
expected: Allows to forward-assign from value
expected: Allows to copy-assign from unexpected
expected: Allows to move-assign from unexpected
expected: Allows to move-assign from move-only unexpected
expected: Allows to emplace value
expected: Allows to emplace value from initializer_list
expected: Allows to be swapped
expected: Allows to observe its value via a pointer
expected: Allows to observe its value via a pointer to constant
expected: Allows to modify its value via a pointer
expected: Allows to observe its value via a l-value reference
expected: Allows to observe its value via a r-value reference
expected: Allows to modify its value via a l-value reference
expected: Allows to modify its value via a r-value reference
expected: Allows to observe if it contains a value (or error)
expected: Allows to observe its value
expected: Allows to modify its value
expected: Allows to move its value
expected: Allows to observe its error
expected: Allows to modify its error
expected: Allows to move its error
expected: Allows to observe its error as unexpected
expected: Allows to query if it contains an exception of a specific base type
expected: Allows to observe its value if available, or obtain a specified value otherwise
expected: Allows to move its value if available, or obtain a specified value otherwise
expected: Throws bad_expected_access on value access when disengaged
expected: Allows to observe its unexpected value, or fallback to the specified value with error_or [monadic p2505r4]
expected: Allows to map value with and_then [monadic p2505r3]
expected: Allows to map unexpected with or_else [monadic p2505r3]
expected: Allows to transform value [monadic p2505r3]
expected: Allows to map errors with transform_error [monadic p2505r3]
expected<void>: Allows to default-construct
expected<void>: Allows to copy-construct from expected<void>: value
expected<void>: Allows to copy-construct from expected<void>: error
expected<void>: Allows to move-construct from expected<void>: value
expected<void>: Allows to move-construct from expected<void>: error
expected<void>: Allows to in-place-construct
expected<void>: Allows to copy-construct from unexpected, explicit converting
expected<void>: Allows to copy-construct from unexpected, non-explicit converting
expected<void>: Allows to move-construct from unexpected, explicit converting
expected<void>: Allows to move-construct from unexpected, non-explicit converting
expected<void>: Allows to in-place-construct unexpected_type
expected<void>: Allows to in-place-construct error from initializer_list
expected<void>: Allows to copy-assign from expected, value
expected<void>: Allows to copy-assign from expected, error
expected<void>: Allows to move-assign from expected, value
expected<void>: Allows to move-assign from expected, error
expected<void>: Allows to emplace value
expected<void>: Allows to be swapped
expected<void>: Allows to observe if it contains a value (or error)
expected<void>: Allows to observe its value
expected<void>: Allows to observe its error
expected<void>: Allows to modify its error
expected<void>: Allows to move its error
expected<void>: Allows to observe its error as unexpected
expected<void>: Allows to query if it contains an exception of a specific base type
expected<void>: Throws bad_expected_access on value access when disengaged
expected<void>: Allows to observe unexpected value, or fallback to a default value with error_or [monadic p2505r4]
expected<void>: Allows to call argless functions with and_then [monadic p2505r3]
expected<void>: Allows to map to expected or unexpected with or_else [monadic p2505r3]
expected<void>: Allows to assign a new expected value using transform [monadic p2505r3]
expected<void>: Allows to map unexpected error value via transform_error [monadic p2505r3]
operators: Provides expected relational operators
operators: Provides expected relational operators (void)
swap: Allows expected to be swapped
std::hash: Allows to compute hash value for expected
tweak header: reads tweak header if supported [tweak]
```

</p>
</details>

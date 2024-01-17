## AWS C Common


[![GitHub](https://img.shields.io/github/license/awslabs/aws-c-common.svg)](https://github.com/awslabs/aws-c-common/blob/main/LICENSE)

Core c99 package for AWS SDK for C. Includes cross-platform primitives, configuration, data structures, and error handling.

## License

This library is licensed under the Apache 2.0 License.

## Usage
### Building
aws-c-common uses CMake for setting up build environments. This library has no non-kernel dependencies so the build is quite
simple.

For example:

    git clone git@github.com:awslabs/aws-c-common.git aws-c-common
    mkdir aws-c-common-build
    cd aws-c-common-build
    cmake ../aws-c-common
    make -j 12
    make test
    sudo make install

Keep in mind that CMake supports multiple build systems, so for each platform you can pass your own build system
as the `-G` option. For example:

    cmake -GNinja ../aws-c-common
    ninja build
    ninja test
    sudo ninja install

Or on windows,

    cmake -G "Visual Studio 14 2015 Win64" ../aws-c-common
    msbuild.exe ALL_BUILD.vcproj

### CMake Options
* -DCMAKE_CLANG_TIDY=/path/to/clang-tidy (or just clang-tidy or clang-tidy-7.0 if it is in your PATH) - Runs clang-tidy as part of your build.
* -DENABLE_SANITIZERS=ON - Enables gcc/clang sanitizers, by default this adds -fsanitizer=address,undefined to the compile flags for projects that call aws_add_sanitizers.
* -DENABLE_FUZZ_TESTS=ON - Includes fuzz tests in the unit test suite. Off by default, because fuzz tests can take a long time. Set -DFUZZ_TESTS_MAX_TIME=N to determine how long to run each fuzz test (default 60s).
* -DCMAKE_INSTALL_PREFIX=/path/to/install - Standard way of installing to a user defined path. If specified when configuring aws-c-common, ensure the same prefix is specified when configuring other aws-c-* SDKs.
* -DSTATIC_CRT=ON - On MSVC, use /MT(d) to link MSVCRT

### API style and conventions
Every API has a specific set of styles and conventions. We'll outline them here. These conventions are followed in every
library in the AWS C SDK ecosystem.

#### Error handling
Every function that returns an `int` type, returns `AWS_OP_SUCCESS` ( 0 ) or `AWS_OP_ERR` (-1) on failure. To retrieve
the error code, use the function `aws_last_error()`. Each error code also has a corresponding error string that can be
accessed via the `aws_error_str()` function.

In addition, you can install both a global and a thread local error handler by using the `aws_set_global_error_handler_fn()`
and `aws_set_thread_local_error_handler_fn()` functions.

All error functions are in the `include/aws/common/error.h` header file.

#### Naming
Any function that allocates and initializes an object will be suffixed with `new` (e.g. `aws_myobj_new()`). Similarly, these objects will always
have a corresponding function with a `destroy` suffix. The `new` functions will return the allocated object
on success and `NULL` on failure. To respond to the error, call `aws_last_error()`. If several `new` or `destroy`
functions are available, the variants should be named like `new_x` or `destroy_x` (e.g. `aws_myobj_new_copy()` or `aws_myobj_destroy_secure()`).

Any function that initializes an existing object will be suffixed with `init` (e.g. `aws_myobj_init()`. These objects will have a corresponding
`clean_up` function if necessary. In these cases, you are responsible for making the decisions for how your object is
allocated. The `init` functions return `AWS_OP_SUCCESS` ( 0 ) or `AWS_OP_ERR` (-1) on failure. If several `init` or
`clean_up` functions are available, they should be named like `init_x` or `clean_up_x` (e.g. `aws_myobj_init_static()` or
`aws_myobj_clean_up_secure()`).

## Contributing

If you are contributing to this code-base, first off, THANK YOU!. There are a few things to keep in mind to minimize the
pull request turn around time.

### Coding "guidelines"
These "guidelines" are followed in every library in the AWS C SDK ecosystem.

#### Memory Management
* All APIs that need to be able to allocate memory, must take an instance of `aws_allocator` and use that. No `malloc()` or
`free()` calls should be made directly.
* If an API does not allocate the memory, it does not free it. All allocations and deallocations should take place at the same level.
For example, if a user allocates memory, the user is responsible for freeing it. There will inevitably be a few exceptions to this
rule, but they will need significant justification to make it through the code-review.
* All functions that allocate memory must raise an `AWS_ERROR_OOM` error code upon allocation failures. If it is a `new()` function
it should return NULL. If it is an `init()` function, it should return `AWS_OP_ERR`.

#### Threading
* Occasionally a thread is necessary. In those cases, prefer for memory not to be shared between threads. If memory must cross
a thread barrier it should be a complete ownership hand-off. Bias towards, "if I need a mutex, I'm doing it wrong".
* Do not sleep or block .... ever .... under any circumstances, in non-test-code.
* Do not expose blocking APIs.

### Error Handling
* For APIs returning an `int` error code. The only acceptable return types are `AWS_OP_SUCCESS` and `AWS_OP_ERR`. Before
returning control to the caller, if you have an error to raise, use the `aws_raise_error()` function.
* For APIs returning an allocated instance of an object, return the memory on success, and `NULL` on failure. Before
returning control to the caller, if you have an error to raise, use the `aws_raise_error()` function.

#### Log Subjects & Error Codes
The logging & error handling infrastructure is designed to support multiple libraries. For this to work, AWS maintained libraries
have pre-slotted log subjects & error codes for each library. The currently allocated ranges are:

| Range | Library Name |
| --- | --- |
| [0x0000, 0x0400) | aws-c-common |
| [0x0400, 0x0800) | aws-c-io |
| [0x0800, 0x0C00) | aws-c-http |
| [0x0C00, 0x1000) | aws-c-compression |
| [0x1000, 0x1400) | aws-c-eventstream |
| [0x1400, 0x1800) | aws-c-mqtt |
| [0x1800, 0x1C00) | aws-c-auth |
| [0x1C00, 0x2000) | aws-c-cal |
| [0x2000, 0x2400) | aws-crt-cpp |
| [0x2400, 0x2800) | aws-crt-java |
| [0x2800, 0x2C00) | aws-crt-python |
| [0x2C00, 0x3000) | aws-crt-nodejs |
| [0x3000, 0x3400) | aws-crt-dotnet |
| [0x3400, 0x3800) | aws-c-iot |
| [0x3800, 0x3C00) | aws-c-s3 |
| [0x3C00, 0x4000) | aws-c-sdkutils |
| [0x4000, 0x4400) | (reserved for future project) |
| [0x4400, 0x4800) | (reserved for future project) |

Each library should begin its error and log subject values at the beginning of its range and follow in sequence (don't skip codes). Upon
adding an AWS maintained library, a new enum range must be approved and added to the above table.

### Testing
We have a high bar for test coverage, and PRs fixing bugs or introducing new functionality need to have tests before
they will be accepted. A couple of tips:

#### Aws Test Harness
We provide a test harness for writing unit tests. This includes an allocator that will fail your test if you have any
memory leaks, as well as some `ASSERT` macros. To write a test:

* Create a *.c test file in the tests directory of the project.
* Implement one or more tests with the signature `int test_case_name(struct aws_allocator *, void *ctx)`
* Use the `AWS_TEST_CASE` macro to declare the test.
* Include your test in the `tests/main.c` file.
* Include your test in the `tests/CMakeLists.txt` file.

### Coding Style
* No Tabs.
* Indent is 4 spaces.
* K & R style for braces.
* Space after if, before the `(`.
* `else` and `else if` stay on the same line as the closing brace.

Example:

    if (condition) {
        do_something();
    } else {
        do_something_else();
    }
* Avoid C99 features in header files. For some types such as bool, uint32_t etc..., these are defined if not available for the language
standard being used in `aws/common/common.h`, so feel free to use them.
* For C++ compatibility, don't put const members in structs.
* Avoid C++ style comments e.g. `//`.
* All public API functions need C++ guards and Windows dll semantics.
* Use Unix line endings.
* Where implementation hiding is desired for either ABI or runtime polymorphism reasons, use the `void *impl` pattern. v-tables
 should be the last member in the struct.
* For #ifdef, put a # as the first character on the line and then indent the compilation branches.

Example:


    #ifdef FOO
        do_something();

    #   ifdef BAR
        do_something_else();
    #   endif
    #endif


* For all error code names with the exception of aws-c-common, use `AWS_ERROR_<lib name>_<error name>`.
* All error strings should be written using correct English grammar.
* SNAKE_UPPER_CASE constants, macros, and enum members.
* snake_lower_case everything else.
* `static` (local file scope) variables that are not `const` are prefixed by `s_` and lower snake case.
* Global variables not prefixed as `const` are prefixed by `g_` and lower snake case.
* Thread local variables are prefixed as `tl_` and lower snake case.
* Macros and `const` variables are upper snake case.
* For constants, prefer anonymous enums.
* Don't typedef structs. It breaks forward declaration ability.
* Don't typedef enums. It breaks forward declaration ability.
* typedef function definitions for use as function pointers as values and suffixed with _fn.

    Do this:

        typedef int(fn_name_fn)(void *);

    Not this:

        typedef int(*fn_name_fn)(void *);
        
* If a callback may be async, then always have it be async.
  Callbacks that are sometimes async and sometimes sync are hard to code around and lead to bugs
  (see [this blog post](https://blog.ometer.com/2011/07/24/callbacks-synchronous-and-asynchronous/)).
  Unfortunately many callbacks in this codebase currently violate this rule,
  so be careful. But do not add any more.
* Every source and header file must have a copyright header (The standard AWS one for apache 2).
* Use standard include guards (e.g. #IFNDEF HEADER_NAME #define HEADER_NAME etc...).
* Include order should be:
    the header for the translation unit for the .c file
    newline
    header files in a directory in alphabetical order
    newline
    header files not in a directory (system and stdlib headers)
* Platform specifics should be handled in c files and partitioned by directory.
* Do not use `extern inline`. It's too unpredictable between compiler versions and language standards.
* Namespace all definitions in header files with `aws_<libname>?_<api>_<what it does>`. Lib name is
not always required if a conflict is not likely and it provides better ergonomics.
* `init`, `clean_up`, `new`, `destroy` are suffixed to the function names for their object.

Example:

    AWS_COMMON_API
 int aws_module_init(aws_module_t *module);
    AWS_COMMON_API
 void aws_module_clean_up(aws_module_t *module);
    AWS_COMMON_API
 aws_module_t *aws_module_new(aws_allocator_t *allocator);
    AWS_COMMON_API
 void aws_module_destroy(aws_module_t *module);

* Avoid c-strings, and don't write code that depends on `NULL` terminators. Expose `struct aws_byte_buf` APIs
and let the user figure it out.
* There is only one valid character encoding-- UTF-8. Try not to ever need to care about character encodings, but
where you do, the working assumption should always be UTF-8 unless it's something we don't get a choice in (e.g. a protocol
explicitly mandates a character set).
* If you are adding/using a compiler specific keyword, macro, or intrinsic, hide it behind a platform independent macro
definition. This mainly applies to header files. Obviously, if you are writing a file that will only be built on a certain
platform, you have more liberty on this.
* When checking more than one error condition, check and log each condition separately with a unique message.

    Do this:
    
        if (options->callback == NULL) {
            AWS_LOGF_ERROR(AWS_LS_SOME_SUBJECT, "Invalid options - callback is null");
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }

        if (options->allocator == NULL) {
            AWS_LOGF_ERROR(AWS_LS_SOME_SUBJECT, "Invalid options - allocator is null");
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }

    Not this:
    
        if (options->callback == NULL || options->allocator == NULL) {
            AWS_LOGF_ERROR(AWS_LS_SOME_SUBJECT, "Invalid options - something is null");
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }

## CBMC

To learn more about CBMC and proofs specifically, review the training material [here](https://model-checking.github.io/cbmc-training).

The `verification/cbmc/proofs` directory contains CBMC proofs.

In order to run these proofs you will need to install CBMC and other tools by following the instructions [here](https://model-checking.github.io/cbmc-training/installation.html).

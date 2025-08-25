# [Boost.Tokenizer](https://boost.org/libs/tokenizer)

Boost.Tokenizer is a part of [Boost C++ Libraries](https://github.com/boostorg).  The Boost.Tokenizer package provides a flexible and easy-to-use way to break a string or other character sequence into a series of tokens.

## License

Distributed under the [Boost Software License, Version 1.0](https://www.boost.org/LICENSE_1_0.txt).

## Properties

* C++03
* Header-Only

## Build Status

<!-- boost-ci/tools/makebadges.sh --project tokenizer --appveyor rpqpywvv4l4637qy --codecov sakwglU1PC --coverity 15854 -->
| Branch          | GHA CI | Appveyor | Coverity Scan | codecov.io | Deps | Docs | Tests |
| :-------------: | ------ | -------- | ------------- | ---------- | ---- | ---- | ----- |
| [`master`](https://github.com/boostorg/tokenizer/tree/master) | [![Build Status](https://github.com/boostorg/tokenizer/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/boostorg/tokenizer/actions?query=branch:master) | [![Build status](https://ci.appveyor.com/api/projects/status/rpqpywvv4l4637qy/branch/master?svg=true)](https://ci.appveyor.com/project/cppalliance/tokenizer/branch/master) | [![Coverity Scan Build Status](https://scan.coverity.com/projects/15854/badge.svg)](https://scan.coverity.com/projects/boostorg-tokenizer) | [![codecov](https://codecov.io/gh/boostorg/tokenizer/branch/master/graph/badge.svg?token=sakwglU1PC)](https://codecov.io/gh/boostorg/tokenizer/tree/master) | [![Deps](https://img.shields.io/badge/deps-master-brightgreen.svg)](https://pdimov.github.io/boostdep-report/master/tokenizer.html) | [![Documentation](https://img.shields.io/badge/docs-master-brightgreen.svg)](https://www.boost.org/doc/libs/master/libs/tokenizer) | [![Enter the Matrix](https://img.shields.io/badge/matrix-master-brightgreen.svg)](https://www.boost.org/development/tests/master/developer/tokenizer.html)
| [`develop`](https://github.com/boostorg/tokenizer/tree/develop) | [![Build Status](https://github.com/boostorg/tokenizer/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/boostorg/tokenizer/actions?query=branch:develop) | [![Build status](https://ci.appveyor.com/api/projects/status/rpqpywvv4l4637qy/branch/develop?svg=true)](https://ci.appveyor.com/project/cppalliance/tokenizer/branch/develop) | [![Coverity Scan Build Status](https://scan.coverity.com/projects/15854/badge.svg)](https://scan.coverity.com/projects/boostorg-tokenizer) | [![codecov](https://codecov.io/gh/boostorg/tokenizer/branch/develop/graph/badge.svg?token=sakwglU1PC)](https://codecov.io/gh/boostorg/tokenizer/tree/develop) | [![Deps](https://img.shields.io/badge/deps-develop-brightgreen.svg)](https://pdimov.github.io/boostdep-report/develop/tokenizer.html) | [![Documentation](https://img.shields.io/badge/docs-develop-brightgreen.svg)](https://www.boost.org/doc/libs/develop/libs/tokenizer) | [![Enter the Matrix](https://img.shields.io/badge/matrix-develop-brightgreen.svg)](https://www.boost.org/development/tests/develop/developer/tokenizer.html)

## Overview

> break up a phrase into words.

 <a target="_blank" href="https://melpon.org/wandbox/permlink/kZeKmQAtqDlpn8if">![Try it online][badge.wandbox]</a>

```c++
#include <iostream>
#include <boost/tokenizer.hpp>
#include <string>

int main(){
    std::string s = "This is,  a test";
    typedef boost::tokenizer<> Tok;
    Tok tok(s);
    for (Tok::iterator beg = tok.begin(); beg != tok.end(); ++beg){
        std::cout << *beg << "\n";
    }
}

```

> Using Range-based for loop (C++11 or later)

 <a target="_blank" href="https://melpon.org/wandbox/permlink/z94YLs8PdYSh7rXz">![Try it online][badge.wandbox]</a>
```c++
#include <iostream>
#include <boost/tokenizer.hpp>
#include <string>

int main(){
    std::string s = "This is,  a test";
    boost::tokenizer<> tok(s);
    for (auto token: tok) {
        std::cout << token << "\n";
    }
}
```

## Related Material

[Boost.Tokenizer](https://theboostcpplibraries.com/boost.tokenizer) Chapter 10 at theboostcpplibraries.com, contains several examples including **escaped_list_separator**.

## Acknowledgements
>From the author:
>
I wish to thank the members of the boost mailing list, whose comments, compliments, and criticisms during both the development and formal review helped make the Tokenizer library what it is. I especially wish to thank Aleksey Gurtovoy for the idea of using a pair of iterators to specify the input, instead of a string. I also wish to thank Jeremy Siek for his idea of providing a container interface for the token iterators and for simplifying the template parameters for the TokenizerFunctions. He and Daryle Walker also emphasized the need to separate interface and implementation. Gary Powell sparked the idea of using the isspace and ispunct as the defaults for char_delimiters_separator. Jeff Garland provided ideas on how to change to order of the template parameters in order to make tokenizer easier to declare. Thanks to Douglas Gregor who served as review manager and provided many insights both on the boost list and in e-mail on how to polish up the implementation and presentation of Tokenizer. Finally, thanks to Beman Dawes who integrated the final version into the boost distribution.

## Directories

| Name        | Purpose                        |
| ----------- | ------------------------------ |
| `example`   | examples                       |
| `include`   | header                         |
| `test`      | unit tests                     |

## More information

* [Ask questions](https://stackoverflow.com/questions/ask?tags=c%2B%2B,boost,boost-tokenizer)
* [Report bugs](https://github.com/boostorg/tokenizer/issues): Be sure to mention Boost version, platform and compiler you're using. A small compilable code sample to reproduce the problem is always good as well.
* Submit your patches as pull requests against **develop** branch. Note that by submitting patches you agree to license your modifications under the [Boost Software License, Version 1.0](https://www.boost.org/LICENSE_1_0.txt).
* Discussions about the library are held on the [Boost developers mailing list](https://www.boost.org/community/groups.html#main). Be sure to read the [discussion policy](https://www.boost.org/community/policy.html) before posting and add the `[tokenizer]` tag at the beginning of the subject line.
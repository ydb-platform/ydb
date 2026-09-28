DateTime, part of the collection of [Boost C++ Libraries](http://github.com/boostorg), makes programming with dates and times as simple and natural as programming with strings and integers. 

### License

Distributed under the [Boost Software License, Version 1.0](http://www.boost.org/LICENSE_1_0.txt).

### Properties

* C++11
* Header only

### Build Status

<!-- boost-ci/tools/makebadges.sh --repo date_time --appveyorbadge upf5c528fy09fudk --codecovbadge nDoh7t8f6g --coverity 14908 -->
| Branch          | GHA CI | Appveyor | Coverity Scan | codecov.io | Deps | Docs | Tests |
| :-------------: | ------ | -------- | ------------- | ---------- | ---- | ---- | ----- |
| [`master`](https://github.com/boostorg/date_time/tree/master) | [![Build Status](https://github.com/boostorg/date_time/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/boostorg/date_time/actions?query=branch:master) | [![Build status](https://ci.appveyor.com/api/projects/status/upf5c528fy09fudk/branch/master?svg=true)](https://ci.appveyor.com/project/cppalliance/date-time/branch/master) | [![Coverity Scan Build Status](https://scan.coverity.com/projects/14908/badge.svg)](https://scan.coverity.com/projects/boostorg-date_time) | [![codecov](https://codecov.io/gh/boostorg/date_time/branch/master/graph/badge.svg?token=nDoh7t8f6g)](https://codecov.io/gh/boostorg/date_time/tree/master) | [![Deps](https://img.shields.io/badge/deps-master-brightgreen.svg)](https://pdimov.github.io/boostdep-report/master/date_time.html) | [![Documentation](https://img.shields.io/badge/docs-master-brightgreen.svg)](https://www.boost.org/doc/libs/master/libs/date_time) | [![Enter the Matrix](https://img.shields.io/badge/matrix-master-brightgreen.svg)](https://www.boost.org/development/tests/master/developer/date_time.html) |
| [`develop`](https://github.com/boostorg/date_time/tree/develop) | [![Build Status](https://github.com/boostorg/date_time/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/boostorg/date_time/actions?query=branch:develop) | [![Build status](https://ci.appveyor.com/api/projects/status/upf5c528fy09fudk/branch/develop?svg=true)](https://ci.appveyor.com/project/cppalliance/date-time/branch/develop) | [![Coverity Scan Build Status](https://scan.coverity.com/projects/14908/badge.svg)](https://scan.coverity.com/projects/boostorg-date_time) | [![codecov](https://codecov.io/gh/boostorg/date_time/branch/develop/graph/badge.svg?token=nDoh7t8f6g)](https://codecov.io/gh/boostorg/date_time/tree/develop) | [![Deps](https://img.shields.io/badge/deps-develop-brightgreen.svg)](https://pdimov.github.io/boostdep-report/develop/date_time.html) | [![Documentation](https://img.shields.io/badge/docs-develop-brightgreen.svg)](https://www.boost.org/doc/libs/develop/libs/date_time) | [![Enter the Matrix](https://img.shields.io/badge/matrix-develop-brightgreen.svg)](https://www.boost.org/development/tests/develop/developer/date_time.html) |

### Directories

Note that the built library is only for build backward compatibility and contains no symbols.  date_time is now header only.

| Name      | Purpose                                 |
| --------- | --------------------------------------- |
| `build`   | build script for optional lib build     |
| `data`    | timezone database                       |
| `doc`     | documentation                           |
| `example` | use case examples                       |
| `include` | headers                                 |
| `src`     | source code for optional link library   |
| `test`    | unit tests                              |
| `xmldoc`  | documentation source                    |

### More information

* [Ask questions](http://stackoverflow.com/questions/ask?tags=c%2B%2B,boost,boost-date_time): Be sure to read the documentation first to see if it answers your question.
* [Report bugs](https://github.com/boostorg/date_time/issues): Be sure to mention Boost version, platform and compiler you're using. A small compilable code sample to reproduce the problem is always good as well.
* [Submit Pull Requests](https://github.com/boostorg/date_time/pulls) against the **develop** branch. Note that by submitting patches you agree to license your modifications under the [Boost Software License, Version 1.0](http://www.boost.org/LICENSE_1_0.txt).  Be sure to include tests proving your changes work properly.
* Discussions about the library are held on the [Boost developers mailing list](http://www.boost.org/community/groups.html#main). Be sure to read the [discussion policy](http://www.boost.org/community/policy.html) before posting and add the `[date_time]` tag at the beginning of the subject line.

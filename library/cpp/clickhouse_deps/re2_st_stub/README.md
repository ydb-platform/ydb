re2_st is a modification of re2 library, which is generated during ClickHouse build:

https://github.com/ClickHouse/ClickHouse/blob/master/contrib/re2-cmake/CMakeLists.txt#L52
https://github.com/ClickHouse/ClickHouse/blob/master/contrib/re2-cmake/re2_transform.cmake

This library mimics re2_st interface via original re2 library, so we can build ClickHouse properly in Arcadia.

This library should not be used outside of ClickHouse build.

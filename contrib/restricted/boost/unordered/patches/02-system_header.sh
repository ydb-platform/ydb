set -xue

echo -e '#pragma clang system_header\n' > _
cat include/boost/unordered/detail/implementation.hpp >> _
mv _ include/boost/unordered/detail/implementation.hpp

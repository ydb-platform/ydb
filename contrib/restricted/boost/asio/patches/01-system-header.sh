set -xue
echo '#pragma clang system_header\n' > _
cat include/boost/asio/detail/is_executor.hpp >> _
mv _ include/boost/asio/detail/is_executor.hpp

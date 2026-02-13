set -xue

prepend_pragma() {
  echo "#pragma clang system_header" > _
  echo >> _
  cat "$1" >> _
  mv _ "$1"
}

prepend_pragma include/boost/asio/detail/is_executor.hpp
prepend_pragma include/boost/asio/impl/connect.hpp
prepend_pragma include/boost/asio/ssl/detail/io.hpp
prepend_pragma include/boost/asio/coroutine.hpp
prepend_pragma include/boost/asio/ip/impl/network_v4.ipp

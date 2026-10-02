#!/bin/sh

set -xue

prepend_pragma() {
  echo "#pragma clang system_header" > _
  echo >> _
  cat "$1" >> _
  mv _ "$1"
}

prepend_pragma include/boost/property_tree/json_parser/detail/standard_callbacks.hpp

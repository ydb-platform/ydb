#!/bin/sh

set -xue

prepend_pragma() {
  echo "#pragma clang system_header" > _
  echo >> _
  cat "$1" >> _
  mv _ "$1"
}

prepend_pragma include/boost/date_time/time_facet.hpp

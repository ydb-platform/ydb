#!/bin/sh

sed -e 's|std::vector<Rule>::iterator::difference_type|std::iterator_traits<std::vector<Rule>::iterator>::difference_type|' \
    -i cpp/src/arrow/vendored/datetime/tz.cpp

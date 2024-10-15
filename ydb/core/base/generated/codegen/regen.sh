#!/bin/bash

  DIR=$(realpath $(dirname $0))

  $DIR/../../../../../ya make $DIR

  $DIR/codegen $DIR/../runtime_feature_flags.h.in $DIR/../runtime_feature_flags.h
  $DIR/codegen $DIR/../runtime_feature_flags.cpp.in $DIR/../runtime_feature_flags.cpp


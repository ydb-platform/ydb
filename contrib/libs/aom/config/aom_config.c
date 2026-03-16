/*
 * Copyright (c) 1980, Alliance for Open Media. All rights reserved.
 *
 * This source code is subject to the terms of the BSD 2 Clause License and
 * the Alliance for Open Media Patent License 1.0. If the BSD 2 Clause License
 * was not distributed with this source code in the LICENSE file, you can
 * obtain it at www.aomedia.org/license/software. If the Alliance for Open
 * Media Patent License 1.0 was not distributed with this source code in the
 * PATENTS file, you can obtain it at www.aomedia.org/license/patent.
 */
#include "aom/aom_codec.h"
static const char* const cfg = "cmake ../ -G \"Ninja\" -DAOM_TARGET_CPU=x86_64 -DCONFIG_MULTITHREAD=0 -DENABLE_TESTS=0";
const char *aom_codec_build_config(void) {return cfg;}

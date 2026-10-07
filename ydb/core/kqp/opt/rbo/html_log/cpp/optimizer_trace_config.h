// optimizer_trace_config.h
#ifndef OPTIMIZER_TRACE_CONFIG_H
#define OPTIMIZER_TRACE_CONFIG_H

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <cassert>
#include <utility>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <unordered_set>
#include <variant>

#if defined(OPTIMIZER_TRACE_ENABLE_BROTLI)
#  if defined(__has_include)
#    if __has_include(<brotli/encode.h>)
#      include <brotli/encode.h>
#      define OPTIMIZER_TRACE_HTML_HAS_LIBBROTLI 1
#    endif
#  endif
#endif

#ifndef OPTIMIZER_TRACE_HTML_HAS_LIBBROTLI
#  define OPTIMIZER_TRACE_HTML_HAS_LIBBROTLI 0
#endif

#ifndef OPTIMIZER_TRACE_HTML_HAS_APPLE_BROTLI
#  define OPTIMIZER_TRACE_HTML_HAS_APPLE_BROTLI 0
#endif

#if OPTIMIZER_TRACE_HTML_HAS_LIBBROTLI || OPTIMIZER_TRACE_HTML_HAS_APPLE_BROTLI
#  define OPTIMIZER_TRACE_HTML_HAS_BROTLI 1
#else
#  define OPTIMIZER_TRACE_HTML_HAS_BROTLI 0
#endif

#endif // OPTIMIZER_TRACE_CONFIG_H

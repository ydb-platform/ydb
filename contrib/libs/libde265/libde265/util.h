/*
 * H.265 video codec.
 * Copyright (c) 2013-2014 struktur AG, Dirk Farin <farin@struktur.de>
 *
 * This file is part of libde265.
 *
 * libde265 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * libde265 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with libde265.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DE265_UTIL_H
#define DE265_UTIL_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef _MSC_VER
#include <inttypes.h>
#endif

#include <stdio.h>
#include <string>

#include "libde265/de265.h"

#ifdef __GNUC__
#define GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#endif

#ifdef _MSC_VER
#define LIBDE265_DECLARE_ALIGNED( var, n ) __declspec(align(n)) var
#define likely(x)      (x)
#define unlikely(x)    (x)
#else
#define LIBDE265_DECLARE_ALIGNED( var, n ) var __attribute__((aligned(n)))
#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)
#endif

#if defined(__GNUC__) && (__GNUC__ >= 4)
#define LIBDE265_CHECK_RESULT __attribute__ ((warn_unused_result))
#elif defined(_MSC_VER) && (_MSC_VER >= 1700)
#define LIBDE265_CHECK_RESULT _Check_return_
#else
#define LIBDE265_CHECK_RESULT
#endif

// Be careful with these alignment instructions. They only specify the alignment within
// a struct. But they cannot make sure that the base address of the struct has the same alignment
// when it is dynamically allocated.
#define ALIGNED_32( var ) LIBDE265_DECLARE_ALIGNED( var, 32 )
#define ALIGNED_16( var ) LIBDE265_DECLARE_ALIGNED( var, 16 )
#define ALIGNED_8( var )  LIBDE265_DECLARE_ALIGNED( var, 8 )
#define ALIGNED_4( var )  LIBDE265_DECLARE_ALIGNED( var, 4 )

// C++11 specific features
#if defined(_MSC_VER) && (defined(__cplusplus_winrt) || defined(__CRL_VER)) \
    && !defined(__clang__) || (!__clang__ && __GNUC__ && GCC_VERSION < 40600)
#define FOR_LOOP(type, var, list)   for each (type var in list)
#undef FOR_LOOP_AUTO_SUPPORT
#else
#define FOR_LOOP(type, var, list)   for (type var : list)
#define FOR_LOOP_AUTO_SUPPORT 1
#endif

#ifdef USE_STD_TR1_NAMESPACE
#error #include <tr1/memory>
namespace std { using namespace std::tr1; }
#endif

#ifdef NEED_STD_MOVE_FALLBACK
// Provide fallback variant of "std::move" for older compilers with
// incomplete/broken C++11 support.
namespace std {

template<typename _Tp>
inline typename std::remove_reference<_Tp>::type&& move(_Tp&& __t) {
  return static_cast<typename std::remove_reference<_Tp>::type&&>(__t);
}

}  // namespace std
#endif

#ifdef NEED_NULLPTR_FALLBACK
// Compilers with partial/incomplete support for C++11 don't know about
// "nullptr". A simple alias should be fine for our use case.
#define nullptr NULL
#endif

#ifdef _MSC_VER
  #ifdef _CPPRTTI
  #define RTTI_ENABLED
  #endif
#else
  #ifdef __GXX_RTTI
  #define RTTI_ENABLED
  #endif
#endif

//inline uint8_t Clip1_8bit(int16_t value) { if (value<=0) return 0; else if (value>=255) return 255; else return value; }
#define Clip1_8bit(value) ((value)<0 ? 0 : (value)>255 ? 255 : (value))
#define Clip_BitDepth(value, bit_depth) ((value)<0 ? 0 : (value)>((1<<bit_depth)-1) ? ((1<<bit_depth)-1) : (value))
#define Clip3(low,high,value) ((value)<(low) ? (low) : (value)>(high) ? (high) : (value))
#define Sign(value) (((value)<0) ? -1 : ((value)>0) ? 1 : 0)
#define abs_value(a) (((a)<0) ? -(a) : (a))
#define libde265_min(a,b) (((a)<(b)) ? (a) : (b))
#define libde265_max(a,b) (((a)>(b)) ? (a) : (b))

LIBDE265_INLINE static int ceil_div(int num,int denom)
{
  num += denom-1;
  return num/denom;
}

LIBDE265_INLINE static int ceil_log2(int val)
{
  int n=0;
  while (val > (1<<n)) {
    n++;
  }

  return n;
}

LIBDE265_INLINE static int Log2(int v)
{
  int n=0;
  while (v>1) {
    n++;
    v>>=1;
  }

  return n;
}

LIBDE265_INLINE static int Log2SizeToArea(int v)
{
  return (1<<(v<<1));
}

void copy_subimage(uint8_t* dst,int dststride,
                   const uint8_t* src,int srcstride,
                   int w, int h);


// === logging ===

enum LogModule {
  LogHighlevel,
  LogHeaders,
  LogSlice,
  LogDPB,
  LogMotion,
  LogTransform,
  LogDeblock,
  LogSAO,
  LogSEI,
  LogIntraPred,
  LogPixels,
  LogSymbols,
  LogCABAC,
  LogEncoder,
  LogEncoderMetadata,
  NUMBER_OF_LogModules
};


#if defined(DE265_LOG_ERROR) || defined(DE265_LOG_INFO) || defined(DE265_LOG_DEBUG) || defined(DE265_LOG_TRACE)
# define DE265_LOGGING 1
void enable_logging(enum LogModule);
void disable_logging(enum LogModule);
#else
#define enable_logging(x) { }
#define disable_logging(x) { }
#endif

#ifdef DE265_LOGGING
void log_set_current_POC(int poc);
#else
#define log_set_current_POC(poc) { }
#endif

#ifdef DE265_LOG_ERROR
void logerror(enum LogModule module, const char* string, ...);
#else
#define logerror(a,b, ...) { }
#endif

#ifdef DE265_LOG_INFO
void loginfo (enum LogModule module, const char* string, ...);
#else
#define loginfo(a,b, ...) { }
#endif

#ifdef DE265_LOG_DEBUG
void logdebug(enum LogModule module, const char* string, ...);
bool logdebug_enabled(enum LogModule module);
#else
#define logdebug(a,b, ...) { }
inline bool logdebug_enabled(enum LogModule module) { return false; }
#endif

#ifdef DE265_LOG_TRACE
void logtrace(enum LogModule module, const char* string, ...);
#else
#define logtrace(a,b, ...) { }
#endif

void log2fh(FILE* fh, const char* string, ...);


void printBlk(const char* title,const int32_t* data, int blksize, int stride, const std::string& prefix="  ");
void printBlk(const char* title,const int16_t* data, int blksize, int stride, const std::string& prefix="  ");
void printBlk(const char* title,const uint8_t* data, int blksize, int stride, const std::string& prefix="  ");

void debug_set_image_output(void (*)(const struct de265_image*, int slot));
void debug_show_image(const struct de265_image*, int slot);

#endif

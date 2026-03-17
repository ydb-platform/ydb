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

#include "util.h"
#include "de265.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>


void copy_subimage(uint8_t* dst,int dststride,
                   const uint8_t* src,int srcstride,
                   int w, int h)
{
  for (int y=0;y<h;y++) {
    memcpy(dst, src, w);
    dst += dststride;
    src += srcstride;
  }
}



#ifdef DE265_LOGGING
static int current_poc=0;
static int log_poc_start=-9999; // frame-numbers can be negative
static bool disable_log[NUMBER_OF_LogModules];
void log_set_current_POC(int poc) { current_poc=poc; }
#endif


static int disable_logging_OLD=0;
static int verbosity = 0;


LIBDE265_API void de265_disable_logging() // DEPRECATED
{
  disable_logging_OLD=1;
}


LIBDE265_API void de265_set_verbosity(int level)
{
  verbosity = level;
}

#if defined(DE265_LOG_ERROR) || defined(DE265_LOG_INFO) || defined(DE265_LOG_DEBUG) || defined(DE265_LOG_INFO)
void enable_logging(enum LogModule module)
{
  disable_log[module]=false;
}
void disable_logging(enum LogModule module)
{
  disable_log[module]=true;
}
#endif

static long logcnt[10];

#ifdef DE265_LOG_ERROR
void logerror(enum LogModule module, const char* string, ...)
{
  if (current_poc < log_poc_start) { return; }
  if (disable_log[module]) return;

  va_list va;

  int noPrefix = (string[0]=='*');
  if (!noPrefix) fprintf(stdout, "ERR: ");
  va_start(va, string);
  vfprintf(stdout, string + (noPrefix ? 1 : 0), va);
  va_end(va);
  fflush(stdout);
}
#endif

#ifdef DE265_LOG_INFO
void loginfo (enum LogModule module, const char* string, ...)
{
  if (verbosity<1) return;
  if (current_poc < log_poc_start) { return; }
  if (disable_log[module]) return;

  va_list va;

  int noPrefix = (string[0]=='*');
  if (!noPrefix) fprintf(stdout, "INFO: ");
  va_start(va, string);
  vfprintf(stdout, string + (noPrefix ? 1 : 0), va);
  va_end(va);
  fflush(stdout);
}
#endif

#ifdef DE265_LOG_DEBUG
void logdebug(enum LogModule module, const char* string, ...)
{
  if (verbosity<2) return;
  if (current_poc < log_poc_start) { return; }
  if (disable_log[module]) return;

  va_list va;

  int noPrefix = (string[0]=='*');
  if (!noPrefix) fprintf(stdout, "DEBUG: ");
  va_start(va, string);
  vfprintf(stdout, string + (noPrefix ? 1 : 0), va);
  va_end(va);
  fflush(stdout);
}

bool logdebug_enabled(enum LogModule module)
{
  return verbosity>=2;
}
#endif

#ifdef DE265_LOG_TRACE
void logtrace(enum LogModule module, const char* string, ...)
{
  if (verbosity<3) return;
  if (current_poc < log_poc_start) { return; }
  if (disable_log[module]) return;

  //if (module != LogSymbols /*&& module != LogCABAC*/) { return; }
  //if (logcnt<319500) return;

  //if (module != LogCABAC) return;

  va_list va;

  if (string[0]=='$') {
    int id = string[1]-'0';
    logcnt[id]++;
    fprintf(stdout, "[%ld] ",logcnt[id]);

    string += 3;
  }

  int noPrefix = (string[0]=='*');
  if (!noPrefix) { } // fprintf(stdout, "ERR: ");
  va_start(va, string);
  vfprintf(stdout, string + (noPrefix ? 1 : 0), va);
  va_end(va);
  fflush(stdout);
}
#endif

void log2fh(FILE* fh, const char* string, ...)
{
  va_list va;

  int noPrefix = (string[0]=='*');
  if (!noPrefix) fprintf(stdout, "INFO: ");
  va_start(va, string);
  vfprintf(fh, string + (noPrefix ? 1 : 0), va);
  va_end(va);
  fflush(stdout);
}



void printBlk(const char* title, const int16_t* data, int blksize, int stride,
              const std::string& prefix)
{
  if (title) printf("%s%s:\n",prefix.c_str(),title);

  for (int y=0;y<blksize;y++) {
    //logtrace(LogTransform,"  ");
    printf("%s",prefix.c_str());
    for (int x=0;x<blksize;x++) {
      //logtrace(LogTransform,"*%3d ", data[x+y*stride]);
      printf("%4d ", data[x+y*stride]);
    }
    //logtrace(LogTransform,"*\n");
    printf("\n");
  }
}


void printBlk(const char* title, const int32_t* data, int blksize, int stride,
              const std::string& prefix)
{
  if (title) printf("%s%s:\n",prefix.c_str(),title);

  for (int y=0;y<blksize;y++) {
    //logtrace(LogTransform,"  ");
    printf("%s",prefix.c_str());
    for (int x=0;x<blksize;x++) {
      //logtrace(LogTransform,"*%3d ", data[x+y*stride]);
      printf("%4d ", data[x+y*stride]);
    }
    //logtrace(LogTransform,"*\n");
    printf("\n");
  }
}


void printBlk(const char* title, const uint8_t* data, int blksize, int stride,
              const std::string& prefix)
{
  if (title) printf("%s%s:\n",prefix.c_str(),title);

  for (int y=0;y<blksize;y++) {
    //logtrace(LogTransform,"  ");
    printf("%s",prefix.c_str());
    for (int x=0;x<blksize;x++) {
      //logtrace(LogTransform,"*%3d ", data[x+y*stride]);
      printf("%02x ", data[x+y*stride]);
    }
    //logtrace(LogTransform,"*\n");
    printf("\n");
  }
}


static void (*debug_image_output_func)(const struct de265_image*, int slot) = NULL;

void debug_set_image_output(void (*func)(const struct de265_image*, int slot))
{
  debug_image_output_func = func;
}

void debug_show_image(const struct de265_image* img, int slot)
{
  if (debug_image_output_func) {
    debug_image_output_func(img,slot);
  }
}

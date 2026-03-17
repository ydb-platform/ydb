//   MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//   Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//   Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_UTILS_H
#define MECAB_UTILS_H

#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include "common.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_STDINT_H
#include <stdint.h>
#else  // HAVE_STDINT_H
#if defined(_WIN32) && !defined(__CYGWIN__)
#if defined(_MSC_VER) && (_MSC_VER <= 1500)
typedef unsigned char uint8_t;
typedef unsigned long uint32_t;
typedef unsigned long long uint64_t;
#else  // _MSC_VER
#include <stdint.h>
#endif  // _MSC_VER
#else   // _WIN32
typedef unsigned char uint8_t;
typedef unsigned long uint32_t;
typedef unsigned __int64 uint64_t;
#endif  // _WIN32
#endif  // HAVE_STDINT_H

namespace MeCab {

class Param;

enum { EUC_JP, CP932, UTF8, UTF16, UTF16LE, UTF16BE, ASCII };
int decode_charset(const char *charset);

void inline dtoa(double val, char *s) {
  std::sprintf(s, "%-16f", val);
  char *p = s;
  for (; *p != ' '; ++p) {}
  *p = '\0';
  return;
}

template <class T>
inline void itoa(T val, char *s) {
  char *t;
  T mod;

  if (val < 0) {
    *s++ = '-';
    val = -val;
  }
  t = s;

  while (val) {
    mod = val % 10;
    *t++ = static_cast<char>(mod) + '0';
    val /= 10;
  }

  if (s == t) *t++ = '0';
  *t = '\0';
  std::reverse(s, t);

  return;
}

template <class T>
inline void uitoa(T val, char *s) {
  char *t;
  T mod;
  t = s;
  while (val) {
    mod = val % 10;
    *t++ = static_cast<char>(mod) + '0';
    val /= 10;
  }

  if (s == t) *t++ = '0';
  *t = '\0';
  std::reverse(s, t);
  return;
}

inline const char *read_ptr(const char **ptr, size_t size) {
  const char *r = *ptr;
  *ptr += size;
  return r;
}

template <class T>
inline void read_static(const char **ptr, T& value) {
  const char *r = read_ptr(ptr, sizeof(T));
  memcpy(&value, r, sizeof(T));
}

bool file_exists(const char *filename);

int load_request_type(const Param &param);

bool load_dictionary_resource(Param *);

bool escape_csv_element(std::string *w);

void enum_csv_dictionaries(const char *path,
                           std::vector<std::string> *dics);

int  progress_bar(const char* message, size_t current, size_t total);

bool toLower(std::string *);

std::string create_filename(const std::string &path,
                            const std::string &file);
void remove_filename(std::string *s);
void remove_pathname(std::string *s);
void replace_string(std::string *s,
                    const std::string &src,
                    const std::string &dst);

template <class Iterator>
inline size_t tokenizeCSV(char *str,
                          Iterator out, size_t max) {
  char *eos = str + std::strlen(str);
  char *start = 0;
  char *end = 0;
  size_t n = 0;

  for (; str < eos; ++str) {
    // skip white spaces
     while (*str == ' ' || *str == '\t') ++str;
    if (*str == '"') {
      start = ++str;
      end = start;
      for (; str < eos; ++str) {
        if (*str == '"') {
          str++;
          if (*str != '"')
            break;
        }
        *end++ = *str;
      }
      str = std::find(str, eos, ',');
    } else {
      start = str;
      str = std::find(str, eos, ',');
      end = str;
    }
    if (max-- > 1) *end = '\0';
    *out++ = start;
    ++n;
    if (max == 0) break;
  }

  return n;
}

template <class Iterator>
inline size_t tokenize(char *str, const char *del,
                       Iterator out, size_t max) {
  char *stre = str + std::strlen(str);
  const char *dele = del + std::strlen(del);
  size_t size = 0;

  while (size < max) {
    char *n = std::find_first_of(str, stre, del, dele);
    *n = '\0';
    *out++ = str;
    ++size;
    if (n == stre) break;
    str = n + 1;
  }

  return size;
}

// continus run of space is regarded as one space
template <class Iterator>
inline size_t tokenize2(char *str, const char *del,
                        Iterator out, size_t max) {
  char *stre = str + std::strlen(str);
  const char *dele = del + std::strlen(del);
  size_t size = 0;

  while (size < max) {
    char *n = std::find_first_of(str, stre, del, dele);
    *n = '\0';
    if (*str != '\0') {
      *out++ = str;
      ++size;
    }
    if (n == stre) break;
    str = n + 1;
  }

  return size;
}

inline double logsumexp(double x, double y, bool flg) {
#define MINUS_LOG_EPSILON  50

  if (flg) return y;  // init mode
  double vmin = std::min<double>(x, y);
  double vmax = std::max<double>(x, y);
  if (vmax > vmin + MINUS_LOG_EPSILON) {
    return vmax;
  } else {
    return vmax + std::log(std::exp(vmin - vmax) + 1.0);
  }
}

inline short int tocost(double d, int n) {
  static const short max = +32767;
  static const short min = -32767;
  return static_cast<short>(std::max<double>(
                                std::min<double>(
                                    -n * d,
                                    static_cast<double>(max)),
                                static_cast<double>(min)) );
}

inline char getEscapedChar(const char p) {
  switch (p) {
    case '0':  return '\0';
    case 'a':  return '\a';
    case 'b':  return '\b';
    case 't':  return '\t';
    case 'n':  return '\n';
    case 'v':  return '\v';
    case 'f':  return '\f';
    case 'r':  return '\r';
    case 's':  return ' ';
    case '\\': return '\\';
    default: break;
  }

  return '\0';  // never be here
}

// return 64 bit hash
uint64_t fingerprint(const char *str, size_t size);
uint64_t fingerprint(const std::string &str);

#if defined(_WIN32) && !defined(__CYGWIN__)
std::wstring Utf8ToWide(const std::string &input);
std::string WideToUtf8(const std::wstring &input);
#endif
}
#endif

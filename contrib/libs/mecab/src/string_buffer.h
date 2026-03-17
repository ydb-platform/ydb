//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#ifndef MECAB_STRINGBUFFER_H
#define MECAB_STRINGBUFFER_H

#include <string>
#include "common.h"
#include "utils.h"

namespace MeCab {

#define _ITOA(n)  do { char fbuf[64]; itoa(n, fbuf); return this->write(fbuf); } while (0)
#define _UITOA(n) do { char fbuf[64]; uitoa(n, fbuf); return this->write(fbuf);} while (0)
#define _DTOA(n)  do { char fbuf[64]; dtoa(n, fbuf); return this->write(fbuf); } while (0)

class StringBuffer {
 private:
  size_t  size_;
  size_t  alloc_size_;
  char   *ptr_;
  bool    is_delete_;
  bool    error_;
  bool    reserve(size_t);

 public:
  explicit StringBuffer(): size_(0), alloc_size_(0),
                           ptr_(0), is_delete_(true), error_(false) {}
  explicit StringBuffer(char *_s, size_t _l):
      size_(0), alloc_size_(_l), ptr_(_s),
      is_delete_(false), error_(false) {}

  virtual ~StringBuffer();

  StringBuffer& write(char);
  StringBuffer& write(const char*, size_t);
  StringBuffer& write(const char*);
  StringBuffer& operator<<(double n)             { _DTOA(n); }
  StringBuffer& operator<<(short int n)          { _ITOA(n); }
  StringBuffer& operator<<(int n)                { _ITOA(n); }
  StringBuffer& operator<<(long int n)           { _ITOA(n); }
  StringBuffer& operator<<(unsigned short int n) { _UITOA(n); }
  StringBuffer& operator<<(unsigned int n)       { _UITOA(n); }
  StringBuffer& operator<<(unsigned long int n)  { _UITOA(n); }
#ifdef HAVE_UNSIGNED_LONG_LONG_INT
  StringBuffer& operator<<(unsigned long long int n) { _UITOA(n); }
#endif

  StringBuffer& operator<< (char n) {
    return this->write(n);
  }

  StringBuffer& operator<< (unsigned char n) {
    return this->write(n);
  }

  StringBuffer& operator<< (const char* n) {
    return this->write(n);
  }

  StringBuffer& operator<< (const std::string& n) {
    return this->write(n.c_str());
  }

  void clear() { size_ = 0; }
  const char *str() const {
    return error_ ?  0 : const_cast<const char*>(ptr_);
  }
};
}

#endif

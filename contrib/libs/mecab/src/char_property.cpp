// MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <fstream>
#include <map>
#include <vector>
#include <set>
#include <string>
#include <sstream>
#include "char_property.h"
#include "common.h"
#include "mmap.h"
#include "param.h"
#include "utils.h"

namespace MeCab {

namespace {
struct Range {
  int low;
  int high;
  std::vector<std::string> c;
};

int atohex(const char *s) {
  int n = 0;

  CHECK_DIE(std::strlen(s) >= 3
            && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'))
      << "no hex value: " << s;

  const char *p = s;
  s += 2;
  while (*s) {
    int r = 0;
    if (*s >= '0' && *s <= '9')
      r = *s - '0';
    else if (*s >= 'A' && *s <= 'F')
      r = *s - 'A' + 10;
    else if (*s >= 'a' && *s <= 'f')
      r = *s - 'a' + 10;
    else
      CHECK_DIE(false) << "no hex value: " << p;

    n = 16 * n + r;
    s++;
  }

  return n;
}

CharInfo encode(const std::vector<std::string> &c,
                std::map<std::string, CharInfo> *category) {
  CHECK_DIE(c.size()) << "category size is empty";

  std::map<std::string, CharInfo>::const_iterator it = category->find(c[0]);
  CHECK_DIE(it != category->end())
      << "category [" << c[0] << "] is undefined";

  CharInfo base = it->second;
  for (size_t i = 0; i < c.size(); ++i) {
    std::map<std::string, CharInfo>::const_iterator it =
        category->find(c[i]);
    CHECK_DIE(it != category->end())
        << "category [" << c[i] << "] is undefined";
    base.type += (1 << it->second.default_type);
  }

  return base;
}
}

bool CharProperty::open(const Param &param) {
  const std::string prefix   = param.get<std::string>("dicdir");
  const std::string filename = create_filename(prefix, CHAR_PROPERTY_FILE);
  return open(filename.c_str());
}

bool CharProperty::open(const char *filename) {
  std::ostringstream error;
  CHECK_FALSE(cmmap_->open(filename, "r"));

  const char *ptr = cmmap_->begin();
  unsigned int csize;
  read_static<unsigned int>(&ptr, csize);

  size_t fsize = sizeof(unsigned int) +
      (32 * csize) + sizeof(unsigned int) * 0xffff;

  CHECK_FALSE(fsize == cmmap_->size())
      << "invalid file size: " << filename;

  clist_.clear();
  for (unsigned int i = 0; i < csize; ++i) {
    const char *s = read_ptr(&ptr, 32);
    clist_.push_back(s);
  }

  map_ = reinterpret_cast<const CharInfo *>(ptr);

  return true;
}

void CharProperty::close() {
  cmmap_->close();
}

size_t CharProperty::size() const { return clist_.size(); }

const char *CharProperty::name(size_t i) const {
  return const_cast<const char*>(clist_[i]);
}

// this function must be rewritten.
void CharProperty::set_charset(const char *ct) {
  charset_ = decode_charset(ct);
}

int CharProperty::id(const char *key) const {
  for (int i = 0; i < static_cast<long>(clist_.size()); ++i) {
    if (std::strcmp(key, clist_[i]) == 0) {
      return i;
    }
  }
  return -1;
}

bool CharProperty::compile(const char *cfile,
                           const char *ufile,
                           const char *ofile) {
  scoped_fixed_array<char, BUF_SIZE> line;
  scoped_fixed_array<char *, 512> col;
  size_t id = 0;
  std::vector<Range> range;
  std::map<std::string, CharInfo> category;
  std::vector<std::string> category_ary;
  std::ifstream ifs(WPATH(cfile));
  std::istringstream iss(CHAR_PROPERTY_DEF_DEFAULT);
  std::istream *is = &ifs;

  if (!ifs) {
    std::cerr << cfile
              << " is not found. minimum setting is used" << std::endl;
    is = &iss;
  }

  while (is->getline(line.get(), line.size())) {
    if (std::strlen(line.get()) == 0 || line[0] == '#') {
      continue;
    }
    const size_t size = tokenize2(line.get(), "\t ", col.get(), col.size());
    CHECK_DIE(size >= 2) << "format error: " << line.get();

    // 0xFFFF..0xFFFF hoge hoge hgoe #
    if (std::strncmp(col[0], "0x", 2) == 0) {
      std::string low = col[0];
      std::string high;
      size_t pos = low.find("..");

      if (pos != std::string::npos) {
        high = low.substr(pos + 2, low.size() - pos - 2);
        low  = low.substr(0, pos);
      } else {
        high = low;
      }

      Range r;
      r.low = atohex(low.c_str());
      r.high = atohex(high.c_str());

      CHECK_DIE(r.low >= 0 && r.low < 0xffff &&
                r.high >= 0 && r.high < 0xffff &&
                r.low <= r.high)
          << "range error: low=" << r.low << " high=" << r.high;

      for (size_t i = 1; i < size; ++i) {
        if (col[i][0] == '#') {
          break;  // skip comments
        }
        CHECK_DIE(category.find(std::string(col[i])) != category.end())
            << "category [" << col[i] << "] is undefined";
        r.c.push_back(col[i]);
      }
      range.push_back(r);
    } else {
      CHECK_DIE(size >= 4) << "format error: " << line.get();

      std::string key = col[0];
      CHECK_DIE(category.find(key) == category.end())
          << "category " << key << " is already defined";

      CharInfo c;
      std::memset(&c, 0, sizeof(c));
      c.invoke  = std::atoi(col[1]);
      c.group   = std::atoi(col[2]);
      c.length  = std::atoi(col[3]);
      c.default_type = id++;

      category.insert(std::pair<std::string, CharInfo>(key, c));
      category_ary.push_back(key);
    }
  }

  CHECK_DIE(category.size() < 18) << "too many categories(>= 18)";

  CHECK_DIE(category.find("DEFAULT") != category.end())
      << "category [DEFAULT] is undefined";

  CHECK_DIE(category.find("SPACE") != category.end())
      << "category [SPACE] is undefined";

  std::istringstream iss2(UNK_DEF_DEFAULT);
  std::ifstream ifs2(WPATH(ufile));
  std::istream *is2 = &ifs2;

  if (!ifs2) {
    std::cerr << ufile
              << " is not found. minimum setting is used." << std::endl;
    is2 = &iss2;
  }

  std::set<std::string> unk;
  while (is2->getline(line.get(), line.size())) {
    const size_t n = tokenizeCSV(line.get(), col.get(), 2);
    CHECK_DIE(n >= 1) << "format error: " << line.get();
    const std::string key = col[0];
    CHECK_DIE(category.find(key) != category.end())
        << "category [" << key << "] is undefined in " << cfile;
    unk.insert(key);
  }

  for (std::map<std::string, CharInfo>::const_iterator it = category.begin();
       it != category.end();
       ++it) {
    CHECK_DIE(unk.find(it->first) != unk.end())
        << "category [" << it->first << "] is undefined in " << ufile;
  }

  std::vector<CharInfo> table(0xffff);
  {
    std::vector<std::string> tmp;
    tmp.push_back("DEFAULT");
    const CharInfo c = encode(tmp, &category);
    std::fill(table.begin(), table.end(), c);
  }

  for (std::vector<Range>::const_iterator it = range.begin();
       it != range.end();
       ++it) {
    const CharInfo c = encode(it->c, &category);
    for (int i = it->low; i <= it->high; ++i) {
      table[i] = c;
    }
  }

  // output binary table
  {
    std::ofstream ofs(WPATH(ofile), std::ios::binary|std::ios::out);
    CHECK_DIE(ofs) << "permission denied: " << ofile;

    unsigned int size = static_cast<unsigned int>(category.size());
    ofs.write(reinterpret_cast<const char*>(&size), sizeof(size));
    for (std::vector<std::string>::const_iterator it = category_ary.begin();
         it != category_ary.end();
         ++it) {
      char buf[32];
      std::fill(buf, buf + sizeof(buf), '\0');
      std::strncpy(buf, it->c_str(), sizeof(buf) - 1);
      ofs.write(reinterpret_cast<const char*>(buf), sizeof(buf));
    }
    ofs.write(reinterpret_cast<const char*>(&table[0]),
              sizeof(CharInfo) * table.size());
    ofs.close();
  }

  return true;
}
}

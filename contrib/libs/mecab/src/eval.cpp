//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <vector>
#include "common.h"
#include "mecab.h"
#include "param.h"
#include "stream_wrapper.h"
#include "scoped_ptr.h"
#include "utils.h"

namespace MeCab {

class Eval {
 private:
  static bool read(std::istream *is,
                   std::vector<std::vector<std::string> > *r,
                   const std::vector<int> &level) {
    if (!*is) {
      return false;
    }

    char *col[2];
    scoped_fixed_array<char, BUF_SIZE> buf;
    scoped_fixed_array<char *, BUF_SIZE> csv;
    r->clear();
    while (is->getline(buf.get(), buf.size())) {
      if (std::strcmp(buf.get(), "EOS") == 0) {
        break;
      }
      CHECK_DIE(tokenize(buf.get(), "\t", col,  2) == 2) << "format error";
      csv[0] = col[0];
      size_t n = tokenizeCSV(col[1], csv.get() + 1, csv.size() - 1);
      std::vector<std::string> tmp;
      for (size_t i = 0; i < level.size(); ++i) {
        size_t m = level[i] < 0 ? n : level[i];
        CHECK_DIE(m <= n) << " out of range " << level[i];
        std::string output;
        for (size_t j = 0; j <= m; ++j) {
          output += csv[j];
          if (j != 0) {
            output += "\t";
          }
        }
        tmp.push_back(output);
      }
      r->push_back(tmp);
    }

    return true;
  }

  static bool parseLevel(const char *level_str,
                         std::vector<int> *level) {
    scoped_fixed_array<char, BUF_SIZE> buf;
    scoped_fixed_array<char *, 512> col;
    std::strncpy(buf.get(), level_str, buf.size());
    level->clear();
    size_t n = tokenize2(buf.get(), "\t ", col.get(), col.size());
    for (size_t i = 0; i < n; ++i) {
      level->push_back(std::atoi(col[i]));
    }
    return true;
  }

  static void printeval(std::ostream *os, size_t c, size_t p, size_t r) {
    double pr = (p == 0) ? 0 : 100.0 * c/p;
    double re = (r == 0) ? 0 : 100.0 * c/r;
    double F = ((pr + re) == 0.0) ? 0 : 2 * pr * re /(pr + re);
    scoped_fixed_array<char, BUF_SIZE> buf;
    sprintf(buf.get(), "%4.4f(%d/%d) %4.4f(%d/%d) %4.4f\n",
            pr,
            static_cast<int>(c),
            static_cast<int>(p),
            re,
            static_cast<int>(c),
            static_cast<int>(r),
            F);
    *os << buf.get();
  }

 public:
  static bool eval(int argc, char **argv) {
    static const MeCab::Option long_options[] = {
      { "level",  'l',  "0 -1",    "STR",    "set level of evaluations" },
      { "output", 'o',  0,         "FILE",   "set the output file name" },
      { "version",  'v',  0,   0,    "show the version and exit"   },
      { "help",  'h',  0,   0,    "show this help and exit."   },
      { 0, 0, 0, 0 }
    };

    MeCab::Param param;
    param.open(argc, argv, long_options);

    if (!param.open(argc, argv, long_options)) {
      std::cout << param.what() << "\n\n" <<  COPYRIGHT
                << "\ntry '--help' for more information." << std::endl;
      return -1;
    }

    if (!param.help_version()) return 0;

    const std::vector<std::string> &files = param.rest_args();
    if (files.size() < 2) {
      std::cout << "Usage: " <<
          param.program_name() << " output answer" << std::endl;
      return -1;
    }

    std::string output = param.get<std::string>("output");
    if (output.empty()) output = "-";
    MeCab::ostream_wrapper ofs(output.c_str());
    CHECK_DIE(*ofs) << "no such file or directory: " << output;

    const std::string system = files[0];
    const std::string answer = files[1];

    const std::string level_str = param.get<std::string>("level");

    std::ifstream ifs1(WPATH(files[0].c_str()));
    std::ifstream ifs2(WPATH(files[1].c_str()));

    CHECK_DIE(ifs1) << "no such file or directory: " << files[0].c_str();
    CHECK_DIE(ifs2) << "no such file or directory: " << files[0].c_str();
    CHECK_DIE(!level_str.empty()) << "level_str is NULL";

    std::vector<int> level;
    parseLevel(level_str.c_str(), &level);
    CHECK_DIE(level.size()) << "level_str is empty: " << level_str;
    std::vector<size_t> result_tbl(level.size());
    std::fill(result_tbl.begin(), result_tbl.end(), 0);

    size_t prec = 0;
    size_t recall = 0;

    std::vector<std::vector<std::string> > r1;
    std::vector<std::vector<std::string> > r2;

    while (true) {
      if (!read(&ifs1, &r1, level) || !read(&ifs2, &r2, level))
        break;

      size_t i1 = 0;
      size_t i2 = 0;
      size_t p1 = 0;
      size_t p2 = 0;

      while (i1 < r1.size() && i2 < r2.size()) {
        if (p1 == p2) {
          for (size_t i = 0; i < result_tbl.size(); ++i) {
            if (r1[i1][i] == r2[i2][i]) {
              result_tbl[i]++;
            }
          }
          p1 += r1[i1][0].size();
          p2 += r2[i2][0].size();
          ++i1;
          ++i2;
          ++prec;
          ++recall;
        } else if (p1 < p2) {
          p1 += r1[i1][0].size();
          ++i1;
          ++prec;
        } else {
          p2 += r2[i2][0].size();
          ++i2;
          ++recall;
        }
      }

      while (i1 < r1.size()) {
        ++prec;
        ++i1;
      }

      while (i2 < r2.size()) {
        ++recall;
        ++i2;
      }
    }

    *ofs <<  "              precision          recall         F"
         << std::endl;
    for (size_t i = 0; i < result_tbl.size(); ++i) {
      if (level[i] == -1) {
        *ofs << "LEVEL ALL: ";
      } else {
        *ofs << "LEVEL " << level[i] << ":    ";
      }
      printeval(&*ofs, result_tbl[i], prec, recall);
    }

    return true;
  }
};

class TestSentenceGenerator {
 public:
  static int run(int argc, char **argv) {
    static const MeCab::Option long_options[] = {
      { "output",   'o',  0,   "FILE", "set the output filename" },
      { "version",  'v',  0,   0,    "show the version and exit"   },
      { "help",  'h',  0,   0,    "show this help and exit."   },
      { 0, 0, 0, 0 }
    };

    MeCab::Param param;
    param.open(argc, argv, long_options);

    if (!param.open(argc, argv, long_options)) {
      std::cout << param.what() << "\n\n" <<  COPYRIGHT
                << "\ntry '--help' for more information." << std::endl;
      return -1;
    }

    if (!param.help_version()) {
      return 0;
    }

    const std::vector<std::string> &tmp = param.rest_args();
    std::vector<std::string> files = tmp;
    if (files.empty()) {
      files.push_back("-");
    }

    std::string output = param.get<std::string>("output");
    if (output.empty()) output = "-";
    MeCab::ostream_wrapper ofs(output.c_str());
    CHECK_DIE(*ofs) << "permission denied: " << output;

    scoped_fixed_array<char, BUF_SIZE> buf;
    char *col[2];
    std::string str;
    for (size_t i = 0; i < files.size(); ++i) {
      MeCab::istream_wrapper ifs(files[i].c_str());
      CHECK_DIE(*ifs) << "no such file or directory: " << files[i];
      while (ifs->getline(buf.get(), buf.size())) {
        const size_t n = tokenize(buf.get(), "\t ", col, 2);
        CHECK_DIE(n <= 2) << "format error: " << buf.get();
        if (std::strcmp(col[0], "EOS") == 0 && !str.empty()) {
          *ofs << str << std::endl;
          str.clear();
        } else {
          str += col[0];
        }
      }
    }

    return 0;
  }
};
}

// exports
int mecab_system_eval(int argc, char **argv) {
  return MeCab::Eval::eval(argc, argv);
}

int mecab_test_gen(int argc, char **argv) {
  return MeCab::TestSentenceGenerator::run(argc, argv);
}

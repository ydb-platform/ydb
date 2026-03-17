//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <iostream>
#include <map>
#include <vector>
#include <string>
#include "char_property.h"
#include "connector.h"
#include "dictionary.h"
#include "dictionary_rewriter.h"
#include "feature_index.h"
#include "mecab.h"
#include "param.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

namespace MeCab {

class DictionaryComplier {
 public:
  static int run(int argc, char **argv) {
    static const MeCab::Option long_options[] = {
      { "dicdir",   'd',   ".",   "DIR", "set DIR as dic dir (default \".\")" },
      { "outdir",   'o',   ".",   "DIR",
        "set DIR as output dir (default \".\")"  },
      { "model",   'm',  0,     "FILE", "use FILE as model file" },
      { "userdic",  'u',   0,   "FILE",   "build user dictionary" },
      { "assign-user-dictionary-costs", 'a', 0, 0,
        "only assign costs/ids to user dictionary" },
      { "build-unknown",  'U',   0,   0,
        "build parameters for unknown words" },
      { "build-model", 'M', 0, 0,   "build model file" },
      { "build-charcategory", 'C', 0, 0,   "build character category maps" },
      { "build-sysdic",  's', 0, 0,   "build system dictionary" },
      { "build-matrix",    'm',  0,   0,   "build connection matrix" },
      { "charset",   'c',  MECAB_DEFAULT_CHARSET, "ENC",
        "make charset of binary dictionary ENC (default "
        MECAB_DEFAULT_CHARSET ")"  },
      { "charset",   't',  MECAB_DEFAULT_CHARSET, "ENC", "alias of -c"  },
      { "dictionary-charset",  'f',  MECAB_DEFAULT_CHARSET,
        "ENC", "assume charset of input CSVs as ENC (default "
        MECAB_DEFAULT_CHARSET ")"  },
      { "wakati",    'w',  0,   0,   "build wakati-gaki only dictionary", },
      { "posid",     'p',  0,   0,   "assign Part-of-speech id" },
      { "node-format", 'F', 0,  "STR",
        "use STR as the user defined node format" },
      { "version",   'v',  0,   0,   "show the version and exit."  },
      { "help",      'h',  0,   0,   "show this help and exit."  },
      { 0, 0, 0, 0 }
    };

    Param param;

    if (!param.open(argc, argv, long_options)) {
      std::cout << param.what() << "\n\n" <<  COPYRIGHT
                << "\ntry '--help' for more information." << std::endl;
      return -1;
    }

    if (!param.help_version()) {
      return 0;
    }

    const std::string dicdir = param.get<std::string>("dicdir");
    const std::string outdir = param.get<std::string>("outdir");
    bool opt_unknown = param.get<bool>("build-unknown");
    bool opt_matrix = param.get<bool>("build-matrix");
    bool opt_charcategory = param.get<bool>("build-charcategory");
    bool opt_sysdic = param.get<bool>("build-sysdic");
    bool opt_model = param.get<bool>("build-model");
    bool opt_assign_user_dictionary_costs = param.get<bool>
        ("assign-user-dictionary-costs");
    const std::string userdic = param.get<std::string>("userdic");

#define DCONF(file) create_filename(dicdir, std::string(file)).c_str()
#define OCONF(file) create_filename(outdir, std::string(file)).c_str()

    CHECK_DIE(param.load(DCONF(DICRC)))
        << "no such file or directory: " << DCONF(DICRC);

    std::vector<std::string> dic;
    if (userdic.empty()) {
      enum_csv_dictionaries(dicdir.c_str(), &dic);
    } else {
      dic = param.rest_args();
    }

    if (!userdic.empty()) {
      CHECK_DIE(dic.size()) << "no dictionaries are specified";
      param.set("type", static_cast<int>(MECAB_USR_DIC));
      if (opt_assign_user_dictionary_costs) {
        Dictionary::assignUserDictionaryCosts(param, dic,
                                              userdic.c_str());
      } else {
        Dictionary::compile(param, dic, userdic.c_str());
      }
    } else {
      if (!opt_unknown && !opt_matrix && !opt_charcategory &&
          !opt_sysdic && !opt_model) {
        opt_unknown = opt_matrix = opt_charcategory =
            opt_sysdic = opt_model = true;
      }

      if (opt_charcategory || opt_unknown) {
        CharProperty::compile(DCONF(CHAR_PROPERTY_DEF_FILE),
                              DCONF(UNK_DEF_FILE),
                              OCONF(CHAR_PROPERTY_FILE));
      }

      if (opt_unknown) {
        std::vector<std::string> tmp;
        tmp.push_back(DCONF(UNK_DEF_FILE));
        param.set("type", static_cast<int>(MECAB_UNK_DIC));
        Dictionary::compile(param, tmp, OCONF(UNK_DIC_FILE));
      }

      if (opt_model) {
        if (file_exists(DCONF(MODEL_DEF_FILE))) {
          FeatureIndex::compile(param,
                                DCONF(MODEL_DEF_FILE),
                                OCONF(MODEL_FILE));
        } else {
          std::cout << DCONF(MODEL_DEF_FILE)
                    << " is not found. skipped." << std::endl;
        }
      }

      if (opt_sysdic) {
        CHECK_DIE(dic.size()) << "no dictionaries are specified";
        param.set("type", static_cast<int>(MECAB_SYS_DIC));
        Dictionary::compile(param, dic, OCONF(SYS_DIC_FILE));
      }

      if (opt_matrix) {
        Connector::compile(DCONF(MATRIX_DEF_FILE),
                           OCONF(MATRIX_FILE));
      }
    }

    std::cout << "\ndone!\n";

    return 0;
  }
};

#undef DCONF
#undef OCONF
}

int mecab_dict_index(int argc, char **argv) {
  return MeCab::DictionaryComplier::run(argc, argv);
}

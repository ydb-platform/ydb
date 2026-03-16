//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <fstream>
#include <climits>
#include "connector.h"
#include "context_id.h"
#include "char_property.h"
#include "common.h"
#include "dictionary.h"
#include "dictionary_rewriter.h"
#include "feature_index.h"
#include "iconv_utils.h"
#include "mmap.h"
#include "param.h"
#include "scoped_ptr.h"
#include "utils.h"
#include "writer.h"

namespace MeCab {
namespace {

const unsigned int DictionaryMagicID = 0xef718f77u;

int toInt(const char *str) {
  if (!str || std::strlen(str) == 0) {
    return INT_MAX;
  }
  return std::atoi(str);
}

int calcCost(const std::string &w, const std::string &feature,
             int factor,
             DecoderFeatureIndex *fi, DictionaryRewriter *rewriter,
             CharProperty *property) {
  CHECK_DIE(fi);
  CHECK_DIE(rewriter);
  CHECK_DIE(property);

  LearnerPath path;
  LearnerNode rnode;
  LearnerNode lnode;
  rnode.stat  = lnode.stat = MECAB_NOR_NODE;
  rnode.rpath = &path;
  lnode.lpath = &path;
  path.lnode  = &lnode;
  path.rnode  = &rnode;

  size_t mblen = 0;
  const CharInfo cinfo = property->getCharInfo(w.c_str(),
                                               w.c_str() + w.size(),
                                               &mblen);
  path.rnode->char_type = cinfo.default_type;
  std::string ufeature, lfeature, rfeature;
  rewriter->rewrite2(feature, &ufeature, &lfeature, &rfeature);
  fi->buildUnigramFeature(&path, ufeature.c_str());
  fi->calcCost(&rnode);
  return tocost(rnode.wcost, factor);
}

int progress_bar_darts(size_t current, size_t total) {
  return progress_bar("emitting double-array", current, total);
}

template <typename T1, typename T2>
struct pair_1st_cmp: public std::binary_function<bool, T1, T2> {
  bool operator()(const std::pair<T1, T2> &x1,
                  const std::pair<T1, T2> &x2)  {
    return x1.first < x2.first;
  }
};
}  // namespace

bool Dictionary::open(const char *file, const char *mode) {
  close();
  filename_.assign(file);
  CHECK_FALSE(dmmap_->open(file, mode))
      << "no such file or directory: " << file;

  CHECK_FALSE(dmmap_->size() >= 100)
      << "dictionary file is broken: " << file;

  const char *ptr = dmmap_->begin();

  unsigned int dsize;
  unsigned int tsize;
  unsigned int fsize;
  unsigned int magic;
  unsigned int dummy;

  read_static<unsigned int>(&ptr, magic);
  CHECK_FALSE((magic ^ DictionaryMagicID) == dmmap_->size())
      << "dictionary file is broken: " << file;

  read_static<unsigned int>(&ptr, version_);
  CHECK_FALSE(version_ == DIC_VERSION)
      << "incompatible version: " << version_;

  read_static<unsigned int>(&ptr, type_);
  read_static<unsigned int>(&ptr, lexsize_);
  read_static<unsigned int>(&ptr, lsize_);
  read_static<unsigned int>(&ptr, rsize_);
  read_static<unsigned int>(&ptr, dsize);
  read_static<unsigned int>(&ptr, tsize);
  read_static<unsigned int>(&ptr, fsize);
  read_static<unsigned int>(&ptr, dummy);

  charset_ = ptr;
  ptr += 32;
  da_.set_array(reinterpret_cast<void *>(const_cast<char*>(ptr)));

  ptr += dsize;

  token_ = reinterpret_cast<const Token *>(ptr);
  ptr += tsize;

  feature_ = ptr;
  ptr += fsize;

  CHECK_FALSE(ptr == dmmap_->end())
      << "dictionary file is broken: " << file;

  return true;
}

void Dictionary::close() {
  dmmap_->close();
}

#define DCONF(file) create_filename(dicdir, std::string(file));

bool Dictionary::assignUserDictionaryCosts(
    const Param &param,
    const std::vector<std::string> &dics,
    const char *output) {
  Connector matrix;
  DictionaryRewriter rewriter;
  DecoderFeatureIndex fi;
  ContextID cid;
  CharProperty property;

  const std::string dicdir = param.get<std::string>("dicdir");

  const std::string matrix_file     = DCONF(MATRIX_DEF_FILE);
  const std::string matrix_bin_file = DCONF(MATRIX_FILE);
  const std::string left_id_file    = DCONF(LEFT_ID_FILE);
  const std::string right_id_file   = DCONF(RIGHT_ID_FILE);
  const std::string rewrite_file    = DCONF(REWRITE_FILE);

  const std::string from = param.get<std::string>("dictionary-charset");

  const int factor = param.get<int>("cost-factor");
  CHECK_DIE(factor > 0)   << "cost factor needs to be positive value";

  std::string config_charset = param.get<std::string>("config-charset");
  if (config_charset.empty()) {
    config_charset = from;
  }

  CHECK_DIE(!from.empty()) << "input dictionary charset is empty";

  Iconv config_iconv;
  CHECK_DIE(config_iconv.open(config_charset.c_str(), from.c_str()))
      << "iconv_open() failed with from=" << config_charset << " to=" << from;

  rewriter.open(rewrite_file.c_str(), &config_iconv);
  CHECK_DIE(fi.open(param)) << "cannot open feature index";

  CHECK_DIE(property.open(param));
  property.set_charset(from.c_str());

  if (!matrix.openText(matrix_file.c_str()) &&
      !matrix.open(matrix_bin_file.c_str())) {
    matrix.set_left_size(1);
    matrix.set_right_size(1);
  }

  cid.open(left_id_file.c_str(),
           right_id_file.c_str(), &config_iconv);
  CHECK_DIE(cid.left_size()  == matrix.left_size() &&
            cid.right_size() == matrix.right_size())
      << "Context ID files("
      << left_id_file
      << " or "
      << right_id_file << " may be broken: "
      << cid.left_size() << " " << matrix.left_size() << " "
      << cid.right_size() << " " << matrix.right_size();

  std::ofstream ofs(output);
  CHECK_DIE(ofs) << "permission denied: " << output;

  for (size_t i = 0; i < dics.size(); ++i) {
    std::ifstream ifs(WPATH(dics[i].c_str()));
    CHECK_DIE(ifs) << "no such file or directory: " << dics[i];
    std::cout << "reading " << dics[i] << " ... ";
    scoped_fixed_array<char, BUF_SIZE> line;
    while (ifs.getline(line.get(), line.size())) {
      char *col[8];
      const size_t n = tokenizeCSV(line.get(), col, 5);
      CHECK_DIE(n == 5) << "format error: " << line.get();
      std::string w = col[0];
      const std::string feature = col[4];
      const int cost = calcCost(w, feature, factor,
                                &fi, &rewriter, &property);
      std::string ufeature, lfeature, rfeature;
      CHECK_DIE(rewriter.rewrite(feature, &ufeature, &lfeature, &rfeature))
          << "rewrite failed: " << feature;
      const int lid = cid.lid(lfeature.c_str());
      const int rid = cid.rid(rfeature.c_str());
      CHECK_DIE(lid >= 0 && rid >= 0 && matrix.is_valid(lid, rid))
          << "invalid ids are found lid=" << lid << " rid=" << rid;
      escape_csv_element(&w);
      ofs << w << ',' << lid << ',' << rid << ','
          << cost << ',' << feature << '\n';
    }
  }

  return true;
}

bool Dictionary::compile(const Param &param,
                         const std::vector<std::string> &dics,
                         const char *output) {
  Connector matrix;
  scoped_ptr<DictionaryRewriter> rewrite;
  scoped_ptr<POSIDGenerator> posid;
  scoped_ptr<DecoderFeatureIndex> fi;
  scoped_ptr<ContextID> cid;
  scoped_ptr<Writer> writer;
  scoped_ptr<Lattice> lattice;
  scoped_ptr<StringBuffer> os;
  scoped_ptr<CharProperty> property;
  Node node;

  const std::string dicdir = param.get<std::string>("dicdir");

  const std::string matrix_file     = DCONF(MATRIX_DEF_FILE);
  const std::string matrix_bin_file = DCONF(MATRIX_FILE);
  const std::string left_id_file    = DCONF(LEFT_ID_FILE);
  const std::string right_id_file   = DCONF(RIGHT_ID_FILE);
  const std::string rewrite_file    = DCONF(REWRITE_FILE);
  const std::string pos_id_file     = DCONF(POS_ID_FILE);

  std::vector<std::pair<std::string, Token*> > dic;

  size_t offset  = 0;
  unsigned int lexsize = 0;
  std::string fbuf;

  const std::string from = param.get<std::string>("dictionary-charset");
  const std::string to = param.get<std::string>("charset");
  const bool wakati = param.get<bool>("wakati");
  const int type = param.get<int>("type");
  const std::string node_format = param.get<std::string>("node-format");
  const int factor = param.get<int>("cost-factor");
  CHECK_DIE(factor > 0)   << "cost factor needs to be positive value";

  // for backward compatibility
  std::string config_charset = param.get<std::string>("config-charset");
  if (config_charset.empty()) {
    config_charset = from;
  }

  CHECK_DIE(!from.empty()) << "input dictionary charset is empty";
  CHECK_DIE(!to.empty())   << "output dictionary charset is empty";

  Iconv iconv;
  CHECK_DIE(iconv.open(from.c_str(), to.c_str()))
      << "iconv_open() failed with from=" << from << " to=" << to;

  Iconv config_iconv;
  CHECK_DIE(config_iconv.open(config_charset.c_str(), from.c_str()))
      << "iconv_open() failed with from=" << config_charset << " to=" << from;

  if (!node_format.empty()) {
    writer.reset(new Writer);
    lattice.reset(createLattice());
    os.reset(new StringBuffer);
    memset(&node, 0, sizeof(node));
  }

  if (!matrix.openText(matrix_file.c_str()) &&
      !matrix.open(matrix_bin_file.c_str())) {
    matrix.set_left_size(1);
    matrix.set_right_size(1);
  }

  posid.reset(new POSIDGenerator);
  posid->open(pos_id_file.c_str(), &config_iconv);

  std::istringstream iss(UNK_DEF_DEFAULT);

  for (size_t i = 0; i < dics.size(); ++i) {
    std::ifstream ifs(WPATH(dics[i].c_str()));
    std::istream *is = &ifs;
    if (!ifs) {
      if (type == MECAB_UNK_DIC) {
        std::cerr << dics[i]
                  << " is not found. minimum setting is used." << std::endl;
        is = &iss;
      } else {
        CHECK_DIE(ifs) << "no such file or directory: " << dics[i];
      }
    }

    std::cout << "reading " << dics[i] << " ... ";

    scoped_fixed_array<char, BUF_SIZE> line;
    size_t num = 0;

    while (is->getline(line.get(), line.size())) {
      char *col[8];
      const size_t n = tokenizeCSV(line.get(), col, 5);
      CHECK_DIE(n == 5) << "format error: " << line.get();

      std::string w = col[0];
      int lid = toInt(col[1]);
      int rid = toInt(col[2]);
      int cost = toInt(col[3]);
      std::string feature = col[4];
      const int pid = posid->id(feature.c_str());

      if (cost == INT_MAX) {
        CHECK_DIE(type == MECAB_USR_DIC)
            << "cost field should not be empty in sys/unk dic.";
        if (!rewrite.get()) {
          rewrite.reset(new DictionaryRewriter);
          rewrite->open(rewrite_file.c_str(), &config_iconv);
          fi.reset(new DecoderFeatureIndex);
          CHECK_DIE(fi->open(param)) << "cannot open feature index";
          property.reset(new CharProperty);
          CHECK_DIE(property->open(param));
          property->set_charset(from.c_str());
        }
        cost = calcCost(w, feature, factor,
                        fi.get(), rewrite.get(), property.get());
      }

      if (lid < 0  || rid < 0 || lid == INT_MAX || rid == INT_MAX) {
        if (!rewrite.get()) {
          rewrite.reset(new DictionaryRewriter);
          rewrite->open(rewrite_file.c_str(), &config_iconv);
        }

        std::string ufeature, lfeature, rfeature;
        CHECK_DIE(rewrite->rewrite(feature, &ufeature, &lfeature, &rfeature))
            << "rewrite failed: " << feature;

        if (!cid.get()) {
          cid.reset(new ContextID);
          cid->open(left_id_file.c_str(),
                    right_id_file.c_str(), &config_iconv);
          CHECK_DIE(cid->left_size()  == matrix.left_size() &&
                    cid->right_size() == matrix.right_size())
              << "Context ID files("
              << left_id_file
              << " or "
              << right_id_file << " may be broken";
        }

        lid = cid->lid(lfeature.c_str());
        rid = cid->rid(rfeature.c_str());
      }

      CHECK_DIE(lid >= 0 && rid >= 0 && matrix.is_valid(lid, rid))
          << "invalid ids are found lid=" << lid << " rid=" << rid;

      if (w.empty()) {
        std::cerr << "empty word is found, discard this line" << std::endl;
        continue;
      }

      if (!iconv.convert(&feature)) {
        std::cerr << "iconv conversion failed. skip this entry"
                  << std::endl;
        continue;
      }

      if (type != MECAB_UNK_DIC && !iconv.convert(&w)) {
        std::cerr << "iconv conversion failed. skip this entry"
                  << std::endl;
        continue;
      }

      if (!node_format.empty()) {
        node.surface = w.c_str();
        node.feature = feature.c_str();
        node.length  = w.size();
        node.rlength = w.size();
        node.posid   = pid;
        node.stat    = MECAB_NOR_NODE;
        lattice->set_sentence(w.c_str());
        CHECK_DIE(os.get());
        CHECK_DIE(writer.get());
        os->clear();
        CHECK_DIE(writer->writeNode(lattice.get(),
                                    node_format.c_str(),
                                    &node, &*os)) <<
            "conversion error: " << feature << " with " << node_format;
        *os << '\0';
        feature = os->str();
      }

      std::string key;
      if (!wakati) {
        key = feature + '\0';
      }

      Token* token  = new Token;
      token->lcAttr = lid;
      token->rcAttr = rid;
      token->posid  = pid;
      token->wcost = cost;
      token->feature = offset;
      token->compound = 0;
      dic.push_back(std::pair<std::string, Token*>(w, token));

      // append to output buffer
      if (!wakati) {
        fbuf.append(key.data(), key.size());
      }
      offset += key.size();

      ++num;
      ++lexsize;
    }

    std::cout << num << std::endl;
  }

  if (wakati) {
    fbuf.append("\0", 1);
  }

  std::stable_sort(dic.begin(), dic.end(),
                   pair_1st_cmp<std::string, Token *>());

  size_t bsize = 0;
  size_t idx = 0;
  std::string prev;
  std::vector<const char *> str;
  std::vector<size_t> len;
  std::vector<Darts::DoubleArray::result_type> val;

  for (size_t i = 0; i < dic.size(); ++i) {
    if (i != 0 && prev != dic[i].first) {
      str.push_back(dic[idx].first.c_str());
      len.push_back(dic[idx].first.size());
      val.push_back(bsize +(idx << 8));
      bsize = 1;
      idx = i;
    } else {
      ++bsize;
    }
    prev = dic[i].first;
  }
  str.push_back(dic[idx].first.c_str());
  len.push_back(dic[idx].first.size());
  val.push_back(bsize +(idx << 8));

  CHECK_DIE(str.size() == len.size());
  CHECK_DIE(str.size() == val.size());

  Darts::DoubleArray da;
  CHECK_DIE(da.build(str.size(), const_cast<char **>(&str[0]),
                     &len[0], &val[0], &progress_bar_darts) == 0)
      << "unkown error in building double-array";

  std::string tbuf;
  for (size_t i = 0; i < dic.size(); ++i) {
    tbuf.append(reinterpret_cast<const char*>(dic[i].second),
                sizeof(Token));
    delete dic[i].second;
  }
  dic.clear();

  // needs to be 8byte(64bit) aligned
  while (tbuf.size() % 8 != 0) {
    Token dummy;
    memset(&dummy, 0, sizeof(Token));
    tbuf.append(reinterpret_cast<const char*>(&dummy), sizeof(Token));
  }

  unsigned int dummy = 0;
  unsigned int lsize = matrix.left_size();
  unsigned int rsize = matrix.right_size();
  unsigned int dsize = da.unit_size() * da.size();
  unsigned int tsize = tbuf.size();
  unsigned int fsize = fbuf.size();

  unsigned int version = DIC_VERSION;
  char charset[32];
  std::fill(charset, charset + sizeof(charset), '\0');
  std::strncpy(charset, to.c_str(), 31);

  std::ofstream bofs(WPATH(output), std::ios::binary|std::ios::out);
  CHECK_DIE(bofs) << "permission denied: " << output;

  unsigned int magic = 0;

  // needs to be 64bit aligned
  // 10*32 = 64*5
  bofs.write(reinterpret_cast<const char *>(&magic),   sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&version), sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&type),    sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&lexsize), sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&lsize),   sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&rsize),   sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&dsize),   sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&tsize),   sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&fsize),   sizeof(unsigned int));
  bofs.write(reinterpret_cast<const char *>(&dummy),   sizeof(unsigned int));

  // 32 * 8 = 64 * 4
  bofs.write(reinterpret_cast<const char *>(charset),  sizeof(charset));

  bofs.write(reinterpret_cast<const char*>(da.array()),
             da.unit_size() * da.size());
  bofs.write(const_cast<const char *>(tbuf.data()), tbuf.size());
  bofs.write(const_cast<const char *>(fbuf.data()), fbuf.size());

  // save magic id
  magic = static_cast<unsigned int>(bofs.tellp());
  magic ^= DictionaryMagicID;
  bofs.seekp(0);
  bofs.write(reinterpret_cast<const char *>(&magic), sizeof(unsigned int));

  bofs.close();

  return true;
}
}

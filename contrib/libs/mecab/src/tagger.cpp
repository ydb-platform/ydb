// MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2006 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <cstring>
#include <iostream>
#include <iterator>
#include "common.h"
#include "connector.h"
#include "mecab.h"
#include "nbest_generator.h"
#include "param.h"
#include "scoped_ptr.h"
#include "stream_wrapper.h"
#include "string_buffer.h"
#include "thread.h"
#include "tokenizer.h"
#include "viterbi.h"
#include "writer.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

const char *getGlobalError();
void setGlobalError(const char *str);

namespace MeCab {
namespace {

const float kDefaultTheta = 0.75;

const MeCab::Option long_options[] = {
  { "rcfile",        'r',  0, "FILE",    "use FILE as resource file" },
  { "dicdir",        'd',  0, "DIR",    "set DIR  as a system dicdir" },
  { "userdic",        'u',  0, "FILE",    "use FILE as a user dictionary" },
  { "lattice-level",      'l', "0", "INT",
    "lattice information level (DEPRECATED)" },
  { "dictionary-info",  'D', 0, 0, "show dictionary information and exit" },
  { "output-format-type", 'O',  0, "TYPE",
    "set output format type (wakati,none,...)" },
  { "all-morphs",      'a', 0, 0,    "output all morphs(default false)" },
  { "nbest",              'N', "1",
    "INT", "output N best results (default 1)" },
  { "partial",            'p',  0, 0,
    "partial parsing mode (default false)" },
  { "marginal",           'm',  0, 0,
    "output marginal probability (default false)" },
  { "max-grouping-size",  'M',  "24",
    "INT",  "maximum grouping size for unknown words (default 24)" },
  { "node-format",        'F',  "%m\\t%H\\n", "STR",
    "use STR as the user-defined node format" },
  { "unk-format",        'U',  "%m\\t%H\\n", "STR",
    "use STR as the user-defined unknown node format"   },
  { "bos-format",        'B',  "", "STR",
    "use STR as the user-defined beginning-of-sentence format"   },
  { "eos-format",        'E',  "EOS\\n", "STR",
    "use STR as the user-defined end-of-sentence format"   },
  { "eon-format",        'S',  "", "STR",
    "use STR as the user-defined end-of-NBest format"   },
  { "unk-feature",       'x',  0, "STR",
    "use STR as the feature for unknown word" },
  { "input-buffer-size",  'b',  0, "INT",
    "set input buffer size (default 8192)" },
  { "dump-config", 'P', 0, 0, "dump MeCab parameters" },
  { "allocate-sentence",  'C', 0, 0,
    "allocate new memory for input sentence" },
  { "theta",        't',  "0.75",  "FLOAT",
    "set temparature parameter theta (default 0.75)"  },
  { "cost-factor",        'c',  "700",  "INT",
    "set cost factor (default 700)"  },
  { "output",        'o',  0,    "FILE",  "set the output file name" },
  { "version",        'v',  0, 0,     "show the version and exit." },
  { "help",          'h',  0, 0,     "show this help and exit." },
  { 0, 0, 0, 0 }
};

class ModelImpl: public Model {
 public:
  ModelImpl();
  virtual ~ModelImpl();

  bool open(int argc, char **argv);
  bool open(const char *arg);
  bool open(const Param &param);

  bool swap(Model *model);

  bool is_available() const {
    return (viterbi_ && writer_.get());
  }

  int request_type() const {
    return request_type_;
  }

  double theta() const {
    return theta_;
  }

  const DictionaryInfo *dictionary_info() const {
    return viterbi_->tokenizer() ?
        viterbi_->tokenizer()->dictionary_info() : 0;
  }

  int transition_cost(unsigned short rcAttr,
                      unsigned short lcAttr) const {
    return viterbi_->connector()->transition_cost(rcAttr, lcAttr);
  }

  Node *lookup(const char *begin, const char *end,
               Lattice *lattice) const {
    return viterbi_->tokenizer()->lookup<false>(
        begin, end,
        lattice->allocator(), lattice);
  }

  Tagger *createTagger() const;

  Lattice *createLattice() const;

  const Viterbi *viterbi() const {
    return viterbi_;
  }

  // moves the owership.
  Viterbi *take_viterbi() {
    Viterbi *result = viterbi_;
    viterbi_ = 0;
    return result;
  }

  const Writer *writer() const {
    return writer_.get();
  }

#ifdef HAVE_ATOMIC_OPS
  read_write_mutex *mutex() const {
    return &mutex_;
  }
#endif

 private:
  Viterbi            *viterbi_;
  scoped_ptr<Writer>  writer_;
  int                 request_type_;
  double              theta_;

#ifdef HAVE_ATOMIC_OPS
  mutable read_write_mutex      mutex_;
#endif
};

class TaggerImpl: public Tagger {
 public:
  bool                  open(int argc, char **argv);
  bool                  open(const char *arg);
  bool                  open(const ModelImpl &model);

  bool                  parse(Lattice *lattice) const;

  void                  set_request_type(int request_type);
  int                   request_type() const;

  const char*           parse(const char*);
  const char*           parse(const char*, size_t);
  const char*           parse(const char*, size_t, char*, size_t);
  const Node*           parseToNode(const char*);
  const Node*           parseToNode(const char*, size_t = 0);
  const char*           parseNBest(size_t, const char*);
  const char*           parseNBest(size_t, const char*, size_t);
  const char*           parseNBest(size_t, const char*,
                                   size_t, char *, size_t);
  bool                  parseNBestInit(const char*);
  bool                  parseNBestInit(const char*, size_t);
  const Node*           nextNode();
  const char*           next();
  const char*           next(char*, size_t);

  const char           *formatNode(const Node *);
  const char           *formatNode(const Node *, char *, size_t);

  const DictionaryInfo *dictionary_info() const;

  void                  set_partial(bool partial);
  bool                  partial() const;
  void                  set_theta(float theta);
  float                 theta() const;
  void                  set_lattice_level(int level);
  int                   lattice_level() const;
  void                  set_all_morphs(bool all_morphs);
  bool                  all_morphs() const;

  const char*           what() const;

  TaggerImpl();
  virtual ~TaggerImpl();

 private:
  const ModelImpl *model() const { return current_model_; }

   void set_what(const char *str) {
     what_.assign(str);
   }

  void initRequestType() {
    mutable_lattice()->set_request_type(request_type_);
    mutable_lattice()->set_theta(theta_);
  }

  Lattice *mutable_lattice() {
    if (!lattice_.get()) {
      lattice_.reset(model()->createLattice());
    }
    return lattice_.get();
  }

  const ModelImpl          *current_model_;
  scoped_ptr<ModelImpl>     model_;
  scoped_ptr<Lattice>       lattice_;
  int                       request_type_;
  double                    theta_;
  std::string               what_;
};

class LatticeImpl : public Lattice {
 public:
  explicit LatticeImpl(const Writer *writer = 0);
  ~LatticeImpl();

  // clear internal lattice
  void clear();

  bool is_available() const {
    return (sentence_ &&
            !begin_nodes_.empty() &&
            !end_nodes_.empty());
  }

  // nbest;
  bool next();

  // return bos/eos node
  Node *bos_node() const { return end_nodes_[0]; }
  Node *eos_node() const { return begin_nodes_[size()]; }
  Node **begin_nodes() const { return const_cast<Node **>(&begin_nodes_[0]); }
  Node **end_nodes() const   { return const_cast<Node **>(&end_nodes_[0]); }
  Node *begin_nodes(size_t pos) const { return begin_nodes_[pos]; }
  Node *end_nodes(size_t pos) const { return end_nodes_[pos]; }

  const char *sentence() const { return sentence_; }
  void set_sentence(const char *sentence);
  void set_sentence(const char *sentence, size_t len);
  size_t size() const { return size_; }

  void set_Z(double Z) { Z_ = Z; }
  double Z() const { return Z_; }

  float theta() const { return theta_; }
  void  set_theta(float theta) { theta_ = theta; }

  int request_type() const { return request_type_; }

  void set_request_type(int request_type) {
    request_type_ = request_type;
  }
  bool has_request_type(int request_type) const {
    return request_type & request_type_;
  }
  void add_request_type(int request_type) {
    request_type_ |= request_type;
  }
  void remove_request_type(int request_type) {
    request_type_ &= ~request_type;
  }

  Allocator<Node, Path> *allocator() const {
    return allocator_.get();
  }

  Node *newNode() {
    return allocator_->newNode();
  }

  bool has_constraint() const;
  int boundary_constraint(size_t pos) const;
  const char *feature_constraint(size_t begin_pos) const;

  void set_boundary_constraint(size_t pos,
                               int boundary_constraint_type);

  void set_feature_constraint(size_t begin_pos, size_t end_pos,
                              const char *feature);

  void set_result(const char *result);

  const char *what() const { return what_.c_str(); }

  void set_what(const char *str) {
    what_.assign(str);
  }

  const char *toString();
  const char *toString(char *buf, size_t size);
  const char *toString(const Node *node);
  const char *toString(const Node *node,
                       char *buf, size_t size);
  const char *enumNBestAsString(size_t N);
  const char *enumNBestAsString(size_t N, char *buf, size_t size);

 private:
  const char                 *sentence_;
  size_t                      size_;
  double                      theta_;
  double                      Z_;
  int                         request_type_;
  std::string                 what_;
  std::vector<Node *>         end_nodes_;
  std::vector<Node *>         begin_nodes_;
  std::vector<const char *>   feature_constraint_;
  std::vector<unsigned char>  boundary_constraint_;
  const Writer               *writer_;
  scoped_ptr<StringBuffer>    ostrs_;
  scoped_ptr<Allocator<Node, Path> > allocator_;

  StringBuffer *stream() {
    if (!ostrs_.get()) {
      ostrs_.reset(new StringBuffer);
    }
    return ostrs_.get();
  }

  const char *toStringInternal(StringBuffer *os);
  const char *toStringInternal(const Node *node, StringBuffer *os);
  const char *enumNBestAsStringInternal(size_t N, StringBuffer *os);
};

ModelImpl::ModelImpl()
    : viterbi_(new Viterbi), writer_(new Writer),
      request_type_(MECAB_ONE_BEST), theta_(0.0) {}

ModelImpl::~ModelImpl() {
  delete viterbi_;
  viterbi_ = 0;
}

bool ModelImpl::open(int argc, char **argv) {
  Param param;
  if (!param.open(argc, argv, long_options) ||
      !load_dictionary_resource(&param)) {
    setGlobalError(param.what());
    return false;
  }
  return open(param);
}

bool ModelImpl::open(const char *arg) {
  Param param;
  if (!param.open(arg, long_options) ||
      !load_dictionary_resource(&param)) {
    setGlobalError(param.what());
    return false;
  }
  return open(param);
}

bool ModelImpl::open(const Param &param) {
  if (!writer_->open(param) || !viterbi_->open(param)) {
    std::string error = viterbi_->what();
    if (!error.empty()) {
      error.append(" ");
    }
    error.append(writer_->what());
    setGlobalError(error.c_str());
    return false;
  }

  request_type_ = load_request_type(param);
  theta_ = param.get<double>("theta");

  return is_available();
}

bool ModelImpl::swap(Model *model) {
  scoped_ptr<Model> model_data(model);

  if (!is_available()) {
    setGlobalError("current model is not available");
    return false;
  }
#ifndef HAVE_ATOMIC_OPS
  setGlobalError("atomic model replacement is not supported");
  return false;
#else
  ModelImpl *m = static_cast<ModelImpl *>(model_data.get());
  if (!m) {
    setGlobalError("Invalid model is passed");
    return false;
  }

  if (!m->is_available()) {
    setGlobalError("Passed model is not available");
    return false;
  }

  Viterbi *current_viterbi = viterbi_;
  {
    scoped_writer_lock l(mutex());
    viterbi_      = m->take_viterbi();
    request_type_ = m->request_type();
    theta_        = m->theta();
  }

  delete current_viterbi;

  return true;
#endif
}

Tagger *ModelImpl::createTagger() const {
  if (!is_available()) {
    setGlobalError("Model is not available");
    return 0;
  }
  TaggerImpl *tagger = new TaggerImpl;
  if (!tagger->open(*this)) {
    setGlobalError(tagger->what());
    delete tagger;
    return 0;
  }
  tagger->set_theta(theta_);
  tagger->set_request_type(request_type_);
  return tagger;
}

Lattice *ModelImpl::createLattice() const {
  if (!is_available()) {
    setGlobalError("Model is not available");
    return 0;
  }
  return new LatticeImpl(writer_.get());
}

TaggerImpl::TaggerImpl()
    : current_model_(0),
      request_type_(MECAB_ONE_BEST), theta_(kDefaultTheta) {}

TaggerImpl::~TaggerImpl() {}

const char *TaggerImpl::what() const {
  return what_.c_str();
}

bool TaggerImpl::open(int argc, char **argv) {
  model_.reset(new ModelImpl);
  if (!model_->open(argc, argv)) {
    model_.reset(0);
    return false;
  }
  current_model_ = model_.get();
  request_type_ = model()->request_type();
  theta_        = model()->theta();
  return true;
}

bool TaggerImpl::open(const char *arg) {
  model_.reset(new ModelImpl);
  if (!model_->open(arg)) {
    model_.reset(0);
    return false;
  }
  current_model_ = model_.get();
  request_type_ = model()->request_type();
  theta_        = model()->theta();
  return true;
}

bool TaggerImpl::open(const ModelImpl &model) {
  if (!model.is_available()) {
    return false;
  }
  model_.reset(0);
  current_model_ = &model;
  request_type_ = current_model_->request_type();
  theta_        = current_model_->theta();
  return true;
}

void TaggerImpl::set_request_type(int request_type) {
  request_type_ = request_type;
}

int TaggerImpl::request_type() const {
  return request_type_;
}

void TaggerImpl::set_partial(bool partial) {
  if (partial) {
    request_type_ |= MECAB_PARTIAL;
  } else {
    request_type_ &= ~MECAB_PARTIAL;
  }
}

bool TaggerImpl::partial() const {
  return request_type_ & MECAB_PARTIAL;
}

void TaggerImpl::set_theta(float theta) {
  theta_ = theta;
}

float TaggerImpl::theta() const {
  return theta_;
}

void TaggerImpl::set_lattice_level(int level) {
  switch (level) {
    case 0: request_type_ |= MECAB_ONE_BEST;
      break;
    case 1: request_type_ |= MECAB_NBEST;
      break;
    case 2: request_type_ |= MECAB_MARGINAL_PROB;
      break;
    default:
      break;
  }
}

int TaggerImpl::lattice_level() const {
  if (request_type_ & MECAB_MARGINAL_PROB) {
    return 2;
  } else if (request_type_ & MECAB_NBEST) {
    return 1;
  } else {
    return 0;
  }
}

void TaggerImpl::set_all_morphs(bool all_morphs) {
  if (all_morphs) {
    request_type_ |= MECAB_ALL_MORPHS;
  } else {
    request_type_ &= ~MECAB_ALL_MORPHS;
  }
}

bool TaggerImpl::all_morphs() const {
  return request_type_ & MECAB_ALL_MORPHS;
}

bool TaggerImpl::parse(Lattice *lattice) const {
#ifdef HAVE_ATOMIC_OPS
  scoped_reader_lock l(model()->mutex());
#endif

  return model()->viterbi()->analyze(lattice);
}

const char *TaggerImpl::parse(const char *str) {
  return parse(str, std::strlen(str));
}

const char *TaggerImpl::parse(const char *str, size_t len) {
  Lattice *lattice = mutable_lattice();
  lattice->set_sentence(str, len);
  initRequestType();
  if (!parse(lattice)) {
    set_what(lattice->what());
    return 0;
  }
  const char *result = lattice->toString();
  if (!result) {
    set_what(lattice->what());
    return 0;
  }
  return result;
}

const char *TaggerImpl::parse(const char *str, size_t len,
                              char *out, size_t len2) {
  Lattice *lattice = mutable_lattice();
  lattice->set_sentence(str, len);
  initRequestType();
  if (!parse(lattice)) {
    set_what(lattice->what());
    return 0;
  }
  const char *result = lattice->toString(out, len2);
  if (!result) {
    set_what(lattice->what());
    return 0;
  }
  return result;
}

const Node *TaggerImpl::parseToNode(const char *str) {
  return parseToNode(str, std::strlen(str));
}

const Node *TaggerImpl::parseToNode(const char *str, size_t len) {
  Lattice *lattice = mutable_lattice();
  lattice->set_sentence(str, len);
  initRequestType();
  if (!parse(lattice)) {
    set_what(lattice->what());
    return 0;
  }
  return lattice->bos_node();
}

bool TaggerImpl::parseNBestInit(const char *str) {
  return parseNBestInit(str, std::strlen(str));
}

bool TaggerImpl::parseNBestInit(const char *str, size_t len) {
  Lattice *lattice = mutable_lattice();
  lattice->set_sentence(str, len);
  initRequestType();
  lattice->add_request_type(MECAB_NBEST);
  if (!parse(lattice)) {
    set_what(lattice->what());
    return false;
  }
  return true;
}

const Node* TaggerImpl::nextNode() {
  Lattice *lattice = mutable_lattice();
  if (!lattice->next()) {
    lattice->set_what("no more results");
    return 0;
  }
  return lattice->bos_node();
}

const char* TaggerImpl::next() {
  Lattice *lattice = mutable_lattice();
  if (!lattice->next()) {
    lattice->set_what("no more results");
    return 0;
  }
  const char *result = lattice->toString();
  if (!result) {
    set_what(lattice->what());
    return 0;
  }
  return result;
}

const char* TaggerImpl::next(char *out, size_t len2) {
  Lattice *lattice = mutable_lattice();
  if (!lattice->next()) {
    lattice->set_what("no more results");
    return 0;
  }
  const char *result = lattice->toString(out, len2);
  if (!result) {
    set_what(lattice->what());
    return 0;
  }
  return result;
}

const char* TaggerImpl::parseNBest(size_t N, const char* str) {
  return parseNBest(N, str, std::strlen(str));
}

const char* TaggerImpl::parseNBest(size_t N,
                                   const char* str, size_t len) {
  Lattice *lattice = mutable_lattice();
  lattice->set_sentence(str, len);
  initRequestType();
  lattice->add_request_type(MECAB_NBEST);

  if (!parse(lattice)) {
    set_what(lattice->what());
    return 0;
  }

  const char *result = lattice->enumNBestAsString(N);
  if (!result) {
    set_what(lattice->what());
    return 0;
  }
  return result;
}

const char* TaggerImpl::parseNBest(size_t N, const char* str, size_t len,
                                   char *out, size_t len2) {
  Lattice *lattice = mutable_lattice();
  lattice->set_sentence(str, len);
  initRequestType();
  lattice->add_request_type(MECAB_NBEST);

  if (!parse(lattice)) {
    set_what(lattice->what());
    return 0;
  }

  const char *result = lattice->enumNBestAsString(N, out, len2);
  if (!result) {
    set_what(lattice->what());
    return 0;
  }
  return result;
}

const char* TaggerImpl::formatNode(const Node* node) {
  const char *result = mutable_lattice()->toString(node);
  if (!result) {
    set_what(mutable_lattice()->what());
    return 0;
  }
  return result;
}

const char* TaggerImpl::formatNode(const Node* node,
                                   char *out, size_t len) {
  const char *result = mutable_lattice()->toString(node, out, len);
  if (!result) {
    set_what(mutable_lattice()->what());
    return 0;
  }
  return result;
}

const DictionaryInfo *TaggerImpl::dictionary_info() const {
  return model()->dictionary_info();
}

LatticeImpl::LatticeImpl(const Writer *writer)
    : sentence_(0), size_(0), theta_(kDefaultTheta), Z_(0.0),
      request_type_(MECAB_ONE_BEST),
      writer_(writer),
      ostrs_(0),
      allocator_(new Allocator<Node, Path>) {
  begin_nodes_.reserve(MIN_INPUT_BUFFER_SIZE);
  end_nodes_.reserve(MIN_INPUT_BUFFER_SIZE);
}

LatticeImpl::~LatticeImpl() {}

void LatticeImpl::clear() {
  allocator_->free();
  if (ostrs_.get()) {
    ostrs_->clear();
  }
  begin_nodes_.clear();
  end_nodes_.clear();
  feature_constraint_.clear();
  boundary_constraint_.clear();
  size_ = 0;
  theta_ = kDefaultTheta;
  Z_ = 0.0;
  sentence_ = 0;
}

void LatticeImpl::set_sentence(const char *sentence) {
  return set_sentence(sentence, strlen(sentence));
}

void LatticeImpl::set_sentence(const char *sentence, size_t len) {
  clear();
  end_nodes_.resize(len + 4);
  begin_nodes_.resize(len + 4);

  if (has_request_type(MECAB_ALLOCATE_SENTENCE) ||
      has_request_type(MECAB_PARTIAL)) {
    char *new_sentence = allocator()->strdup(sentence, len);
    sentence_ = new_sentence;
  } else {
    sentence_ = sentence;
  }

  size_ = len;
  std::memset(&end_nodes_[0],   0,
              sizeof(end_nodes_[0]) * (len + 4));
  std::memset(&begin_nodes_[0], 0,
              sizeof(begin_nodes_[0]) * (len + 4));
}

bool LatticeImpl::next() {
  if (!has_request_type(MECAB_NBEST)) {
    set_what("MECAB_NBEST request type is not set");
    return false;
  }

  if (!allocator()->nbest_generator()->next()) {
    return false;
  }

  Viterbi::buildResultForNBest(this);
  return true;
}

void LatticeImpl::set_result(const char *result) {
  char *str = allocator()->strdup(result, std::strlen(result));
  std::vector<char *> lines;
  const size_t lsize = tokenize(str, "\n",
                                std::back_inserter(lines),
                                std::strlen(result));
  CHECK_DIE(lsize == lines.size());

  std::string sentence;
  std::vector<std::string> surfaces, features;
  for (size_t i = 0; i < lines.size(); ++i) {
    if (::strcmp("EOS", lines[i]) == 0) {
      break;
    }
    char *cols[2];
    if (tokenize(lines[i], "\t", cols, 2) != 2) {
      break;
    }
    sentence += cols[0];
    surfaces.push_back(cols[0]);
    features.push_back(cols[1]);
  }

  CHECK_DIE(features.size() == surfaces.size());

  set_sentence(allocator()->strdup(sentence.c_str(), sentence.size()));

  Node *bos_node = allocator()->newNode();
  bos_node->surface = const_cast<const char *>(BOS_KEY);  // dummy
  bos_node->feature = "BOS/EOS";
  bos_node->isbest = 1;
  bos_node->stat = MECAB_BOS_NODE;

  Node *eos_node = allocator()->newNode();
  eos_node->surface = const_cast<const char *>(BOS_KEY);  // dummy
  eos_node->feature = "BOS/EOS";
  eos_node->isbest = 1;
  eos_node->stat = MECAB_EOS_NODE;

  bos_node->surface = sentence_;
  end_nodes_[0] = bos_node;

  size_t offset = 0;
  Node *prev = bos_node;
  for (size_t i = 0; i < surfaces.size(); ++i) {
    Node *node = allocator()->newNode();
    node->prev = prev;
    prev->next = node;
    node->surface = sentence_ + offset;
    node->length = surfaces[i].size();
    node->rlength = surfaces[i].size();
    node->isbest = 1;
    node->stat = MECAB_NOR_NODE;
    node->wcost = 0;
    node->cost = 0;
    node->feature = allocator()->strdup(features[i].c_str(),
                                        features[i].size());
    begin_nodes_[offset] = node;
    end_nodes_[offset + node->length] = node;
    offset += node->length;
    prev = node;
  }

  prev->next = eos_node;
  eos_node->prev = prev;
}

// default implementation of Lattice formatter.
namespace {
void writeLattice(Lattice *lattice, StringBuffer *os) {
  for (const Node *node = lattice->bos_node()->next;
       node->next; node = node->next) {
    os->write(node->surface, node->length);
    *os << '\t' << node->feature;
    *os << '\n';
  }
  *os << "EOS\n";
}
}  // namespace

const char *LatticeImpl::toString() {
  return toStringInternal(stream());
}

const char *LatticeImpl::toString(char *buf, size_t size) {
  StringBuffer os(buf, size);
  return toStringInternal(&os);
}

const char *LatticeImpl::toStringInternal(StringBuffer *os) {
  os->clear();
  if (writer_) {
    if (!writer_->write(this, os)) {
      return 0;
    }
  } else {
    writeLattice(this, os);
  }
  *os << '\0';
  if (!os->str()) {
    set_what("output buffer overflow");
    return 0;
  }
  return os->str();
}

const char *LatticeImpl::toString(const Node *node) {
  return toStringInternal(node, stream());
}

const char *LatticeImpl::toString(const Node *node,
                                  char *buf, size_t size) {
  StringBuffer os(buf, size);
  return toStringInternal(node, &os);
}

const char *LatticeImpl::toStringInternal(const Node *node,
                                          StringBuffer *os) {
  os->clear();
  if (!node) {
    set_what("node is NULL");
    return 0;
  }
  if (writer_) {
    if (!writer_->writeNode(this, node, os)) {
      return 0;
    }
  } else {
    os->write(node->surface, node->length);
    *os << '\t' << node->feature;
  }
  *os << '\0';
  if (!os->str()) {
    set_what("output buffer overflow");
    return 0;
  }
  return os->str();
}

const char *LatticeImpl::enumNBestAsString(size_t N) {
  return enumNBestAsStringInternal(N, stream());
}

const char *LatticeImpl::enumNBestAsString(size_t N, char *buf, size_t size) {
  StringBuffer os(buf, size);
  return enumNBestAsStringInternal(N, &os);
}

const char *LatticeImpl::enumNBestAsStringInternal(size_t N,
                                                   StringBuffer *os) {
  os->clear();

  if (N == 0 || N > NBEST_MAX) {
    set_what("nbest size must be 1 <= nbest <= 512");
    return 0;
  }

  for (size_t i = 0; i < N; ++i) {
    if (!next()) {
      break;
    }
    if (writer_) {
      if (!writer_->write(this, os)) {
        return 0;
      }
    } else {
      writeLattice(this, os);
    }
  }

  // make a dummy node for EON
  if (writer_) {
    Node eon_node;
    memset(&eon_node, 0, sizeof(eon_node));
    eon_node.stat = MECAB_EON_NODE;
    eon_node.next = 0;
    eon_node.surface = this->sentence() + this->size();
    if (!writer_->writeNode(this, &eon_node, os)) {
      return 0;
    }
  }
  *os << '\0';

  if (!os->str()) {
    set_what("output buffer overflow");
    return 0;
  }

  return os->str();
}

bool LatticeImpl::has_constraint() const {
  return !boundary_constraint_.empty();
}

int LatticeImpl::boundary_constraint(size_t pos) const {
  if (!boundary_constraint_.empty()) {
    return boundary_constraint_[pos];
  }
  return MECAB_ANY_BOUNDARY;
}

const char *LatticeImpl::feature_constraint(size_t begin_pos) const {
  if (!feature_constraint_.empty()) {
    return feature_constraint_[begin_pos];
  }
  return 0;
}

void LatticeImpl::set_boundary_constraint(size_t pos,
                                          int boundary_constraint_type) {
  if (boundary_constraint_.empty()) {
    boundary_constraint_.resize(size() + 4, MECAB_ANY_BOUNDARY);
  }
  boundary_constraint_[pos] = boundary_constraint_type;
}

void LatticeImpl::set_feature_constraint(size_t begin_pos, size_t end_pos,
                                         const char *feature) {
  if (begin_pos >= end_pos || !feature) {
    return;
  }

  if (feature_constraint_.empty()) {
    feature_constraint_.resize(size() + 4, 0);
  }

  end_pos = std::min(end_pos, size());

  set_boundary_constraint(begin_pos, MECAB_TOKEN_BOUNDARY);
  set_boundary_constraint(end_pos, MECAB_TOKEN_BOUNDARY);
  for (size_t i = begin_pos + 1; i < end_pos; ++i) {
    set_boundary_constraint(i, MECAB_INSIDE_TOKEN);
  }

  feature_constraint_[begin_pos] = feature;
}
}  // namespace

Tagger *Tagger::create(int argc, char **argv) {
  return createTagger(argc, argv);
}

Tagger *Tagger::create(const char *arg) {
  return createTagger(arg);
}

const char *Tagger::version() {
  return VERSION;
}

Tagger *createTagger(int argc, char **argv) {
  TaggerImpl *tagger = new TaggerImpl();
  if (!tagger->open(argc, argv)) {
    setGlobalError(tagger->what());
    delete tagger;
    return 0;
  }
  return tagger;
}

Tagger *createTagger(const char *argv) {
  TaggerImpl *tagger = new TaggerImpl();
  if (!tagger->open(argv)) {
    setGlobalError(tagger->what());
    delete tagger;
    return 0;
  }
  return tagger;
}

void deleteTagger(Tagger *tagger) {
  delete tagger;
}

const char *getTaggerError() {
  return getLastError();
}

const char *getLastError() {
  return getGlobalError();
}

Model *createModel(int argc, char **argv) {
  ModelImpl *model = new ModelImpl;
  if (!model->open(argc, argv)) {
    delete model;
    return 0;
  }
  return model;
}

Model *createModel(const char *arg) {
  ModelImpl *model = new ModelImpl;
  if (!model->open(arg)) {
    delete model;
    return 0;
  }
  return model;
}

void deleteModel(Model *model) {
  delete model;
}

Model *Model::create(int argc, char **argv) {
  return createModel(argc, argv);
}

Model *Model::create(const char *arg) {
  return createModel(arg);
}

const char *Model::version() {
  return VERSION;
}

bool Tagger::parse(const Model &model, Lattice *lattice) {
  scoped_ptr<Tagger> tagger(model.createTagger());
  return tagger->parse(lattice);
}

Lattice *Lattice::create() {
  return createLattice();
}

Lattice *createLattice() {
  return new LatticeImpl;
}

void deleteLattice(Lattice *lattice) {
  delete lattice;
}
}  // MeCab

int mecab_do(int argc, char **argv) {
#define WHAT_ERROR(msg) do {                    \
    std::cout << msg << std::endl;              \
    return EXIT_FAILURE; }                      \
  while (0);

  MeCab::Param param;
  if (!param.open(argc, argv, MeCab::long_options)) {
    std::cout << param.what() << std::endl;
    return EXIT_FAILURE;
  }

  if (param.get<bool>("help")) {
    std::cout << param.help() << std::endl;
    return EXIT_SUCCESS;
  }

  if (param.get<bool>("version")) {
    std::cout << param.version() << std::endl;
    return EXIT_SUCCESS;
  }

  if (!load_dictionary_resource(&param)) {
    std::cout << param.what() << std::endl;
    return EXIT_SUCCESS;
  }

  if (param.get<int>("lattice-level") >= 1) {
    std::cerr << "lattice-level is DEPERCATED. "
              << "use --marginal or --nbest." << std::endl;
  }

  MeCab::scoped_ptr<MeCab::ModelImpl> model(new MeCab::ModelImpl);
  if (!model->open(param)) {
    std::cout << MeCab::getLastError() << std::endl;
    return EXIT_FAILURE;
  }

  std::string ofilename = param.get<std::string>("output");
  if (ofilename.empty()) {
    ofilename = "-";
  }

  const int nbest = param.get<int>("nbest");
  if (nbest <= 0 || nbest > NBEST_MAX) {
    WHAT_ERROR("invalid N value");
  }

  MeCab::ostream_wrapper ofs(ofilename.c_str());
  if (!*ofs) {
    WHAT_ERROR("no such file or directory: " << ofilename);
  }

  if (param.get<bool>("dump-config")) {
    param.dump_config(&*ofs);
    return EXIT_FAILURE;
  }

  if (param.get<bool>("dictionary-info")) {
    for (const MeCab::DictionaryInfo *d = model->dictionary_info();
         d; d = d->next) {
      *ofs << "filename:\t" << d->filename << std::endl;
      *ofs << "version:\t" << d->version << std::endl;
      *ofs << "charset:\t" << d->charset << std::endl;
      *ofs << "type:\t" << d->type   << std::endl;
      *ofs << "size:\t" << d->size << std::endl;
      *ofs << "left size:\t" << d->lsize << std::endl;
      *ofs << "right size:\t" << d->rsize << std::endl;
      *ofs << std::endl;
    }
    return EXIT_FAILURE;
  }

  const std::vector<std::string>& rest_ = param.rest_args();
  std::vector<std::string> rest = rest_;

  if (rest.empty()) {
    rest.push_back("-");
  }

  size_t ibufsize = std::min(MAX_INPUT_BUFFER_SIZE,
                             std::max(param.get<int>
                                            ("input-buffer-size"),
                                            MIN_INPUT_BUFFER_SIZE));

  const bool partial = param.get<bool>("partial");
  if (partial) {
    ibufsize *= 8;
  }

  MeCab::scoped_array<char> ibuf_data(new char[ibufsize]);
  char *ibuf = ibuf_data.get();

  MeCab::scoped_ptr<MeCab::Tagger> tagger(model->createTagger());

  if (!tagger.get()) {
    WHAT_ERROR("cannot create tagger");
  }

  for (size_t i = 0; i < rest.size(); ++i) {
    MeCab::istream_wrapper ifs(rest[i].c_str());
    if (!*ifs) {
      WHAT_ERROR("no such file or directory: " << rest[i]);
    }

    while (true) {
      if (!partial) {
        ifs->getline(ibuf, ibufsize);
      } else {
        std::string sentence;
        MeCab::scoped_fixed_array<char, BUF_SIZE> line;
        for (;;) {
          if (!ifs->getline(line.get(), line.size())) {
            ifs->clear(std::ios::eofbit|std::ios::badbit);
            break;
          }
          sentence += line.get();
          sentence += '\n';
          if (std::strcmp(line.get(), "EOS") == 0 || line[0] == '\0') {
            break;
          }
        }
        std::strncpy(ibuf, sentence.c_str(), ibufsize);
      }
      if (ifs->eof() && !ibuf[0]) {
        return false;
      }
      if (ifs->fail()) {
        std::cerr << "input-buffer overflow. "
                  << "The line is split. use -b #SIZE option." << std::endl;
        ifs->clear();
      }
      const char *r = (nbest >= 2) ? tagger->parseNBest(nbest, ibuf) :
          tagger->parse(ibuf);
      if (!r)  {
        WHAT_ERROR(tagger->what());
      }
      *ofs << r << std::flush;
    }
  }

  return EXIT_SUCCESS;

#undef WHAT_ERROR
}

// MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2011 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <algorithm>
#include <iterator>
#include <cmath>
#include <cstring>
#include "common.h"
#include "connector.h"
#include "mecab.h"
#include "nbest_generator.h"
#include "param.h"
#include "viterbi.h"
#include "scoped_ptr.h"
#include "string_buffer.h"
#include "tokenizer.h"

namespace MeCab {

namespace {
void calc_alpha(Node *n, double beta) {
  n->alpha = 0.0;
  for (Path *path = n->lpath; path; path = path->lnext) {
    n->alpha = logsumexp(n->alpha,
                         -beta * path->cost + path->lnode->alpha,
                         path == n->lpath);
  }
}

void calc_beta(Node *n, double beta) {
  n->beta = 0.0;
  for (Path *path = n->rpath; path; path = path->rnext) {
    n->beta = logsumexp(n->beta,
                        -beta * path->cost + path->rnode->beta,
                        path == n->rpath);
  }
}
}  // namespace

Viterbi::Viterbi()
    :  tokenizer_(0), connector_(0),
       cost_factor_(0) {}

Viterbi::~Viterbi() {}

bool Viterbi::open(const Param &param) {
  tokenizer_.reset(new Tokenizer<Node, Path>);
  CHECK_FALSE(tokenizer_->open(param)) << tokenizer_->what();
  CHECK_FALSE(tokenizer_->dictionary_info()) << "Dictionary is empty";

  connector_.reset(new Connector);
  CHECK_FALSE(connector_->open(param)) << connector_->what();

  CHECK_FALSE(tokenizer_->dictionary_info()->lsize ==
              connector_->left_size() &&
              tokenizer_->dictionary_info()->rsize ==
              connector_->right_size())
      << "Transition table and dictionary are not compatible";

  cost_factor_ = param.get<int>("cost-factor");
  if (cost_factor_ == 0) {
    cost_factor_ = 800;
  }

  return true;
}

bool Viterbi::analyze(Lattice *lattice) const {
  if (!lattice || !lattice->sentence()) {
    return false;
  }

  if (!initPartial(lattice)) {
    return false;
  }

  bool result = false;
  if (lattice->has_request_type(MECAB_NBEST) ||
      lattice->has_request_type(MECAB_MARGINAL_PROB)) {
    // IsAllPath=true
    if (lattice->has_constraint()) {
      result = viterbi<true, true>(lattice);
    } else {
      result = viterbi<true, false>(lattice);
    }
  } else {
    // IsAllPath=false
    if (lattice->has_constraint()) {
      result = viterbi<false, true>(lattice);
    } else {
      result = viterbi<false, false>(lattice);
    }
  }

  if (!result) {
    return false;
  }

  if (!forwardbackward(lattice)) {
    return false;
  }

  if (!buildBestLattice(lattice)) {
    return false;
  }

  if (!buildAllLattice(lattice)) {
    return false;
  }

  if (!initNBest(lattice)) {
    return false;
  }

  return true;
}

const Tokenizer<Node, Path> *Viterbi::tokenizer() const {
  return tokenizer_.get();
}

const Connector *Viterbi::connector() const {
  return connector_.get();
}

// static
bool Viterbi::forwardbackward(Lattice *lattice) {
  if (!lattice->has_request_type(MECAB_MARGINAL_PROB)) {
    return true;
  }

  Node **end_node_list   = lattice->end_nodes();
  Node **begin_node_list = lattice->begin_nodes();

  const size_t len = lattice->size();
  const double theta = lattice->theta();

  end_node_list[0]->alpha = 0.0;
  for (int pos = 0; pos <= static_cast<long>(len); ++pos) {
    for (Node *node = begin_node_list[pos]; node; node = node->bnext) {
      calc_alpha(node, theta);
    }
  }

  begin_node_list[len]->beta = 0.0;
  for (int pos = static_cast<long>(len); pos >= 0; --pos) {
    for (Node *node = end_node_list[pos]; node; node = node->enext) {
      calc_beta(node, theta);
    }
  }

  const double Z = begin_node_list[len]->alpha;
  lattice->set_Z(Z);  // alpha of EOS

  for (int pos = 0; pos <= static_cast<long>(len); ++pos) {
    for (Node *node = begin_node_list[pos]; node; node = node->bnext) {
      node->prob = std::exp(node->alpha + node->beta - Z);
      for (Path *path = node->lpath; path; path = path->lnext) {
        path->prob = std::exp(path->lnode->alpha
                              - theta * path->cost
                              + path->rnode->beta - Z);
      }
    }
  }

  return true;
}

// static
bool Viterbi::buildResultForNBest(Lattice *lattice) {
  return buildAllLattice(lattice);
}

// static
bool Viterbi::buildAllLattice(Lattice *lattice) {
  if (!lattice->has_request_type(MECAB_ALL_MORPHS)) {
    return true;
  }

  Node *prev = lattice->bos_node();
  const size_t len = lattice->size();
  Node **begin_node_list = lattice->begin_nodes();

  for (long pos = 0; pos <= static_cast<long>(len); ++pos) {
    for (Node *node = begin_node_list[pos]; node; node = node->bnext) {
      prev->next = node;
      node->prev = prev;
      prev = node;
    }
  }

  return true;
}

// static
bool Viterbi::buildAlternative(Lattice *lattice) {
  Node **begin_node_list = lattice->begin_nodes();

  const Node *bos_node = lattice->bos_node();
  for (const Node *node = bos_node; node; node = node->next) {
    if (node->stat == MECAB_BOS_NODE || node->stat == MECAB_EOS_NODE) {
      continue;
    }
    const size_t pos = node->surface - lattice->sentence() -
        node->rlength + node->length;
    std::cout.write(node->surface, node->length);
    std::cout << "\t" << node->feature << std::endl;
    for (const Node *anode = begin_node_list[pos];
         anode; anode = anode->bnext) {
      if (anode->rlength == node->rlength &&
          anode->length == node->length) {
        std::cout << "@ ";
        std::cout.write(anode->surface, anode->length);
        std::cout << "\t" << anode->feature << std::endl;
      }
    }
  }

  std::cout << "EOS" << std::endl;

  return true;
}

// static
bool Viterbi::buildBestLattice(Lattice *lattice) {
  Node *node = lattice->eos_node();
  for (Node *prev_node; node->prev;) {
    node->isbest = 1;
    prev_node = node->prev;
    prev_node->next = node;
    node = prev_node;
  }

  return true;
}

// static
bool Viterbi::initNBest(Lattice *lattice) {
  if (!lattice->has_request_type(MECAB_NBEST)) {
    return true;
  }
  lattice->allocator()->nbest_generator()->set(lattice);
  return true;
}

// static
bool Viterbi::initPartial(Lattice *lattice) {
  if (!lattice->has_request_type(MECAB_PARTIAL)) {
    if (lattice->has_constraint()) {
      lattice->set_boundary_constraint(0, MECAB_TOKEN_BOUNDARY);
      lattice->set_boundary_constraint(lattice->size(),
                                       MECAB_TOKEN_BOUNDARY);
    }
    return true;
  }

  Allocator<Node, Path> *allocator = lattice->allocator();
  char *str = allocator->partial_buffer(lattice->size() + 1);
  strncpy(str, lattice->sentence(), lattice->size() + 1);

  std::vector<char *> lines;
  const size_t lsize = tokenize(str, "\n",
                                std::back_inserter(lines),
                                lattice->size() + 1);
  char* column[2];
  scoped_array<char> buf(new char[lattice->size() + 1]);
  StringBuffer os(buf.get(), lattice->size() + 1);

  std::vector<std::pair<char *, char *> > tokens;
  tokens.reserve(lsize);

  size_t pos = 0;
  for (size_t i = 0; i < lsize; ++i) {
    const size_t size = tokenize(lines[i], "\t", column, 2);
    if (size == 1 && std::strcmp(column[0], "EOS") == 0) {
      break;
    }
    const size_t len = std::strlen(column[0]);
    if (size == 2) {
      tokens.push_back(std::make_pair(column[0], column[1]));
    } else {
      tokens.push_back(std::make_pair(column[0], reinterpret_cast<char *>(0)));
    }
    os << column[0];
    pos += len;
  }

  os << '\0';

  lattice->set_sentence(os.str());

  pos = 0;
  for (size_t i = 0; i < tokens.size(); ++i) {
    const char *surface = tokens[i].first;
    const char *feature = tokens[i].second;
    const size_t len = std::strlen(surface);
    lattice->set_boundary_constraint(pos, MECAB_TOKEN_BOUNDARY);
    lattice->set_boundary_constraint(pos + len, MECAB_TOKEN_BOUNDARY);
    if (feature) {
      lattice->set_feature_constraint(pos, pos + len, feature);
      for (size_t n = 1; n < len; ++n) {
        lattice->set_boundary_constraint(pos + n,
                                         MECAB_INSIDE_TOKEN);
      }
    }
    pos += len;
  }

  return true;
}

namespace {
template <bool IsAllPath> bool connect(size_t pos, Node *rnode,
                                       Node **begin_node_list,
                                       Node **end_node_list,
                                       const Connector *connector,
                                       Allocator<Node, Path> *allocator) {
  for (;rnode; rnode = rnode->bnext) {
    long best_cost = 2147483647;
    Node* best_node = 0;
    for (Node *lnode = end_node_list[pos]; lnode; lnode = lnode->enext) {
      int lcost = connector->cost(lnode, rnode);  // local cost
      long cost = lnode->cost + lcost;

      if (cost < best_cost) {
        best_node  = lnode;
        best_cost  = cost;
      }

      if (IsAllPath) {
        Path *path   = allocator->newPath();
        path->cost   = lcost;
        path->rnode  = rnode;
        path->lnode  = lnode;
        path->lnext  = rnode->lpath;
        rnode->lpath = path;
        path->rnext  = lnode->rpath;
        lnode->rpath = path;
      }
    }

    // overflow check 2003/03/09
    if (!best_node) {
      return false;
    }

    rnode->prev = best_node;
    rnode->next = 0;
    rnode->cost = best_cost;
    const size_t x = rnode->rlength + pos;
    rnode->enext = end_node_list[x];
    end_node_list[x] = rnode;
  }

  return true;
}
}  // namespace

template <bool IsAllPath, bool IsPartial>
bool Viterbi::viterbi(Lattice *lattice) const {
  Node **end_node_list   = lattice->end_nodes();
  Node **begin_node_list = lattice->begin_nodes();
  Allocator<Node, Path> *allocator = lattice->allocator();
  const size_t len = lattice->size();
  const char *begin = lattice->sentence();
  const char *end = begin + len;

  Node *bos_node = tokenizer_->getBOSNode(lattice->allocator());
  bos_node->surface = lattice->sentence();
  end_node_list[0] = bos_node;

  for (size_t pos = 0; pos < len; ++pos) {
    if (end_node_list[pos]) {
      Node *right_node = tokenizer_->lookup<IsPartial>(begin + pos, end,
                                                       allocator, lattice);
      begin_node_list[pos] = right_node;
      if (!connect<IsAllPath>(pos, right_node,
                              begin_node_list,
                              end_node_list,
                              connector_.get(),
                              allocator)) {
        lattice->set_what("too long sentence.");
        return false;
      }
    }
  }

  Node *eos_node = tokenizer_->getEOSNode(lattice->allocator());
  eos_node->surface = lattice->sentence() + lattice->size();
  begin_node_list[lattice->size()] = eos_node;

  for (long pos = len; static_cast<long>(pos) >= 0; --pos) {
    if (end_node_list[pos]) {
      if (!connect<IsAllPath>(pos, eos_node,
                              begin_node_list,
                              end_node_list,
                              connector_.get(),
                              allocator)) {
        lattice->set_what("too long sentence.");
        return false;
      }
      break;
    }
  }

  end_node_list[0] = bos_node;
  begin_node_list[lattice->size()] = eos_node;

  return true;
}
}  // Mecab

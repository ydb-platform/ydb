//  MeCab -- Yet Another Part-of-Speech and Morphological Analyzer
//
//
//  Copyright(C) 2001-2011 Taku Kudo <taku@chasen.org>
//  Copyright(C) 2004-2006 Nippon Telegraph and Telephone Corporation
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include "common.h"
#include "param.h"
#include "string_buffer.h"
#include "utils.h"
#include "writer.h"

namespace MeCab {

Writer::Writer() : write_(&Writer::writeLattice) {}
Writer::~Writer() {}

void Writer::close() {
  write_ = &Writer::writeLattice;
}

bool Writer::open(const Param &param) {
  const std::string ostyle = param.get<std::string>("output-format-type");
  write_ = &Writer::writeLattice;

  if (ostyle == "wakati") {
    write_ = &Writer::writeWakati;
  } else if (ostyle == "none") {
    write_ = &Writer::writeNone;
  } else if (ostyle == "dump") {
    write_ = &Writer::writeDump;
  } else if (ostyle == "em") {
    write_ = &Writer::writeEM;
  } else {
    // default values
    std::string node_format = "%m\\t%H\\n";
    std::string unk_format  = "%m\\t%H\\n";
    std::string bos_format  = "";
    std::string eos_format  = "EOS\\n";
    std::string eon_format  = "";

    std::string node_format_key = "node-format";
    std::string bos_format_key  = "bos-format";
    std::string eos_format_key  = "eos-format";
    std::string unk_format_key  = "unk-format";
    std::string eon_format_key  = "eon-format";

    if (!ostyle.empty()) {
      node_format_key += "-";
      node_format_key += ostyle;
      bos_format_key += "-";
      bos_format_key += ostyle;
      eos_format_key += "-";
      eos_format_key += ostyle;
      unk_format_key += "-";
      unk_format_key += ostyle;
      eon_format_key += "-";
      eon_format_key += ostyle;
      const std::string tmp = param.get<std::string>(node_format_key.c_str());
      CHECK_FALSE(!tmp.empty()) << "unkown format type [" << ostyle << "]";
    }

    const std::string node_format2 =
        param.get<std::string>(node_format_key.c_str());
    const std::string bos_format2 =
        param.get<std::string>(bos_format_key.c_str());
    const std::string eos_format2 =
        param.get<std::string>(eos_format_key.c_str());
    const std::string unk_format2 =
        param.get<std::string>(unk_format_key.c_str());
    const std::string eon_format2 =
        param.get<std::string>(eon_format_key.c_str());

    if (node_format != node_format2 || bos_format != bos_format2 ||
        eos_format != eos_format2 || unk_format != unk_format2) {
      write_ = &Writer::writeUser;
      if (node_format != node_format2) {
        node_format = node_format2;
      }
      if (bos_format != bos_format2) {
        bos_format = bos_format2;
      }
      if (eos_format != eos_format2) {
        eos_format = eos_format2;
      }
      if (unk_format != unk_format2) {
        unk_format = unk_format2;
      } else if (node_format != node_format2) {
        unk_format = node_format2;
      } else {
        unk_format = node_format;
      }
      if (eon_format != eon_format2) {
        eon_format = eon_format2;
      }
      node_format_.reset_string(node_format.c_str());
      bos_format_.reset_string(bos_format.c_str());
      eos_format_.reset_string(eos_format.c_str());
      unk_format_.reset_string(unk_format.c_str());
      eon_format_.reset_string(eon_format.c_str());
    }
  }

  return true;
}

bool Writer::write(Lattice *lattice, StringBuffer *os) const {
  if (!lattice || !lattice->is_available()) {
    return false;
  }
  return (this->*write_)(lattice, os);
}

bool Writer::writeLattice(Lattice *lattice, StringBuffer *os) const {
  for (const Node *node = lattice->bos_node()->next;
       node->next; node = node->next) {
    os->write(node->surface, node->length);
    *os << '\t' << node->feature;  // << '\t';
    *os << '\n';
  }
  *os << "EOS\n";
  return true;
}

bool Writer::writeWakati(Lattice *lattice, StringBuffer *os) const {
  for (const Node *node = lattice->bos_node()->next;
       node->next; node = node->next) {
    os->write(node->surface, node->length);
    *os << ' ';
  }
  *os << '\n';
  return true;
}

bool Writer::writeNone(Lattice *lattice, StringBuffer *os) const {
  return true;  // do nothing
}

bool Writer::writeEM(Lattice *lattice, StringBuffer *os) const {
  static const float min_prob = 0.0001;
  for (const Node *node = lattice->bos_node(); node; node = node->next) {
    if (node->prob >= min_prob) {
      *os << "U\t";
      if (node->stat == MECAB_BOS_NODE) {
        *os << "BOS";
      } else if (node->stat == MECAB_EOS_NODE) {
        *os << "EOS";
      }  else {
        os->write(node->surface, node->length);
      }
      *os << '\t' << node->feature << '\t' << node->prob << '\n';
    }
    for (const Path *path = node->lpath; path; path = path->lnext) {
      if (path->prob >= min_prob) {
        *os << "B\t" << path->lnode->feature << '\t'
            << node->feature << '\t' << path->prob << '\n';
      }
    }
  }
  *os << "EOS\n";
  return true;
}

bool Writer::writeDump(Lattice *lattice, StringBuffer *os) const {
  const char *str = lattice->sentence();
  for (const Node *node = lattice->bos_node(); node; node = node->next) {
    *os << node->id << ' ';
    if (node->stat == MECAB_BOS_NODE) {
      *os << "BOS";
    } else if (node->stat == MECAB_EOS_NODE) {
      *os << "EOS";
    } else {
      os->write(node->surface, node->length);
    }

    *os << ' ' << node->feature
        << ' ' << static_cast<int>(node->surface - str)
        << ' ' << static_cast<int>(node->surface - str + node->length)
        << ' ' << node->rcAttr
        << ' ' << node->lcAttr
        << ' ' << node->posid
        << ' ' << static_cast<int>(node->char_type)
        << ' ' << static_cast<int>(node->stat)
        << ' ' << static_cast<int>(node->isbest)
        << ' ' << node->alpha
        << ' ' << node->beta
        << ' ' << node->prob
        << ' ' << node->cost;

    for (const Path *path = node->lpath; path; path = path->lnext) {
      *os << ' ' << path->lnode->id << ':' << path->cost << ':' << path->prob;
    }
    *os << '\n';
  }
  return true;
}

bool Writer::writeUser(Lattice *lattice, StringBuffer *os) const {
  if (!writeNode(lattice, bos_format_.get(), lattice->bos_node(), os)) {
    return false;
  }
  const Node *node = 0;
  for (node = lattice->bos_node()->next; node->next; node = node->next) {
    const char *fmt = (node->stat == MECAB_UNK_NODE ? unk_format_.get() :
                       node_format_.get());
    if (!writeNode(lattice, fmt, node, os)) {
      return false;
    }
  }
  if (!writeNode(lattice, eos_format_.get(), node, os)) {
    return false;
  }
  return true;
}

bool Writer::writeNode(Lattice *lattice, const Node *node,
                       StringBuffer *os) const {
  switch (node->stat) {
    case MECAB_BOS_NODE:
      return writeNode(lattice, bos_format_.get(), node, os);
    case MECAB_EOS_NODE:
      return writeNode(lattice, eos_format_.get(), node, os);
    case MECAB_UNK_NODE:
      return writeNode(lattice, unk_format_.get(), node, os);
    case MECAB_NOR_NODE:
      return writeNode(lattice, node_format_.get(), node, os);
    case MECAB_EON_NODE:
      return writeNode(lattice, eon_format_.get(), node, os);
  }
  return true;
}

bool Writer::writeNode(Lattice *lattice,
                       const char *p,
                       const Node *node,
                       StringBuffer *os) const {
  scoped_fixed_array<char, BUF_SIZE> buf;
  scoped_fixed_array<char *, 64> ptr;
  size_t psize = 0;

  for (; *p; p++) {
    switch (*p) {
      default: *os << *p; break;

      case '\\': *os << getEscapedChar(*++p); break;

      case '%': {  // macros
        switch (*++p) {
          default: {
            const std::string error = "unknown meta char: " + *p;
            lattice->set_what(error.c_str());
            return false;
          }
            // input sentence
          case 'S': os->write(lattice->sentence(), lattice->size()); break;
            // sentence length
          case 'L': *os << lattice->size(); break;
            // morph
          case 'm': os->write(node->surface, node->length); break;
          case 'M': os->write(reinterpret_cast<const char *>
                              (node->surface - node->rlength + node->length),
                              node->rlength);
            break;
          case 'h': *os << node->posid; break;  // Part-Of-Speech ID
          case '%': *os << '%'; break;         // %
          case 'c': *os << static_cast<int>(node->wcost); break;  // word cost
          case 'H': *os << node->feature; break;
          case 't': *os << static_cast<unsigned int>(node->char_type); break;
          case 's': *os << static_cast<unsigned int>(node->stat); break;
          case 'P': *os << node->prob; break;
          case 'p': {
            switch (*++p) {
              default:
                lattice->set_what("[iseSCwcnblLh] is required after %p");
                return false;
              case 'i': *os << node->id; break;  // node id
              case 'S': os->write(reinterpret_cast<const char*>
                                  (node->surface -
                                   node->rlength + node->length),
                                  node->rlength - node->length);
                break;  // space
                // start position
              case 's': *os << static_cast<int>(
                  node->surface - lattice->sentence());
                break;
                // end position
              case 'e': *os << static_cast<int>
                    (node->surface - lattice->sentence() + node->length);
                break;
                // connection cost
              case 'C': *os << node->cost -
                    node->prev->cost - node->wcost;
                break;
              case 'w': *os << node->wcost; break;  // word cost
              case 'c': *os << node->cost; break;  // best cost
              case 'n': *os << (node->cost - node->prev->cost); break;
                // node cost
                // * if best path, otherwise ' '
              case 'b': *os << (node->isbest ? '*' : ' '); break;
              case 'P': *os << node->prob; break;
              case 'A': *os << node->alpha; break;
              case 'B': *os << node->beta; break;
              case 'l': *os << node->length; break;  // length of morph
                // length of morph including the spaces
              case 'L': *os << node->rlength;    break;
              case 'h': {  // Hidden Layer ID
                switch (*++p) {
                  default:
                    lattice->set_what("lr is required after %ph");
                    return false;
                  case 'l': *os << node->lcAttr; break;   // current
                  case 'r': *os << node->rcAttr; break;   // prev
                }
              } break;

              case 'p': {
                char mode = *++p;
                char sep = *++p;
                if (sep == '\\') {
                  sep = getEscapedChar(*++p);
                }
                if (!node->lpath) {
                  lattice->set_what("no path information is available");
                  return false;
                }
                for (Path *path = node->lpath; path; path = path->lnext) {
                  if (path != node->lpath) *os << sep;
                  switch (mode) {
                    case 'i': *os << path->lnode->id; break;
                    case 'c': *os << path->cost; break;
                    case 'P': *os << path->prob; break;
                    default:
                      lattice->set_what("[icP] is required after %pp");
                      return false;
                  }
                }
              } break;

            }
          } break;

          case 'F':
          case 'f': {
            if (node->feature[0] == '\0') {
              lattice->set_what("no feature information available");
              return false;
            }
            if (!psize) {
              std::strncpy(buf.get(), node->feature, buf.size());
              psize = tokenizeCSV(buf.get(), ptr.get(), ptr.size());
            }

            // separator
            char separator = '\t';  // default separator
            if (*p == 'F') {  // change separator
              if (*++p == '\\') {
                separator = getEscapedChar(*++p);
              } else {
                separator = *p;
              }
            }

            if (*++p !='[') {
              lattice->set_what("cannot find '['");
              return false;
            }
            size_t n = 0;
            bool sep = false;
            bool isfil = false;
            p++;

            for (;; ++p) {
              switch (*p) {
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                  n = 10 * n +(*p - '0');
                  break;
                case ',': case ']':
                  if (n >= psize) {
                    lattice->set_what("given index is out of range");
                    return false;
                  }
                  isfil = (ptr[n][0] != '*');
                  if (isfil) {
                    if (sep) {
                      *os << separator;
                    }
                    *os << ptr[n];
                  }
                  if (*p == ']') {
                    goto last;
                  }
                  sep = isfil;
                  n = 0;
                  break;
                default:
                  lattice->set_what("cannot find ']'");
                  return false;
              }
            }
          } last: break;
        }  // end switch
      } break;  // end case '%'
    }  // end switch
  }

  return true;
}
}

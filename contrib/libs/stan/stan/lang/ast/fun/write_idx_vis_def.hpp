#ifndef STAN_LANG_AST_FUN_WRITE_IDX_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_WRITE_IDX_VIS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace lang {
write_idx_vis::write_idx_vis() {}

std::string write_idx_vis::operator()(const lb_idx& idx) const {
  std::stringstream ss;
  ss << idx.lb_.to_string();
  ss << ":";
  return ss.str();
}

std::string write_idx_vis::operator()(const lub_idx& idx) const {
  std::stringstream ss;
  ss << idx.lb_.to_string();
  ss << ":";
  ss << idx.ub_.to_string();
  return ss.str();
}

std::string write_idx_vis::operator()(const multi_idx& idx) const {
  return idx.idxs_.to_string();
}

std::string write_idx_vis::operator()(const omni_idx& idx) const {
  return ":";
}

std::string write_idx_vis::operator()(const ub_idx& idx) const {
  std::stringstream ss;
  ss << ":";
  ss << idx.ub_.to_string();
  return ss.str();
}

std::string write_idx_vis::operator()(const uni_idx& idx) const {
  return idx.idx_.to_string();
}

}  // namespace lang
}  // namespace stan
#endif

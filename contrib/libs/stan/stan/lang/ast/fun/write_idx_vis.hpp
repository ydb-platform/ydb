#ifndef STAN_LANG_AST_FUN_WRITE_IDX_VIS_HPP
#define STAN_LANG_AST_FUN_WRITE_IDX_VIS_HPP

#include <stan/lang/ast/node/lb_idx.hpp>
#include <stan/lang/ast/node/lub_idx.hpp>
#include <stan/lang/ast/node/multi_idx.hpp>
#include <stan/lang/ast/node/omni_idx.hpp>
#include <stan/lang/ast/node/ub_idx.hpp>
#include <stan/lang/ast/node/uni_idx.hpp>
#include <boost/variant/static_visitor.hpp>
#include <ostream>
#include <string>

namespace stan {
namespace lang {

/**
 * Visitor to format idx for parser error messages.
 */
struct write_idx_vis : public boost::static_visitor<std::string> {
  /**
   * Construct a visitor.
   */
  write_idx_vis();

  /**
   * Return string representation for idx.
   */
  std::string operator()(const lb_idx& idx) const;

  /**
   * Return string representation for idx.
   */
  std::string operator()(const lub_idx& idx) const;

  /**
   * Return string representation for idx.
   */
  std::string operator()(const multi_idx& idx) const;

  /**
   * Return string representation for idx.
   */
  std::string operator()(const omni_idx& idx) const;

  /**
   * Return string representation for idx.
   */
  std::string operator()(const ub_idx& idx) const;

  /**
   * Return string representation for idx.
   */
  std::string operator()(const uni_idx& idx) const;
};

}  // namespace lang
}  // namespace stan
#endif

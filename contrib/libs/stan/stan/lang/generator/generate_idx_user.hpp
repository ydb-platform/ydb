#ifndef STAN_LANG_GENERATOR_GENERATE_IDX_USER_HPP
#define STAN_LANG_GENERATOR_GENERATE_IDX_USER_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/idx_user_visgen.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate specified multiple index for user on specified
     * stream.
     *
     * @param[in] i multiple index
     * @param[in,out] o stream for generating
     */
    void generate_idx_user(const idx& i, std::ostream& o) {
      idx_user_visgen vis(o);
      boost::apply_visitor(vis, i.idx_);
    }

  }
}
#endif

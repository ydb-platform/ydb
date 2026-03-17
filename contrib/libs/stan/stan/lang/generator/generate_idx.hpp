#ifndef STAN_LANG_GENERATOR_GENERATE_IDX_HPP
#define STAN_LANG_GENERATOR_GENERATE_IDX_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/idx_visgen.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the specified multiple index on the specified stream.
     *
     * @param[in] i multiple index to generate
     * @param[in,out] o stream for generating
     */
    void generate_idx(const idx& i, std::ostream& o) {
      idx_visgen vis(o);
      boost::apply_visitor(vis, i.idx_);
    }

  }
}
#endif

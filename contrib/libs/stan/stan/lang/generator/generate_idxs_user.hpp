#ifndef STAN_LANG_GENERATOR_GENERATE_IDXS_USER_HPP
#define STAN_LANG_GENERATOR_GENERATE_IDXS_USER_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_idx_user.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the specified multiple indexes for users to the
     * specified stream.
     *
     * @param[in] idxs multiple indexes
     * @param[in,out] o stream for writing
     */
    void generate_idxs_user(const std::vector<idx>& idxs, std::ostream& o) {
      if (idxs.size() == 0)
        return;
      o << "[";
      for (size_t i = 0; i < idxs.size(); ++i) {
        if (i > 0)
          o << ", ";
        generate_idx_user(idxs[i], o);
      }
      o << "]";
    }

  }
}
#endif

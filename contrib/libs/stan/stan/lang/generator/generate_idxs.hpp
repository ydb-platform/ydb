#ifndef STAN_LANG_GENERATOR_GENERATE_IDXS_HPP
#define STAN_LANG_GENERATOR_GENERATE_IDXS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_idx.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Recursive helper function tracking position to generate
     * specified multiple indexes on the specified stream in order to
     * terminate with a nil index properly.
     *
     * @param[in] pos position in list to generate next
     * @param[in] idxs multiple indexes to generate
     * @param[in,out] o stream for generating
     */
    void generate_idxs(size_t pos, const std::vector<idx>& idxs,
                       std::ostream& o) {
      if (pos == idxs.size()) {
        o << "stan::model::nil_index_list()";
      } else {
        o << "stan::model::cons_list(";
        generate_idx(idxs[pos], o);
        o << ", ";
        generate_idxs(pos + 1, idxs, o);
        o << ")";
      }
    }

    /**
     * Generate the specified multiple indexes on the specified stream.
     *
     * @param[in] idxs multiple indexes to generate
     * @param[in,out] o stream for generating
     */
    void generate_idxs(const std::vector<idx>& idxs, std::ostream& o) {
      generate_idxs(0, idxs, o);
    }

  }
}
#endif

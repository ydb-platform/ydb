#ifndef STAN_LANG_GENERATOR_GENERATE_TYPEDEFS_HPP
#define STAN_LANG_GENERATOR_GENERATE_TYPEDEFS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_typedef.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the typedefs required for the Stan model class to the
     * specified stream.
     *
     * @param[in,out] o stream for generating
     */
    void generate_typedefs(std::ostream& o) {
      generate_typedef("Eigen::Matrix<double, Eigen::Dynamic, 1>",
                       "vector_d", o);
      generate_typedef("Eigen::Matrix<double, 1, Eigen::Dynamic>",
                       "row_vector_d", o);
      generate_typedef("Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>",
                       "matrix_d", o);
      o << EOL;
    }

  }
}
#endif

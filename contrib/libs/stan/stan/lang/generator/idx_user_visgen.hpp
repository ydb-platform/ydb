#ifndef STAN_LANG_GENERATOR_IDX_USER_VISGEN_HPP
#define STAN_LANG_GENERATOR_IDX_USER_VISGEN_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/visgen.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Visitor for generating user-facing multiple index.
     */
    struct idx_user_visgen : public visgen {
      /**
       * Construct a visitor for generating user-facing multiple
       * indexex writing to the specified stream.
       *
       * @param[in,out] o stream for generating
       */
      explicit idx_user_visgen(std::ostream& o): visgen(o) { }

      void operator()(const uni_idx& i) const {
        generate_expression(i.idx_, USER_FACING, o_);
      }

      void operator()(const multi_idx& i) const {
        generate_expression(i.idxs_, USER_FACING, o_);
      }

      void operator()(const omni_idx& i) const {
        o_ << " ";
      }

      void operator()(const lb_idx& i) const {
        generate_expression(i.lb_, USER_FACING, o_);
        o_ << ": ";
      }

      void operator()(const ub_idx& i) const {
        o_ << " :";
        generate_expression(i.ub_, USER_FACING, o_);
      }

      void operator()(const lub_idx& i) const {
        generate_expression(i.lb_, USER_FACING, o_);
        o_ << ":";
        generate_expression(i.ub_, USER_FACING, o_);
      }
    };

  }
}
#endif

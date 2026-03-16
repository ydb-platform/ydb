#ifndef STAN_LANG_GENERATOR_IDX_VISGEN_HPP
#define STAN_LANG_GENERATOR_IDX_VISGEN_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/visgen.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    struct idx_visgen : public visgen {
      /**
       * Construct a visitor for generating multiple indexes.
       *
       * @param o stream for generating
       */
      explicit idx_visgen(std::ostream& o): visgen(o) { }

      void operator()(const uni_idx& i) const {
        o_ << "stan::model::index_uni(";
        generate_expression(i.idx_, NOT_USER_FACING, o_);
        o_ << ")";
      }

      void operator()(const multi_idx& i) const {
        o_ << "stan::model::index_multi(";
        generate_expression(i.idxs_, NOT_USER_FACING, o_);
        o_ << ")";
      }

      void operator()(const omni_idx& i) const {
        o_ << "stan::model::index_omni()";
      }

      void operator()(const lb_idx& i) const {
        o_ << "stan::model::index_min(";
        generate_expression(i.lb_, NOT_USER_FACING, o_);
        o_ << ")";
      }

      void operator()(const ub_idx& i) const {
        o_ << "stan::model::index_max(";
        generate_expression(i.ub_, NOT_USER_FACING, o_);
        o_ << ")";
      }

      void operator()(const lub_idx& i) const {
        o_ << "stan::model::index_min_max(";
        generate_expression(i.lb_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(i.ub_, NOT_USER_FACING, o_);
        o_ << ")";
      }
    };

  }
}
#endif

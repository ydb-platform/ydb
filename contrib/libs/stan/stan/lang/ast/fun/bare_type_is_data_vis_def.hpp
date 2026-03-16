#ifndef STAN_LANG_AST_FUN_BARE_TYPE_IS_DATA_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_IS_DATA_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {
    bare_type_is_data_vis::bare_type_is_data_vis() { }

    bool bare_type_is_data_vis::operator()(const bare_array_type& x) const {
      return x.contains().is_data();
    }

    bool bare_type_is_data_vis::operator()(const double_type& x) const {
      return x.is_data_;
    }

    bool bare_type_is_data_vis::operator()(const ill_formed_type& x) const {
      return false;
    }

    bool bare_type_is_data_vis::operator()(const int_type& x) const {
      return x.is_data_;
    }

    bool bare_type_is_data_vis::operator()(const matrix_type& x) const {
      return x.is_data_;
    }

    bool bare_type_is_data_vis::operator()(const row_vector_type& x) const {
      return x.is_data_;
    }

    bool bare_type_is_data_vis::operator()(const vector_type& x) const {
      return x.is_data_;
    }

    bool bare_type_is_data_vis::operator()(const void_type& x) const {
      return false;
    }
  }
}
#endif

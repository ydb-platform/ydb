#ifndef STAN_LANG_AST_FUN_BARE_TYPE_SET_IS_DATA_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_SET_IS_DATA_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {
    bare_type_set_is_data_vis::bare_type_set_is_data_vis() { }

    void bare_type_set_is_data_vis::operator()(bare_array_type& x) const {
      x.is_data_ = true;
      x.element_type_.set_is_data();
    }

    void bare_type_set_is_data_vis::operator()(double_type& x) const {
      x.is_data_ = true;
    }

    void bare_type_set_is_data_vis::operator()(ill_formed_type& x) const {
      // do nothing
    }

    void bare_type_set_is_data_vis::operator()(int_type& x) const {
      x.is_data_ = true;
    }

    void bare_type_set_is_data_vis::operator()(matrix_type& x) const {
      x.is_data_ = true;
    }

    void bare_type_set_is_data_vis::operator()(row_vector_type& x) const {
      x.is_data_ = true;
    }

    void bare_type_set_is_data_vis::operator()(vector_type& x) const {
      x.is_data_ = true;
    }

    void bare_type_set_is_data_vis::operator()(void_type& x) const {
      // do nothing
    }
  }
}
#endif

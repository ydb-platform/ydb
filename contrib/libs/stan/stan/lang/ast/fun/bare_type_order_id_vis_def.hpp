#ifndef STAN_LANG_AST_FUN_BARE_TYPE_ORDER_ID_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_ORDER_ID_VIS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {
    bare_type_order_id_vis::bare_type_order_id_vis() {}

    template <typename T>
    std::string bare_type_order_id_vis::operator()(const T& x) const {
      return x.oid();
    }

    template std::string
    bare_type_order_id_vis::operator()(const bare_array_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const double_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const ill_formed_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const int_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const matrix_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const row_vector_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const vector_type&) const;

    template std::string
    bare_type_order_id_vis::operator()(const void_type&) const;

  }  // namespace lang
}  // namespace stan
#endif

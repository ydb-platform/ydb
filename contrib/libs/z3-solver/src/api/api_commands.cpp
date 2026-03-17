// Automatically generated file
#include"api/z3.h"
#include"api/z3_replayer.h"
void Z3_replayer_error_handler(Z3_context ctx, Z3_error_code c) { printf("[REPLAYER ERROR HANDLER]: %s\n", Z3_get_error_msg(ctx, c)); }
void exec_Z3_global_param_set(z3_replayer & in) {
  Z3_global_param_set(
    in.get_str(0),
    in.get_str(1));
}
void exec_Z3_global_param_reset_all(z3_replayer & in) {
  Z3_global_param_reset_all(
    );
}
void exec_Z3_global_param_get(z3_replayer & in) {
  Z3_global_param_get(
    in.get_str(0),
    in.get_str_addr(1));
}
void exec_Z3_mk_config(z3_replayer & in) {
  Z3_config result = Z3_mk_config(
    );
  in.store_result(result);
}
void exec_Z3_del_config(z3_replayer & in) {
  Z3_del_config(
    reinterpret_cast<Z3_config>(in.get_obj(0)));
}
void exec_Z3_set_param_value(z3_replayer & in) {
  Z3_set_param_value(
    reinterpret_cast<Z3_config>(in.get_obj(0)),
    in.get_str(1),
    in.get_str(2));
}
void exec_Z3_mk_context(z3_replayer & in) {
  Z3_context result = Z3_mk_context(
    reinterpret_cast<Z3_config>(in.get_obj(0)));
  in.store_result(result);
  Z3_set_error_handler(result, Z3_replayer_error_handler);}
void exec_Z3_mk_context_rc(z3_replayer & in) {
  Z3_context result = Z3_mk_context_rc(
    reinterpret_cast<Z3_config>(in.get_obj(0)));
  in.store_result(result);
  Z3_set_error_handler(result, Z3_replayer_error_handler);}
void exec_Z3_del_context(z3_replayer & in) {
  Z3_del_context(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_inc_ref(z3_replayer & in) {
  Z3_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_dec_ref(z3_replayer & in) {
  Z3_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_update_param_value(z3_replayer & in) {
  Z3_update_param_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    in.get_str(2));
}
void exec_Z3_get_global_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_get_global_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_interrupt(z3_replayer & in) {
  Z3_interrupt(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_enable_concurrent_dec_ref(z3_replayer & in) {
  Z3_enable_concurrent_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_mk_params(z3_replayer & in) {
  Z3_params result = Z3_mk_params(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_params_inc_ref(z3_replayer & in) {
  Z3_params_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)));
}
void exec_Z3_params_dec_ref(z3_replayer & in) {
  Z3_params_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)));
}
void exec_Z3_params_set_bool(z3_replayer & in) {
  Z3_params_set_bool(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)),
    in.get_symbol(2),
    in.get_bool(3));
}
void exec_Z3_params_set_uint(z3_replayer & in) {
  Z3_params_set_uint(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)),
    in.get_symbol(2),
    in.get_uint(3));
}
void exec_Z3_params_set_double(z3_replayer & in) {
  Z3_params_set_double(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)),
    in.get_symbol(2),
    in.get_double(3));
}
void exec_Z3_params_set_symbol(z3_replayer & in) {
  Z3_params_set_symbol(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)),
    in.get_symbol(2),
    in.get_symbol(3));
}
void exec_Z3_params_to_string(z3_replayer & in) {
  Z3_params_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)));
}
void exec_Z3_params_validate(z3_replayer & in) {
  Z3_params_validate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_params>(in.get_obj(1)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(2)));
}
void exec_Z3_param_descrs_inc_ref(z3_replayer & in) {
  Z3_param_descrs_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)));
}
void exec_Z3_param_descrs_dec_ref(z3_replayer & in) {
  Z3_param_descrs_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)));
}
void exec_Z3_param_descrs_get_kind(z3_replayer & in) {
  Z3_param_descrs_get_kind(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)),
    in.get_symbol(2));
}
void exec_Z3_param_descrs_size(z3_replayer & in) {
  Z3_param_descrs_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)));
}
void exec_Z3_param_descrs_get_name(z3_replayer & in) {
  Z3_param_descrs_get_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_param_descrs_get_documentation(z3_replayer & in) {
  Z3_param_descrs_get_documentation(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)),
    in.get_symbol(2));
}
void exec_Z3_param_descrs_to_string(z3_replayer & in) {
  Z3_param_descrs_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_param_descrs>(in.get_obj(1)));
}
void exec_Z3_mk_int_symbol(z3_replayer & in) {
  Z3_mk_int_symbol(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int(1));
}
void exec_Z3_mk_string_symbol(z3_replayer & in) {
  Z3_mk_string_symbol(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
}
void exec_Z3_mk_uninterpreted_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_uninterpreted_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
  in.store_result(result);
}
void exec_Z3_mk_type_variable(z3_replayer & in) {
  Z3_sort result = Z3_mk_type_variable(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
  in.store_result(result);
}
void exec_Z3_mk_bool_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_bool_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_int_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_int_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_real_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_real_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_bv_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_bv_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1));
  in.store_result(result);
}
void exec_Z3_mk_finite_domain_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_finite_domain_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint64(2));
  in.store_result(result);
}
void exec_Z3_mk_array_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_array_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_array_sort_n(z3_replayer & in) {
  Z3_sort result = Z3_mk_array_sort_n(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(2)),
    reinterpret_cast<Z3_sort>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_tuple_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_tuple_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint(2),
    in.get_symbol_array(3),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(4)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(5)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(6)));
  in.store_result(result);
}
void exec_Z3_mk_enumeration_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_enumeration_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint(2),
    in.get_symbol_array(3),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(4)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(5)));
  in.store_result(result);
}
void exec_Z3_mk_list_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_list_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(3)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(4)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(5)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(6)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(7)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(8)));
  in.store_result(result);
}
void exec_Z3_mk_constructor(z3_replayer & in) {
  Z3_constructor result = Z3_mk_constructor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_symbol(2),
    in.get_uint(3),
    in.get_symbol_array(4),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(5)),
    in.get_uint_array(6));
  in.store_result(result);
}
void exec_Z3_constructor_num_fields(z3_replayer & in) {
  Z3_constructor_num_fields(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_constructor>(in.get_obj(1)));
}
void exec_Z3_del_constructor(z3_replayer & in) {
  Z3_del_constructor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_constructor>(in.get_obj(1)));
}
void exec_Z3_mk_datatype(z3_replayer & in) {
  Z3_sort result = Z3_mk_datatype(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint(2),
    reinterpret_cast<Z3_constructor*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_mk_datatype_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_datatype_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
  in.store_result(result);
}
void exec_Z3_mk_constructor_list(z3_replayer & in) {
  Z3_constructor_list result = Z3_mk_constructor_list(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_constructor*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_del_constructor_list(z3_replayer & in) {
  Z3_del_constructor_list(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_constructor_list>(in.get_obj(1)));
}
void exec_Z3_mk_datatypes(z3_replayer & in) {
  Z3_mk_datatypes(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_symbol_array(2),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_constructor_list*>(in.get_obj_array(4)));
}
void exec_Z3_query_constructor(z3_replayer & in) {
  Z3_query_constructor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_constructor>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(3)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_addr(4)),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(5)));
}
void exec_Z3_mk_func_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint(2),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_app(z3_replayer & in) {
  Z3_ast result = Z3_mk_app(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_mk_const(z3_replayer & in) {
  Z3_ast result = Z3_mk_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fresh_func_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_fresh_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    in.get_uint(2),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_fresh_const(z3_replayer & in) {
  Z3_ast result = Z3_mk_fresh_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_rec_func_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_rec_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint(2),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_add_rec_def(z3_replayer & in) {
  Z3_add_rec_def(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
}
void exec_Z3_mk_true(z3_replayer & in) {
  Z3_ast result = Z3_mk_true(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_false(z3_replayer & in) {
  Z3_ast result = Z3_mk_false(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_eq(z3_replayer & in) {
  Z3_ast result = Z3_mk_eq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_distinct(z3_replayer & in) {
  Z3_ast result = Z3_mk_distinct(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_not(z3_replayer & in) {
  Z3_ast result = Z3_mk_not(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_ite(z3_replayer & in) {
  Z3_ast result = Z3_mk_ite(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_iff(z3_replayer & in) {
  Z3_ast result = Z3_mk_iff(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_implies(z3_replayer & in) {
  Z3_ast result = Z3_mk_implies(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_xor(z3_replayer & in) {
  Z3_ast result = Z3_mk_xor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_and(z3_replayer & in) {
  Z3_ast result = Z3_mk_and(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_or(z3_replayer & in) {
  Z3_ast result = Z3_mk_or(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_add(z3_replayer & in) {
  Z3_ast result = Z3_mk_add(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_mul(z3_replayer & in) {
  Z3_ast result = Z3_mk_mul(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_sub(z3_replayer & in) {
  Z3_ast result = Z3_mk_sub(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_unary_minus(z3_replayer & in) {
  Z3_ast result = Z3_mk_unary_minus(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_div(z3_replayer & in) {
  Z3_ast result = Z3_mk_div(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_mod(z3_replayer & in) {
  Z3_ast result = Z3_mk_mod(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_rem(z3_replayer & in) {
  Z3_ast result = Z3_mk_rem(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_power(z3_replayer & in) {
  Z3_ast result = Z3_mk_power(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_abs(z3_replayer & in) {
  Z3_ast result = Z3_mk_abs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_lt(z3_replayer & in) {
  Z3_ast result = Z3_mk_lt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_le(z3_replayer & in) {
  Z3_ast result = Z3_mk_le(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_gt(z3_replayer & in) {
  Z3_ast result = Z3_mk_gt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_ge(z3_replayer & in) {
  Z3_ast result = Z3_mk_ge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_divides(z3_replayer & in) {
  Z3_ast result = Z3_mk_divides(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_int2real(z3_replayer & in) {
  Z3_ast result = Z3_mk_int2real(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_real2int(z3_replayer & in) {
  Z3_ast result = Z3_mk_real2int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_is_int(z3_replayer & in) {
  Z3_ast result = Z3_mk_is_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_bvnot(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvnot(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_bvredand(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvredand(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_bvredor(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvredor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_bvand(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvand(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvor(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvxor(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvxor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvnand(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvnand(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvnor(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvnor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvxnor(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvxnor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvneg(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvneg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_bvadd(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvadd(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsub(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsub(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvmul(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvmul(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvudiv(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvudiv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsdiv(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsdiv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvurem(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvurem(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsrem(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsrem(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsmod(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsmod(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvult(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvult(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvslt(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvslt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvule(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvule(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsle(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsle(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvuge(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvuge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsge(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvugt(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvugt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsgt(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsgt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_concat(z3_replayer & in) {
  Z3_ast result = Z3_mk_concat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_extract(z3_replayer & in) {
  Z3_ast result = Z3_mk_extract(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint(2),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_sign_ext(z3_replayer & in) {
  Z3_ast result = Z3_mk_sign_ext(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_zero_ext(z3_replayer & in) {
  Z3_ast result = Z3_mk_zero_ext(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_repeat(z3_replayer & in) {
  Z3_ast result = Z3_mk_repeat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bit2bool(z3_replayer & in) {
  Z3_ast result = Z3_mk_bit2bool(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvshl(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvshl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvlshr(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvlshr(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvashr(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvashr(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_rotate_left(z3_replayer & in) {
  Z3_ast result = Z3_mk_rotate_left(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_rotate_right(z3_replayer & in) {
  Z3_ast result = Z3_mk_rotate_right(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_ext_rotate_left(z3_replayer & in) {
  Z3_ast result = Z3_mk_ext_rotate_left(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_ext_rotate_right(z3_replayer & in) {
  Z3_ast result = Z3_mk_ext_rotate_right(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_int2bv(z3_replayer & in) {
  Z3_ast result = Z3_mk_int2bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bv2int(z3_replayer & in) {
  Z3_ast result = Z3_mk_bv2int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_bool(2));
  in.store_result(result);
}
void exec_Z3_mk_bvadd_no_overflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvadd_no_overflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_bool(3));
  in.store_result(result);
}
void exec_Z3_mk_bvadd_no_underflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvadd_no_underflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsub_no_overflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsub_no_overflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvsub_no_underflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsub_no_underflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_bool(3));
  in.store_result(result);
}
void exec_Z3_mk_bvsdiv_no_overflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvsdiv_no_overflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bvneg_no_overflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvneg_no_overflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_bvmul_no_overflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvmul_no_overflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_bool(3));
  in.store_result(result);
}
void exec_Z3_mk_bvmul_no_underflow(z3_replayer & in) {
  Z3_ast result = Z3_mk_bvmul_no_underflow(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_select(z3_replayer & in) {
  Z3_ast result = Z3_mk_select(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_select_n(z3_replayer & in) {
  Z3_ast result = Z3_mk_select_n(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_mk_store(z3_replayer & in) {
  Z3_ast result = Z3_mk_store(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_store_n(z3_replayer & in) {
  Z3_ast result = Z3_mk_store_n(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_const_array(z3_replayer & in) {
  Z3_ast result = Z3_mk_const_array(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_map(z3_replayer & in) {
  Z3_ast result = Z3_mk_map(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_mk_array_default(z3_replayer & in) {
  Z3_ast result = Z3_mk_array_default(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_as_array(z3_replayer & in) {
  Z3_ast result = Z3_mk_as_array(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_set_has_size(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_has_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_set_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_empty_set(z3_replayer & in) {
  Z3_ast result = Z3_mk_empty_set(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_full_set(z3_replayer & in) {
  Z3_ast result = Z3_mk_full_set(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_set_add(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_add(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_del(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_del(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_union(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_union(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_intersect(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_intersect(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_difference(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_difference(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_complement(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_complement(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_set_member(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_member(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_set_subset(z3_replayer & in) {
  Z3_ast result = Z3_mk_set_subset(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_array_ext(z3_replayer & in) {
  Z3_ast result = Z3_mk_array_ext(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_numeral(z3_replayer & in) {
  Z3_ast result = Z3_mk_numeral(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_real(z3_replayer & in) {
  Z3_ast result = Z3_mk_real(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int(1),
    in.get_int(2));
  in.store_result(result);
}
void exec_Z3_mk_real_int64(z3_replayer & in) {
  Z3_ast result = Z3_mk_real_int64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int64(1),
    in.get_int64(2));
  in.store_result(result);
}
void exec_Z3_mk_int(z3_replayer & in) {
  Z3_ast result = Z3_mk_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_unsigned_int(z3_replayer & in) {
  Z3_ast result = Z3_mk_unsigned_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_int64(z3_replayer & in) {
  Z3_ast result = Z3_mk_int64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int64(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_unsigned_int64(z3_replayer & in) {
  Z3_ast result = Z3_mk_unsigned_int64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint64(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_bv_numeral(z3_replayer & in) {
  Z3_ast result = Z3_mk_bv_numeral(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_bool_array(2));
  in.store_result(result);
}
void exec_Z3_mk_seq_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_seq_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_is_seq_sort(z3_replayer & in) {
  Z3_is_seq_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_seq_sort_basis(z3_replayer & in) {
  Z3_sort result = Z3_get_seq_sort_basis(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_re_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_is_re_sort(z3_replayer & in) {
  Z3_is_re_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_re_sort_basis(z3_replayer & in) {
  Z3_sort result = Z3_get_re_sort_basis(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_string_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_string_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_char_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_char_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_is_string_sort(z3_replayer & in) {
  Z3_is_string_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_is_char_sort(z3_replayer & in) {
  Z3_is_char_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_mk_string(z3_replayer & in) {
  Z3_ast result = Z3_mk_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
  in.store_result(result);
}
void exec_Z3_mk_lstring(z3_replayer & in) {
  Z3_ast result = Z3_mk_lstring(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_str(2));
  in.store_result(result);
}
void exec_Z3_mk_u32string(z3_replayer & in) {
  Z3_ast result = Z3_mk_u32string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint_array(2));
  in.store_result(result);
}
void exec_Z3_is_string(z3_replayer & in) {
  Z3_is_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_string(z3_replayer & in) {
  Z3_get_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_lstring(z3_replayer & in) {
  Z3_get_lstring(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint_addr(2));
}
void exec_Z3_get_string_length(z3_replayer & in) {
  Z3_get_string_length(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_string_contents(z3_replayer & in) {
  Z3_get_string_contents(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    in.get_uint_array(3));
}
void exec_Z3_mk_seq_empty(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_empty(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_seq_unit(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_unit(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_seq_concat(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_concat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_prefix(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_prefix(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_suffix(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_suffix(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_contains(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_contains(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_str_lt(z3_replayer & in) {
  Z3_ast result = Z3_mk_str_lt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_str_le(z3_replayer & in) {
  Z3_ast result = Z3_mk_str_le(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_extract(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_extract(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_seq_replace(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_replace(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_seq_at(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_at(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_nth(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_nth(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_length(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_length(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_seq_index(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_index(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_seq_last_index(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_last_index(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_map(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_map(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_seq_mapi(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_mapi(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_seq_foldl(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_foldl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_seq_foldli(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_foldli(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_str_to_int(z3_replayer & in) {
  Z3_ast result = Z3_mk_str_to_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_int_to_str(z3_replayer & in) {
  Z3_ast result = Z3_mk_int_to_str(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_string_to_code(z3_replayer & in) {
  Z3_ast result = Z3_mk_string_to_code(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_string_from_code(z3_replayer & in) {
  Z3_ast result = Z3_mk_string_from_code(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_ubv_to_str(z3_replayer & in) {
  Z3_ast result = Z3_mk_ubv_to_str(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_sbv_to_str(z3_replayer & in) {
  Z3_ast result = Z3_mk_sbv_to_str(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_seq_to_re(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_to_re(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_seq_in_re(z3_replayer & in) {
  Z3_ast result = Z3_mk_seq_in_re(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_re_plus(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_plus(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_star(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_star(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_option(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_option(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_union(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_union(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_re_concat(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_concat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_re_range(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_range(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_re_allchar(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_allchar(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_loop(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_loop(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_mk_re_power(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_power(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_re_intersect(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_intersect(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_re_complement(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_complement(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_diff(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_diff(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_re_empty(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_empty(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_re_full(z3_replayer & in) {
  Z3_ast result = Z3_mk_re_full(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_char(z3_replayer & in) {
  Z3_ast result = Z3_mk_char(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1));
  in.store_result(result);
}
void exec_Z3_mk_char_le(z3_replayer & in) {
  Z3_ast result = Z3_mk_char_le(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_char_to_int(z3_replayer & in) {
  Z3_ast result = Z3_mk_char_to_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_char_to_bv(z3_replayer & in) {
  Z3_ast result = Z3_mk_char_to_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_char_from_bv(z3_replayer & in) {
  Z3_ast result = Z3_mk_char_from_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_char_is_digit(z3_replayer & in) {
  Z3_ast result = Z3_mk_char_is_digit(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_linear_order(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_linear_order(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_partial_order(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_partial_order(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_piecewise_linear_order(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_piecewise_linear_order(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_tree_order(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_tree_order(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_transitive_closure(z3_replayer & in) {
  Z3_func_decl result = Z3_mk_transitive_closure(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_pattern(z3_replayer & in) {
  Z3_pattern result = Z3_mk_pattern(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_mk_bound(z3_replayer & in) {
  Z3_ast result = Z3_mk_bound(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_forall(z3_replayer & in) {
  Z3_ast result = Z3_mk_forall(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint(2),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(3)),
    in.get_uint(4),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(5)),
    in.get_symbol_array(6),
    reinterpret_cast<Z3_ast>(in.get_obj(7)));
  in.store_result(result);
}
void exec_Z3_mk_exists(z3_replayer & in) {
  Z3_ast result = Z3_mk_exists(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint(2),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(3)),
    in.get_uint(4),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(5)),
    in.get_symbol_array(6),
    reinterpret_cast<Z3_ast>(in.get_obj(7)));
  in.store_result(result);
}
void exec_Z3_mk_quantifier(z3_replayer & in) {
  Z3_ast result = Z3_mk_quantifier(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_uint(2),
    in.get_uint(3),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(4)),
    in.get_uint(5),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(6)),
    in.get_symbol_array(7),
    reinterpret_cast<Z3_ast>(in.get_obj(8)));
  in.store_result(result);
}
void exec_Z3_mk_quantifier_ex(z3_replayer & in) {
  Z3_ast result = Z3_mk_quantifier_ex(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_uint(2),
    in.get_symbol(3),
    in.get_symbol(4),
    in.get_uint(5),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(6)),
    in.get_uint(7),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(8)),
    in.get_uint(9),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(10)),
    in.get_symbol_array(11),
    reinterpret_cast<Z3_ast>(in.get_obj(12)));
  in.store_result(result);
}
void exec_Z3_mk_forall_const(z3_replayer & in) {
  Z3_ast result = Z3_mk_forall_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint(2),
    reinterpret_cast<Z3_app*>(in.get_obj_array(3)),
    in.get_uint(4),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(5)),
    reinterpret_cast<Z3_ast>(in.get_obj(6)));
  in.store_result(result);
}
void exec_Z3_mk_exists_const(z3_replayer & in) {
  Z3_ast result = Z3_mk_exists_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint(2),
    reinterpret_cast<Z3_app*>(in.get_obj_array(3)),
    in.get_uint(4),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(5)),
    reinterpret_cast<Z3_ast>(in.get_obj(6)));
  in.store_result(result);
}
void exec_Z3_mk_quantifier_const(z3_replayer & in) {
  Z3_ast result = Z3_mk_quantifier_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_uint(2),
    in.get_uint(3),
    reinterpret_cast<Z3_app*>(in.get_obj_array(4)),
    in.get_uint(5),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(6)),
    reinterpret_cast<Z3_ast>(in.get_obj(7)));
  in.store_result(result);
}
void exec_Z3_mk_quantifier_const_ex(z3_replayer & in) {
  Z3_ast result = Z3_mk_quantifier_const_ex(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_uint(2),
    in.get_symbol(3),
    in.get_symbol(4),
    in.get_uint(5),
    reinterpret_cast<Z3_app*>(in.get_obj_array(6)),
    in.get_uint(7),
    reinterpret_cast<Z3_pattern*>(in.get_obj_array(8)),
    in.get_uint(9),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(10)),
    reinterpret_cast<Z3_ast>(in.get_obj(11)));
  in.store_result(result);
}
void exec_Z3_mk_lambda(z3_replayer & in) {
  Z3_ast result = Z3_mk_lambda(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(2)),
    in.get_symbol_array(3),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_lambda_const(z3_replayer & in) {
  Z3_ast result = Z3_mk_lambda_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_app*>(in.get_obj_array(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_get_symbol_kind(z3_replayer & in) {
  Z3_get_symbol_kind(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
}
void exec_Z3_get_symbol_int(z3_replayer & in) {
  Z3_get_symbol_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
}
void exec_Z3_get_symbol_string(z3_replayer & in) {
  Z3_get_symbol_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
}
void exec_Z3_get_sort_name(z3_replayer & in) {
  Z3_get_sort_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_sort_id(z3_replayer & in) {
  Z3_get_sort_id(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_sort_to_ast(z3_replayer & in) {
  Z3_ast result = Z3_sort_to_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_is_eq_sort(z3_replayer & in) {
  Z3_is_eq_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
}
void exec_Z3_get_sort_kind(z3_replayer & in) {
  Z3_get_sort_kind(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_bv_sort_size(z3_replayer & in) {
  Z3_get_bv_sort_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_finite_domain_sort_size(z3_replayer & in) {
  Z3_get_finite_domain_sort_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint64_addr(2));
}
void exec_Z3_get_array_arity(z3_replayer & in) {
  Z3_get_array_arity(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_array_sort_domain(z3_replayer & in) {
  Z3_sort result = Z3_get_array_sort_domain(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_array_sort_domain_n(z3_replayer & in) {
  Z3_sort result = Z3_get_array_sort_domain_n(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_array_sort_range(z3_replayer & in) {
  Z3_sort result = Z3_get_array_sort_range(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_tuple_sort_mk_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_get_tuple_sort_mk_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_tuple_sort_num_fields(z3_replayer & in) {
  Z3_get_tuple_sort_num_fields(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_tuple_sort_field_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_get_tuple_sort_field_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_is_recursive_datatype_sort(z3_replayer & in) {
  Z3_is_recursive_datatype_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_datatype_sort_num_constructors(z3_replayer & in) {
  Z3_get_datatype_sort_num_constructors(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_datatype_sort_constructor(z3_replayer & in) {
  Z3_func_decl result = Z3_get_datatype_sort_constructor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_datatype_sort_recognizer(z3_replayer & in) {
  Z3_func_decl result = Z3_get_datatype_sort_recognizer(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_datatype_sort_constructor_accessor(z3_replayer & in) {
  Z3_func_decl result = Z3_get_datatype_sort_constructor_accessor(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_datatype_update_field(z3_replayer & in) {
  Z3_ast result = Z3_datatype_update_field(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_get_relation_arity(z3_replayer & in) {
  Z3_get_relation_arity(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_get_relation_column(z3_replayer & in) {
  Z3_sort result = Z3_get_relation_column(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_atmost(z3_replayer & in) {
  Z3_ast result = Z3_mk_atmost(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_mk_atleast(z3_replayer & in) {
  Z3_ast result = Z3_mk_atleast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_mk_pble(z3_replayer & in) {
  Z3_ast result = Z3_mk_pble(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)),
    in.get_int_array(3),
    in.get_int(4));
  in.store_result(result);
}
void exec_Z3_mk_pbge(z3_replayer & in) {
  Z3_ast result = Z3_mk_pbge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)),
    in.get_int_array(3),
    in.get_int(4));
  in.store_result(result);
}
void exec_Z3_mk_pbeq(z3_replayer & in) {
  Z3_ast result = Z3_mk_pbeq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(2)),
    in.get_int_array(3),
    in.get_int(4));
  in.store_result(result);
}
void exec_Z3_func_decl_to_ast(z3_replayer & in) {
  Z3_ast result = Z3_func_decl_to_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_is_eq_func_decl(z3_replayer & in) {
  Z3_is_eq_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
}
void exec_Z3_get_func_decl_id(z3_replayer & in) {
  Z3_get_func_decl_id(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_get_decl_name(z3_replayer & in) {
  Z3_get_decl_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_get_decl_kind(z3_replayer & in) {
  Z3_get_decl_kind(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_get_domain_size(z3_replayer & in) {
  Z3_get_domain_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_get_arity(z3_replayer & in) {
  Z3_get_arity(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_get_domain(z3_replayer & in) {
  Z3_sort result = Z3_get_domain(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_range(z3_replayer & in) {
  Z3_sort result = Z3_get_range(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_decl_num_parameters(z3_replayer & in) {
  Z3_get_decl_num_parameters(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_get_decl_parameter_kind(z3_replayer & in) {
  Z3_get_decl_parameter_kind(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_decl_int_parameter(z3_replayer & in) {
  Z3_get_decl_int_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_decl_double_parameter(z3_replayer & in) {
  Z3_get_decl_double_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_decl_symbol_parameter(z3_replayer & in) {
  Z3_get_decl_symbol_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_decl_sort_parameter(z3_replayer & in) {
  Z3_sort result = Z3_get_decl_sort_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_decl_ast_parameter(z3_replayer & in) {
  Z3_ast result = Z3_get_decl_ast_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_decl_func_decl_parameter(z3_replayer & in) {
  Z3_func_decl result = Z3_get_decl_func_decl_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_decl_rational_parameter(z3_replayer & in) {
  Z3_get_decl_rational_parameter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_app_to_ast(z3_replayer & in) {
  Z3_ast result = Z3_app_to_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_app>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_app_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_get_app_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_app>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_app_num_args(z3_replayer & in) {
  Z3_get_app_num_args(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_app>(in.get_obj(1)));
}
void exec_Z3_get_app_arg(z3_replayer & in) {
  Z3_ast result = Z3_get_app_arg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_app>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_is_eq_ast(z3_replayer & in) {
  Z3_is_eq_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_get_ast_id(z3_replayer & in) {
  Z3_get_ast_id(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_ast_hash(z3_replayer & in) {
  Z3_get_ast_hash(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_sort(z3_replayer & in) {
  Z3_sort result = Z3_get_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_is_well_sorted(z3_replayer & in) {
  Z3_is_well_sorted(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_bool_value(z3_replayer & in) {
  Z3_get_bool_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_ast_kind(z3_replayer & in) {
  Z3_get_ast_kind(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_app(z3_replayer & in) {
  Z3_is_app(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_ground(z3_replayer & in) {
  Z3_is_ground(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_depth(z3_replayer & in) {
  Z3_get_depth(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_numeral_ast(z3_replayer & in) {
  Z3_is_numeral_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_algebraic_number(z3_replayer & in) {
  Z3_is_algebraic_number(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_to_app(z3_replayer & in) {
  Z3_app result = Z3_to_app(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_to_func_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_to_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_numeral_string(z3_replayer & in) {
  Z3_get_numeral_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_numeral_binary_string(z3_replayer & in) {
  Z3_get_numeral_binary_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_numeral_decimal_string(z3_replayer & in) {
  Z3_get_numeral_decimal_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_numeral_double(z3_replayer & in) {
  Z3_get_numeral_double(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_numerator(z3_replayer & in) {
  Z3_ast result = Z3_get_numerator(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_denominator(z3_replayer & in) {
  Z3_ast result = Z3_get_denominator(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_numeral_small(z3_replayer & in) {
  Z3_get_numeral_small(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_int64_addr(2),
    in.get_int64_addr(3));
}
void exec_Z3_get_numeral_int(z3_replayer & in) {
  Z3_get_numeral_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_int_addr(2));
}
void exec_Z3_get_numeral_uint(z3_replayer & in) {
  Z3_get_numeral_uint(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint_addr(2));
}
void exec_Z3_get_numeral_uint64(z3_replayer & in) {
  Z3_get_numeral_uint64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint64_addr(2));
}
void exec_Z3_get_numeral_int64(z3_replayer & in) {
  Z3_get_numeral_int64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_int64_addr(2));
}
void exec_Z3_get_numeral_rational_int64(z3_replayer & in) {
  Z3_get_numeral_rational_int64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_int64_addr(2),
    in.get_int64_addr(3));
}
void exec_Z3_get_algebraic_number_lower(z3_replayer & in) {
  Z3_ast result = Z3_get_algebraic_number_lower(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_algebraic_number_upper(z3_replayer & in) {
  Z3_ast result = Z3_get_algebraic_number_upper(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_pattern_to_ast(z3_replayer & in) {
  Z3_ast result = Z3_pattern_to_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_pattern>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_pattern_num_terms(z3_replayer & in) {
  Z3_get_pattern_num_terms(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_pattern>(in.get_obj(1)));
}
void exec_Z3_get_pattern(z3_replayer & in) {
  Z3_ast result = Z3_get_pattern(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_pattern>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_index_value(z3_replayer & in) {
  Z3_get_index_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_quantifier_forall(z3_replayer & in) {
  Z3_is_quantifier_forall(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_quantifier_exists(z3_replayer & in) {
  Z3_is_quantifier_exists(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_is_lambda(z3_replayer & in) {
  Z3_is_lambda(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_weight(z3_replayer & in) {
  Z3_get_quantifier_weight(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_skolem_id(z3_replayer & in) {
  Z3_get_quantifier_skolem_id(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_id(z3_replayer & in) {
  Z3_get_quantifier_id(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_num_patterns(z3_replayer & in) {
  Z3_get_quantifier_num_patterns(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_pattern_ast(z3_replayer & in) {
  Z3_pattern result = Z3_get_quantifier_pattern_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_quantifier_num_no_patterns(z3_replayer & in) {
  Z3_get_quantifier_num_no_patterns(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_no_pattern_ast(z3_replayer & in) {
  Z3_ast result = Z3_get_quantifier_no_pattern_ast(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_quantifier_num_bound(z3_replayer & in) {
  Z3_get_quantifier_num_bound(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_quantifier_bound_name(z3_replayer & in) {
  Z3_get_quantifier_bound_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_quantifier_bound_sort(z3_replayer & in) {
  Z3_sort result = Z3_get_quantifier_bound_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_get_quantifier_body(z3_replayer & in) {
  Z3_ast result = Z3_get_quantifier_body(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_simplify(z3_replayer & in) {
  Z3_ast result = Z3_simplify(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_simplify_ex(z3_replayer & in) {
  Z3_ast result = Z3_simplify_ex(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_params>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_simplify_get_help(z3_replayer & in) {
  Z3_simplify_get_help(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_simplify_get_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_simplify_get_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_update_term(z3_replayer & in) {
  Z3_ast result = Z3_update_term(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_substitute(z3_replayer & in) {
  Z3_ast result = Z3_substitute(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(4)));
  in.store_result(result);
}
void exec_Z3_substitute_vars(z3_replayer & in) {
  Z3_ast result = Z3_substitute_vars(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_substitute_funs(z3_replayer & in) {
  Z3_ast result = Z3_substitute_funs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(4)));
  in.store_result(result);
}
void exec_Z3_translate(z3_replayer & in) {
  Z3_ast result = Z3_translate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_context>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_model(z3_replayer & in) {
  Z3_model result = Z3_mk_model(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_model_inc_ref(z3_replayer & in) {
  Z3_model_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)));
}
void exec_Z3_model_dec_ref(z3_replayer & in) {
  Z3_model_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)));
}
void exec_Z3_model_eval(z3_replayer & in) {
  Z3_model_eval(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_bool(3),
    reinterpret_cast<Z3_ast*>(in.get_obj_addr(4)));
}
void exec_Z3_model_get_const_interp(z3_replayer & in) {
  Z3_ast result = Z3_model_get_const_interp(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_model_has_interp(z3_replayer & in) {
  Z3_model_has_interp(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
}
void exec_Z3_model_get_func_interp(z3_replayer & in) {
  Z3_func_interp result = Z3_model_get_func_interp(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_model_get_num_consts(z3_replayer & in) {
  Z3_model_get_num_consts(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)));
}
void exec_Z3_model_get_const_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_model_get_const_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_model_get_num_funcs(z3_replayer & in) {
  Z3_model_get_num_funcs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)));
}
void exec_Z3_model_get_func_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_model_get_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_model_get_num_sorts(z3_replayer & in) {
  Z3_model_get_num_sorts(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)));
}
void exec_Z3_model_get_sort(z3_replayer & in) {
  Z3_sort result = Z3_model_get_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_model_get_sort_universe(z3_replayer & in) {
  Z3_ast_vector result = Z3_model_get_sort_universe(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_model_translate(z3_replayer & in) {
  Z3_model result = Z3_model_translate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_context>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_is_as_array(z3_replayer & in) {
  Z3_is_as_array(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_get_as_array_func_decl(z3_replayer & in) {
  Z3_func_decl result = Z3_get_as_array_func_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_add_func_interp(z3_replayer & in) {
  Z3_func_interp result = Z3_add_func_interp(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_add_const_interp(z3_replayer & in) {
  Z3_add_const_interp(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_func_interp_inc_ref(z3_replayer & in) {
  Z3_func_interp_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)));
}
void exec_Z3_func_interp_dec_ref(z3_replayer & in) {
  Z3_func_interp_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)));
}
void exec_Z3_func_interp_get_num_entries(z3_replayer & in) {
  Z3_func_interp_get_num_entries(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)));
}
void exec_Z3_func_interp_get_entry(z3_replayer & in) {
  Z3_func_entry result = Z3_func_interp_get_entry(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_func_interp_get_else(z3_replayer & in) {
  Z3_ast result = Z3_func_interp_get_else(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_func_interp_set_else(z3_replayer & in) {
  Z3_func_interp_set_else(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_func_interp_get_arity(z3_replayer & in) {
  Z3_func_interp_get_arity(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)));
}
void exec_Z3_func_interp_add_entry(z3_replayer & in) {
  Z3_func_interp_add_entry(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_interp>(in.get_obj(1)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_func_entry_inc_ref(z3_replayer & in) {
  Z3_func_entry_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_entry>(in.get_obj(1)));
}
void exec_Z3_func_entry_dec_ref(z3_replayer & in) {
  Z3_func_entry_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_entry>(in.get_obj(1)));
}
void exec_Z3_func_entry_get_value(z3_replayer & in) {
  Z3_ast result = Z3_func_entry_get_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_entry>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_func_entry_get_num_args(z3_replayer & in) {
  Z3_func_entry_get_num_args(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_entry>(in.get_obj(1)));
}
void exec_Z3_func_entry_get_arg(z3_replayer & in) {
  Z3_ast result = Z3_func_entry_get_arg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_entry>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_toggle_warning_messages(z3_replayer & in) {
  Z3_toggle_warning_messages(
    in.get_bool(0));
}
void exec_Z3_set_ast_print_mode(z3_replayer & in) {
  Z3_set_ast_print_mode(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    static_cast<Z3_ast_print_mode>(in.get_uint(1)));
}
void exec_Z3_ast_to_string(z3_replayer & in) {
  Z3_ast_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_pattern_to_string(z3_replayer & in) {
  Z3_pattern_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_pattern>(in.get_obj(1)));
}
void exec_Z3_sort_to_string(z3_replayer & in) {
  Z3_sort_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_func_decl_to_string(z3_replayer & in) {
  Z3_func_decl_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(1)));
}
void exec_Z3_model_to_string(z3_replayer & in) {
  Z3_model_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)));
}
void exec_Z3_benchmark_to_smtlib_string(z3_replayer & in) {
  Z3_benchmark_to_smtlib_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    in.get_str(2),
    in.get_str(3),
    in.get_str(4),
    in.get_uint(5),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(6)),
    reinterpret_cast<Z3_ast>(in.get_obj(7)));
}
void exec_Z3_parse_smtlib2_string(z3_replayer & in) {
  Z3_ast_vector result = Z3_parse_smtlib2_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    in.get_uint(2),
    in.get_symbol_array(3),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(4)),
    in.get_uint(5),
    in.get_symbol_array(6),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(7)));
  in.store_result(result);
}
void exec_Z3_parse_smtlib2_file(z3_replayer & in) {
  Z3_ast_vector result = Z3_parse_smtlib2_file(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1),
    in.get_uint(2),
    in.get_symbol_array(3),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(4)),
    in.get_uint(5),
    in.get_symbol_array(6),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(7)));
  in.store_result(result);
}
void exec_Z3_eval_smtlib2_string(z3_replayer & in) {
  Z3_eval_smtlib2_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
}
void exec_Z3_mk_parser_context(z3_replayer & in) {
  Z3_parser_context result = Z3_mk_parser_context(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_parser_context_inc_ref(z3_replayer & in) {
  Z3_parser_context_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_parser_context>(in.get_obj(1)));
}
void exec_Z3_parser_context_dec_ref(z3_replayer & in) {
  Z3_parser_context_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_parser_context>(in.get_obj(1)));
}
void exec_Z3_parser_context_add_sort(z3_replayer & in) {
  Z3_parser_context_add_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_parser_context>(in.get_obj(1)),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
}
void exec_Z3_parser_context_add_decl(z3_replayer & in) {
  Z3_parser_context_add_decl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_parser_context>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
}
void exec_Z3_parser_context_from_string(z3_replayer & in) {
  Z3_ast_vector result = Z3_parser_context_from_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_parser_context>(in.get_obj(1)),
    in.get_str(2));
  in.store_result(result);
}
void exec_Z3_get_error_code(z3_replayer & in) {
  Z3_get_error_code(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_set_error(z3_replayer & in) {
  Z3_set_error(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    static_cast<Z3_error_code>(in.get_uint(1)));
}
void exec_Z3_get_error_msg(z3_replayer & in) {
  Z3_get_error_msg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    static_cast<Z3_error_code>(in.get_uint(1)));
}
void exec_Z3_get_version(z3_replayer & in) {
  Z3_get_version(
    in.get_uint_addr(0),
    in.get_uint_addr(1),
    in.get_uint_addr(2),
    in.get_uint_addr(3));
}
void exec_Z3_get_full_version(z3_replayer & in) {
  Z3_get_full_version(
    );
}
void exec_Z3_enable_trace(z3_replayer & in) {
  Z3_enable_trace(
    in.get_str(0));
}
void exec_Z3_disable_trace(z3_replayer & in) {
  Z3_disable_trace(
    in.get_str(0));
}
void exec_Z3_reset_memory(z3_replayer & in) {
  Z3_reset_memory(
    );
}
void exec_Z3_finalize_memory(z3_replayer & in) {
  Z3_finalize_memory(
    );
}
void exec_Z3_mk_goal(z3_replayer & in) {
  Z3_goal result = Z3_mk_goal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_bool(2),
    in.get_bool(3));
  in.store_result(result);
}
void exec_Z3_goal_inc_ref(z3_replayer & in) {
  Z3_goal_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_dec_ref(z3_replayer & in) {
  Z3_goal_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_precision(z3_replayer & in) {
  Z3_goal_precision(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_assert(z3_replayer & in) {
  Z3_goal_assert(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_goal_inconsistent(z3_replayer & in) {
  Z3_goal_inconsistent(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_depth(z3_replayer & in) {
  Z3_goal_depth(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_reset(z3_replayer & in) {
  Z3_goal_reset(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_size(z3_replayer & in) {
  Z3_goal_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_formula(z3_replayer & in) {
  Z3_ast result = Z3_goal_formula(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_goal_num_exprs(z3_replayer & in) {
  Z3_goal_num_exprs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_is_decided_sat(z3_replayer & in) {
  Z3_goal_is_decided_sat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_is_decided_unsat(z3_replayer & in) {
  Z3_goal_is_decided_unsat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_translate(z3_replayer & in) {
  Z3_goal result = Z3_goal_translate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)),
    reinterpret_cast<Z3_context>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_goal_convert_model(z3_replayer & in) {
  Z3_model result = Z3_goal_convert_model(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)),
    reinterpret_cast<Z3_model>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_goal_to_string(z3_replayer & in) {
  Z3_goal_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)));
}
void exec_Z3_goal_to_dimacs_string(z3_replayer & in) {
  Z3_goal_to_dimacs_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_goal>(in.get_obj(1)),
    in.get_bool(2));
}
void exec_Z3_mk_tactic(z3_replayer & in) {
  Z3_tactic result = Z3_mk_tactic(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
  in.store_result(result);
}
void exec_Z3_tactic_inc_ref(z3_replayer & in) {
  Z3_tactic_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)));
}
void exec_Z3_tactic_dec_ref(z3_replayer & in) {
  Z3_tactic_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)));
}
void exec_Z3_mk_probe(z3_replayer & in) {
  Z3_probe result = Z3_mk_probe(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
  in.store_result(result);
}
void exec_Z3_probe_inc_ref(z3_replayer & in) {
  Z3_probe_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)));
}
void exec_Z3_probe_dec_ref(z3_replayer & in) {
  Z3_probe_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)));
}
void exec_Z3_tactic_and_then(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_and_then(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    reinterpret_cast<Z3_tactic>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_tactic_or_else(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_or_else(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    reinterpret_cast<Z3_tactic>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_tactic_par_or(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_par_or(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_tactic*>(in.get_obj_array(2)));
  in.store_result(result);
}
void exec_Z3_tactic_par_and_then(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_par_and_then(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    reinterpret_cast<Z3_tactic>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_tactic_try_for(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_try_for(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_tactic_when(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_when(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_tactic>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_tactic_cond(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_cond(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_tactic>(in.get_obj(2)),
    reinterpret_cast<Z3_tactic>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_tactic_repeat(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_repeat(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_tactic_skip(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_skip(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_tactic_fail(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_fail(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_tactic_fail_if(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_fail_if(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_tactic_fail_if_not_decided(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_fail_if_not_decided(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_tactic_using_params(z3_replayer & in) {
  Z3_tactic result = Z3_tactic_using_params(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    reinterpret_cast<Z3_params>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_simplifier(z3_replayer & in) {
  Z3_simplifier result = Z3_mk_simplifier(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
  in.store_result(result);
}
void exec_Z3_simplifier_inc_ref(z3_replayer & in) {
  Z3_simplifier_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(1)));
}
void exec_Z3_simplifier_dec_ref(z3_replayer & in) {
  Z3_simplifier_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(1)));
}
void exec_Z3_solver_add_simplifier(z3_replayer & in) {
  Z3_solver result = Z3_solver_add_simplifier(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_simplifier_and_then(z3_replayer & in) {
  Z3_simplifier result = Z3_simplifier_and_then(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(1)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_simplifier_using_params(z3_replayer & in) {
  Z3_simplifier result = Z3_simplifier_using_params(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(1)),
    reinterpret_cast<Z3_params>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_get_num_simplifiers(z3_replayer & in) {
  Z3_get_num_simplifiers(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_get_simplifier_name(z3_replayer & in) {
  Z3_get_simplifier_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1));
}
void exec_Z3_simplifier_get_help(z3_replayer & in) {
  Z3_simplifier_get_help(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(1)));
}
void exec_Z3_simplifier_get_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_simplifier_get_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_simplifier>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_simplifier_get_descr(z3_replayer & in) {
  Z3_simplifier_get_descr(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
}
void exec_Z3_probe_const(z3_replayer & in) {
  Z3_probe result = Z3_probe_const(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_double(1));
  in.store_result(result);
}
void exec_Z3_probe_lt(z3_replayer & in) {
  Z3_probe result = Z3_probe_lt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_gt(z3_replayer & in) {
  Z3_probe result = Z3_probe_gt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_le(z3_replayer & in) {
  Z3_probe result = Z3_probe_le(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_ge(z3_replayer & in) {
  Z3_probe result = Z3_probe_ge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_eq(z3_replayer & in) {
  Z3_probe result = Z3_probe_eq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_and(z3_replayer & in) {
  Z3_probe result = Z3_probe_and(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_or(z3_replayer & in) {
  Z3_probe result = Z3_probe_or(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_probe>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_probe_not(z3_replayer & in) {
  Z3_probe result = Z3_probe_not(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_get_num_tactics(z3_replayer & in) {
  Z3_get_num_tactics(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_get_tactic_name(z3_replayer & in) {
  Z3_get_tactic_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1));
}
void exec_Z3_get_num_probes(z3_replayer & in) {
  Z3_get_num_probes(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
}
void exec_Z3_get_probe_name(z3_replayer & in) {
  Z3_get_probe_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1));
}
void exec_Z3_tactic_get_help(z3_replayer & in) {
  Z3_tactic_get_help(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)));
}
void exec_Z3_tactic_get_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_tactic_get_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_tactic_get_descr(z3_replayer & in) {
  Z3_tactic_get_descr(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
}
void exec_Z3_probe_get_descr(z3_replayer & in) {
  Z3_probe_get_descr(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
}
void exec_Z3_probe_apply(z3_replayer & in) {
  Z3_probe_apply(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_probe>(in.get_obj(1)),
    reinterpret_cast<Z3_goal>(in.get_obj(2)));
}
void exec_Z3_tactic_apply(z3_replayer & in) {
  Z3_apply_result result = Z3_tactic_apply(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    reinterpret_cast<Z3_goal>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_tactic_apply_ex(z3_replayer & in) {
  Z3_apply_result result = Z3_tactic_apply_ex(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)),
    reinterpret_cast<Z3_goal>(in.get_obj(2)),
    reinterpret_cast<Z3_params>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_apply_result_inc_ref(z3_replayer & in) {
  Z3_apply_result_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_apply_result>(in.get_obj(1)));
}
void exec_Z3_apply_result_dec_ref(z3_replayer & in) {
  Z3_apply_result_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_apply_result>(in.get_obj(1)));
}
void exec_Z3_apply_result_to_string(z3_replayer & in) {
  Z3_apply_result_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_apply_result>(in.get_obj(1)));
}
void exec_Z3_apply_result_get_num_subgoals(z3_replayer & in) {
  Z3_apply_result_get_num_subgoals(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_apply_result>(in.get_obj(1)));
}
void exec_Z3_apply_result_get_subgoal(z3_replayer & in) {
  Z3_goal result = Z3_apply_result_get_subgoal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_apply_result>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_solver(z3_replayer & in) {
  Z3_solver result = Z3_mk_solver(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_simple_solver(z3_replayer & in) {
  Z3_solver result = Z3_mk_simple_solver(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_solver_for_logic(z3_replayer & in) {
  Z3_solver result = Z3_mk_solver_for_logic(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1));
  in.store_result(result);
}
void exec_Z3_mk_solver_from_tactic(z3_replayer & in) {
  Z3_solver result = Z3_mk_solver_from_tactic(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_tactic>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_translate(z3_replayer & in) {
  Z3_solver result = Z3_solver_translate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_context>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_solver_import_model_converter(z3_replayer & in) {
  Z3_solver_import_model_converter(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_solver>(in.get_obj(2)));
}
void exec_Z3_solver_get_help(z3_replayer & in) {
  Z3_solver_get_help(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_get_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_solver_get_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_set_params(z3_replayer & in) {
  Z3_solver_set_params(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_params>(in.get_obj(2)));
}
void exec_Z3_solver_inc_ref(z3_replayer & in) {
  Z3_solver_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_dec_ref(z3_replayer & in) {
  Z3_solver_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_interrupt(z3_replayer & in) {
  Z3_solver_interrupt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_push(z3_replayer & in) {
  Z3_solver_push(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_pop(z3_replayer & in) {
  Z3_solver_pop(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_solver_reset(z3_replayer & in) {
  Z3_solver_reset(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_get_num_scopes(z3_replayer & in) {
  Z3_solver_get_num_scopes(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_assert(z3_replayer & in) {
  Z3_solver_assert(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_solver_assert_and_track(z3_replayer & in) {
  Z3_solver_assert_and_track(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_solver_from_file(z3_replayer & in) {
  Z3_solver_from_file(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_str(2));
}
void exec_Z3_solver_from_string(z3_replayer & in) {
  Z3_solver_from_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_str(2));
}
void exec_Z3_solver_get_assertions(z3_replayer & in) {
  Z3_ast_vector result = Z3_solver_get_assertions(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_units(z3_replayer & in) {
  Z3_ast_vector result = Z3_solver_get_units(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_trail(z3_replayer & in) {
  Z3_ast_vector result = Z3_solver_get_trail(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_non_units(z3_replayer & in) {
  Z3_ast_vector result = Z3_solver_get_non_units(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_levels(z3_replayer & in) {
  Z3_solver_get_levels(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(2)),
    in.get_uint(3),
    in.get_uint_array(4));
}
void exec_Z3_solver_congruence_root(z3_replayer & in) {
  Z3_ast result = Z3_solver_congruence_root(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_solver_congruence_next(z3_replayer & in) {
  Z3_ast result = Z3_solver_congruence_next(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_solver_congruence_explain(z3_replayer & in) {
  Z3_ast result = Z3_solver_congruence_explain(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_solver_solve_for(z3_replayer & in) {
  Z3_solver_solve_for(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(2)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(3)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(4)));
}
void exec_Z3_solver_register_on_clause(z3_replayer & in) {
  Z3_solver_register_on_clause(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_obj_addr(2),
    reinterpret_cast<Z3_on_clause_eh*>(in.get_obj(3)));
}
void exec_Z3_solver_propagate_init(z3_replayer & in) {
  Z3_solver_propagate_init(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_obj_addr(2),
    reinterpret_cast<Z3_push_eh*>(in.get_obj(3)),
    reinterpret_cast<Z3_pop_eh*>(in.get_obj(4)),
    reinterpret_cast<Z3_fresh_eh*>(in.get_obj(5)));
}
void exec_Z3_solver_propagate_fixed(z3_replayer & in) {
  Z3_solver_propagate_fixed(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_fixed_eh*>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_final(z3_replayer & in) {
  Z3_solver_propagate_final(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_final_eh*>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_eq(z3_replayer & in) {
  Z3_solver_propagate_eq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_eq_eh*>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_diseq(z3_replayer & in) {
  Z3_solver_propagate_diseq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_eq_eh*>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_created(z3_replayer & in) {
  Z3_solver_propagate_created(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_created_eh*>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_decide(z3_replayer & in) {
  Z3_solver_propagate_decide(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_decide_eh*>(in.get_obj(2)));
}
void exec_Z3_solver_next_split(z3_replayer & in) {
  Z3_solver_next_split(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver_callback>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_uint(3),
    static_cast<Z3_lbool>(in.get_int(4)));
}
void exec_Z3_solver_propagate_declare(z3_replayer & in) {
  Z3_func_decl result = Z3_solver_propagate_declare(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_symbol(1),
    in.get_uint(2),
    reinterpret_cast<Z3_sort*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_solver_propagate_register(z3_replayer & in) {
  Z3_solver_propagate_register(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_register_cb(z3_replayer & in) {
  Z3_solver_propagate_register_cb(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver_callback>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_solver_propagate_consequence(z3_replayer & in) {
  Z3_solver_propagate_consequence(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver_callback>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)),
    in.get_uint(4),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(5)),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(6)),
    reinterpret_cast<Z3_ast>(in.get_obj(7)));
}
void exec_Z3_solver_set_initial_value(z3_replayer & in) {
  Z3_solver_set_initial_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_solver_check(z3_replayer & in) {
  Z3_solver_check(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_check_assumptions(z3_replayer & in) {
  Z3_solver_check_assumptions(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
}
void exec_Z3_get_implied_equalities(z3_replayer & in) {
  Z3_get_implied_equalities(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)),
    in.get_uint_array(4));
}
void exec_Z3_solver_get_consequences(z3_replayer & in) {
  Z3_solver_get_consequences(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(2)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(3)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(4)));
}
void exec_Z3_solver_cube(z3_replayer & in) {
  Z3_ast_vector result = Z3_solver_cube(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(2)),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_solver_get_model(z3_replayer & in) {
  Z3_model result = Z3_solver_get_model(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_proof(z3_replayer & in) {
  Z3_ast result = Z3_solver_get_proof(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_unsat_core(z3_replayer & in) {
  Z3_ast_vector result = Z3_solver_get_unsat_core(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_get_reason_unknown(z3_replayer & in) {
  Z3_solver_get_reason_unknown(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_get_statistics(z3_replayer & in) {
  Z3_stats result = Z3_solver_get_statistics(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_solver_to_string(z3_replayer & in) {
  Z3_solver_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)));
}
void exec_Z3_solver_to_dimacs_string(z3_replayer & in) {
  Z3_solver_to_dimacs_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_solver>(in.get_obj(1)),
    in.get_bool(2));
}
void exec_Z3_stats_to_string(z3_replayer & in) {
  Z3_stats_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)));
}
void exec_Z3_stats_inc_ref(z3_replayer & in) {
  Z3_stats_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)));
}
void exec_Z3_stats_dec_ref(z3_replayer & in) {
  Z3_stats_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)));
}
void exec_Z3_stats_size(z3_replayer & in) {
  Z3_stats_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)));
}
void exec_Z3_stats_get_key(z3_replayer & in) {
  Z3_stats_get_key(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_stats_is_uint(z3_replayer & in) {
  Z3_stats_is_uint(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_stats_is_double(z3_replayer & in) {
  Z3_stats_is_double(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_stats_get_uint_value(z3_replayer & in) {
  Z3_stats_get_uint_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_stats_get_double_value(z3_replayer & in) {
  Z3_stats_get_double_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_stats>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_get_estimated_alloc_size(z3_replayer & in) {
  Z3_get_estimated_alloc_size(
    );
}
void exec_Z3_mk_ast_vector(z3_replayer & in) {
  Z3_ast_vector result = Z3_mk_ast_vector(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_ast_vector_inc_ref(z3_replayer & in) {
  Z3_ast_vector_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)));
}
void exec_Z3_ast_vector_dec_ref(z3_replayer & in) {
  Z3_ast_vector_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)));
}
void exec_Z3_ast_vector_size(z3_replayer & in) {
  Z3_ast_vector_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)));
}
void exec_Z3_ast_vector_get(z3_replayer & in) {
  Z3_ast result = Z3_ast_vector_get(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_ast_vector_set(z3_replayer & in) {
  Z3_ast_vector_set(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_ast_vector_resize(z3_replayer & in) {
  Z3_ast_vector_resize(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_ast_vector_push(z3_replayer & in) {
  Z3_ast_vector_push(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_ast_vector_translate(z3_replayer & in) {
  Z3_ast_vector result = Z3_ast_vector_translate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)),
    reinterpret_cast<Z3_context>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_ast_vector_to_string(z3_replayer & in) {
  Z3_ast_vector_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)));
}
void exec_Z3_mk_ast_map(z3_replayer & in) {
  Z3_ast_map result = Z3_mk_ast_map(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_ast_map_inc_ref(z3_replayer & in) {
  Z3_ast_map_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)));
}
void exec_Z3_ast_map_dec_ref(z3_replayer & in) {
  Z3_ast_map_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)));
}
void exec_Z3_ast_map_contains(z3_replayer & in) {
  Z3_ast_map_contains(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_ast_map_find(z3_replayer & in) {
  Z3_ast result = Z3_ast_map_find(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_ast_map_insert(z3_replayer & in) {
  Z3_ast_map_insert(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_ast_map_erase(z3_replayer & in) {
  Z3_ast_map_erase(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_ast_map_reset(z3_replayer & in) {
  Z3_ast_map_reset(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)));
}
void exec_Z3_ast_map_size(z3_replayer & in) {
  Z3_ast_map_size(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)));
}
void exec_Z3_ast_map_keys(z3_replayer & in) {
  Z3_ast_vector result = Z3_ast_map_keys(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_ast_map_to_string(z3_replayer & in) {
  Z3_ast_map_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(1)));
}
void exec_Z3_algebraic_is_value(z3_replayer & in) {
  Z3_algebraic_is_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_algebraic_is_pos(z3_replayer & in) {
  Z3_algebraic_is_pos(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_algebraic_is_neg(z3_replayer & in) {
  Z3_algebraic_is_neg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_algebraic_is_zero(z3_replayer & in) {
  Z3_algebraic_is_zero(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_algebraic_sign(z3_replayer & in) {
  Z3_algebraic_sign(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_algebraic_add(z3_replayer & in) {
  Z3_ast result = Z3_algebraic_add(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_algebraic_sub(z3_replayer & in) {
  Z3_ast result = Z3_algebraic_sub(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_algebraic_mul(z3_replayer & in) {
  Z3_ast result = Z3_algebraic_mul(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_algebraic_div(z3_replayer & in) {
  Z3_ast result = Z3_algebraic_div(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_algebraic_root(z3_replayer & in) {
  Z3_ast result = Z3_algebraic_root(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_algebraic_power(z3_replayer & in) {
  Z3_ast result = Z3_algebraic_power(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_algebraic_lt(z3_replayer & in) {
  Z3_algebraic_lt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_algebraic_gt(z3_replayer & in) {
  Z3_algebraic_gt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_algebraic_le(z3_replayer & in) {
  Z3_algebraic_le(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_algebraic_ge(z3_replayer & in) {
  Z3_algebraic_ge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_algebraic_eq(z3_replayer & in) {
  Z3_algebraic_eq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_algebraic_neq(z3_replayer & in) {
  Z3_algebraic_neq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_algebraic_roots(z3_replayer & in) {
  Z3_ast_vector result = Z3_algebraic_roots(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
  in.store_result(result);
}
void exec_Z3_algebraic_eval(z3_replayer & in) {
  Z3_algebraic_eval(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
}
void exec_Z3_algebraic_get_poly(z3_replayer & in) {
  Z3_ast_vector result = Z3_algebraic_get_poly(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_algebraic_get_i(z3_replayer & in) {
  Z3_algebraic_get_i(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_polynomial_subresultants(z3_replayer & in) {
  Z3_ast_vector result = Z3_polynomial_subresultants(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_rcf_del(z3_replayer & in) {
  Z3_rcf_del(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_mk_rational(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_mk_rational(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_str(1));
  in.store_result(result);
}
void exec_Z3_rcf_mk_small_int(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_mk_small_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int(1));
  in.store_result(result);
}
void exec_Z3_rcf_mk_pi(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_mk_pi(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_rcf_mk_e(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_mk_e(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_rcf_mk_infinitesimal(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_mk_infinitesimal(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_rcf_mk_roots(z3_replayer & in) {
  Z3_rcf_mk_roots(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    reinterpret_cast<Z3_rcf_num*>(in.get_obj_array(2)),
    reinterpret_cast<Z3_rcf_num*>(in.get_obj_array(3)));
}
void exec_Z3_rcf_add(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_add(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_rcf_sub(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_sub(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_rcf_mul(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_mul(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_rcf_div(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_div(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_rcf_neg(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_neg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_rcf_inv(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_inv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_rcf_power(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_power(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_rcf_lt(z3_replayer & in) {
  Z3_rcf_lt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
}
void exec_Z3_rcf_gt(z3_replayer & in) {
  Z3_rcf_gt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
}
void exec_Z3_rcf_le(z3_replayer & in) {
  Z3_rcf_le(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
}
void exec_Z3_rcf_ge(z3_replayer & in) {
  Z3_rcf_ge(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
}
void exec_Z3_rcf_eq(z3_replayer & in) {
  Z3_rcf_eq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
}
void exec_Z3_rcf_neq(z3_replayer & in) {
  Z3_rcf_neq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(2)));
}
void exec_Z3_rcf_num_to_string(z3_replayer & in) {
  Z3_rcf_num_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_bool(2),
    in.get_bool(3));
}
void exec_Z3_rcf_num_to_decimal_string(z3_replayer & in) {
  Z3_rcf_num_to_decimal_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_rcf_get_numerator_denominator(z3_replayer & in) {
  Z3_rcf_get_numerator_denominator(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    reinterpret_cast<Z3_rcf_num*>(in.get_obj_addr(2)),
    reinterpret_cast<Z3_rcf_num*>(in.get_obj_addr(3)));
}
void exec_Z3_rcf_is_rational(z3_replayer & in) {
  Z3_rcf_is_rational(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_is_algebraic(z3_replayer & in) {
  Z3_rcf_is_algebraic(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_is_infinitesimal(z3_replayer & in) {
  Z3_rcf_is_infinitesimal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_is_transcendental(z3_replayer & in) {
  Z3_rcf_is_transcendental(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_extension_index(z3_replayer & in) {
  Z3_rcf_extension_index(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_transcendental_name(z3_replayer & in) {
  Z3_rcf_transcendental_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_infinitesimal_name(z3_replayer & in) {
  Z3_rcf_infinitesimal_name(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_num_coefficients(z3_replayer & in) {
  Z3_rcf_num_coefficients(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_coefficient(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_coefficient(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_rcf_interval(z3_replayer & in) {
  Z3_rcf_interval(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_int_addr(2),
    in.get_int_addr(3),
    reinterpret_cast<Z3_rcf_num*>(in.get_obj_addr(4)),
    in.get_int_addr(5),
    in.get_int_addr(6),
    reinterpret_cast<Z3_rcf_num*>(in.get_obj_addr(7)));
}
void exec_Z3_rcf_num_sign_conditions(z3_replayer & in) {
  Z3_rcf_num_sign_conditions(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)));
}
void exec_Z3_rcf_sign_condition_sign(z3_replayer & in) {
  Z3_rcf_sign_condition_sign(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_rcf_num_sign_condition_coefficients(z3_replayer & in) {
  Z3_rcf_num_sign_condition_coefficients(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_uint(2));
}
void exec_Z3_rcf_sign_condition_coefficient(z3_replayer & in) {
  Z3_rcf_num result = Z3_rcf_sign_condition_coefficient(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_rcf_num>(in.get_obj(1)),
    in.get_uint(2),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_mk_fixedpoint(z3_replayer & in) {
  Z3_fixedpoint result = Z3_mk_fixedpoint(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_inc_ref(z3_replayer & in) {
  Z3_fixedpoint_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
}
void exec_Z3_fixedpoint_dec_ref(z3_replayer & in) {
  Z3_fixedpoint_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
}
void exec_Z3_fixedpoint_add_rule(z3_replayer & in) {
  Z3_fixedpoint_add_rule(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_symbol(3));
}
void exec_Z3_fixedpoint_add_fact(z3_replayer & in) {
  Z3_fixedpoint_add_fact(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)),
    in.get_uint(3),
    in.get_uint_array(4));
}
void exec_Z3_fixedpoint_assert(z3_replayer & in) {
  Z3_fixedpoint_assert(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_fixedpoint_query(z3_replayer & in) {
  Z3_fixedpoint_query(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_fixedpoint_query_relations(z3_replayer & in) {
  Z3_fixedpoint_query_relations(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_func_decl*>(in.get_obj_array(3)));
}
void exec_Z3_fixedpoint_get_answer(z3_replayer & in) {
  Z3_ast result = Z3_fixedpoint_get_answer(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_get_reason_unknown(z3_replayer & in) {
  Z3_fixedpoint_get_reason_unknown(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
}
void exec_Z3_fixedpoint_update_rule(z3_replayer & in) {
  Z3_fixedpoint_update_rule(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_symbol(3));
}
void exec_Z3_fixedpoint_get_num_levels(z3_replayer & in) {
  Z3_fixedpoint_get_num_levels(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
}
void exec_Z3_fixedpoint_get_cover_delta(z3_replayer & in) {
  Z3_ast result = Z3_fixedpoint_get_cover_delta(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    in.get_int(2),
    reinterpret_cast<Z3_func_decl>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_add_cover(z3_replayer & in) {
  Z3_fixedpoint_add_cover(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    in.get_int(2),
    reinterpret_cast<Z3_func_decl>(in.get_obj(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
}
void exec_Z3_fixedpoint_get_statistics(z3_replayer & in) {
  Z3_stats result = Z3_fixedpoint_get_statistics(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_register_relation(z3_replayer & in) {
  Z3_fixedpoint_register_relation(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
}
void exec_Z3_fixedpoint_set_predicate_representation(z3_replayer & in) {
  Z3_fixedpoint_set_predicate_representation(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)),
    in.get_uint(3),
    in.get_symbol_array(4));
}
void exec_Z3_fixedpoint_get_rules(z3_replayer & in) {
  Z3_ast_vector result = Z3_fixedpoint_get_rules(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_get_assertions(z3_replayer & in) {
  Z3_ast_vector result = Z3_fixedpoint_get_assertions(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_set_params(z3_replayer & in) {
  Z3_fixedpoint_set_params(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_params>(in.get_obj(2)));
}
void exec_Z3_fixedpoint_get_help(z3_replayer & in) {
  Z3_fixedpoint_get_help(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
}
void exec_Z3_fixedpoint_get_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_fixedpoint_get_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_to_string(z3_replayer & in) {
  Z3_fixedpoint_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
}
void exec_Z3_fixedpoint_from_string(z3_replayer & in) {
  Z3_ast_vector result = Z3_fixedpoint_from_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    in.get_str(2));
  in.store_result(result);
}
void exec_Z3_fixedpoint_from_file(z3_replayer & in) {
  Z3_ast_vector result = Z3_fixedpoint_from_file(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    in.get_str(2));
  in.store_result(result);
}
void exec_Z3_mk_optimize(z3_replayer & in) {
  Z3_optimize result = Z3_mk_optimize(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_optimize_inc_ref(z3_replayer & in) {
  Z3_optimize_inc_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_dec_ref(z3_replayer & in) {
  Z3_optimize_dec_ref(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_assert(z3_replayer & in) {
  Z3_optimize_assert(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_optimize_assert_and_track(z3_replayer & in) {
  Z3_optimize_assert_and_track(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_optimize_assert_soft(z3_replayer & in) {
  Z3_optimize_assert_soft(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_str(3),
    in.get_symbol(4));
}
void exec_Z3_optimize_maximize(z3_replayer & in) {
  Z3_optimize_maximize(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_optimize_minimize(z3_replayer & in) {
  Z3_optimize_minimize(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
}
void exec_Z3_optimize_push(z3_replayer & in) {
  Z3_optimize_push(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_pop(z3_replayer & in) {
  Z3_optimize_pop(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_set_initial_value(z3_replayer & in) {
  Z3_optimize_set_initial_value(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_optimize_check(z3_replayer & in) {
  Z3_optimize_check(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_ast*>(in.get_obj_array(3)));
}
void exec_Z3_optimize_get_reason_unknown(z3_replayer & in) {
  Z3_optimize_get_reason_unknown(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_get_model(z3_replayer & in) {
  Z3_model result = Z3_optimize_get_model(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_optimize_get_unsat_core(z3_replayer & in) {
  Z3_ast_vector result = Z3_optimize_get_unsat_core(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_optimize_set_params(z3_replayer & in) {
  Z3_optimize_set_params(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    reinterpret_cast<Z3_params>(in.get_obj(2)));
}
void exec_Z3_optimize_get_param_descrs(z3_replayer & in) {
  Z3_param_descrs result = Z3_optimize_get_param_descrs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_optimize_get_lower(z3_replayer & in) {
  Z3_ast result = Z3_optimize_get_lower(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_optimize_get_upper(z3_replayer & in) {
  Z3_ast result = Z3_optimize_get_upper(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_optimize_get_lower_as_vector(z3_replayer & in) {
  Z3_ast_vector result = Z3_optimize_get_lower_as_vector(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_optimize_get_upper_as_vector(z3_replayer & in) {
  Z3_ast_vector result = Z3_optimize_get_upper_as_vector(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_optimize_to_string(z3_replayer & in) {
  Z3_optimize_to_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_from_string(z3_replayer & in) {
  Z3_optimize_from_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_str(2));
}
void exec_Z3_optimize_from_file(z3_replayer & in) {
  Z3_optimize_from_file(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)),
    in.get_str(2));
}
void exec_Z3_optimize_get_help(z3_replayer & in) {
  Z3_optimize_get_help(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
}
void exec_Z3_optimize_get_statistics(z3_replayer & in) {
  Z3_stats result = Z3_optimize_get_statistics(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_optimize_get_assertions(z3_replayer & in) {
  Z3_ast_vector result = Z3_optimize_get_assertions(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_optimize_get_objectives(z3_replayer & in) {
  Z3_ast_vector result = Z3_optimize_get_objectives(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_optimize>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rounding_mode_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_rounding_mode_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_round_nearest_ties_to_even(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_round_nearest_ties_to_even(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rne(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_rne(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_round_nearest_ties_to_away(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_round_nearest_ties_to_away(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rna(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_rna(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_round_toward_positive(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_round_toward_positive(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rtp(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_rtp(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_round_toward_negative(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_round_toward_negative(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rtn(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_rtn(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_round_toward_zero(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_round_toward_zero(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rtz(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_rtz(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_uint(1),
    in.get_uint(2));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_half(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_half(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_16(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_16(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_single(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_single(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_32(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_32(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_double(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_double(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_64(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_64(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_quadruple(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_quadruple(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sort_128(z3_replayer & in) {
  Z3_sort result = Z3_mk_fpa_sort_128(
    reinterpret_cast<Z3_context>(in.get_obj(0)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_nan(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_nan(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_inf(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_inf(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_bool(2));
  in.store_result(result);
}
void exec_Z3_mk_fpa_zero(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_zero(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)),
    in.get_bool(2));
  in.store_result(result);
}
void exec_Z3_mk_fpa_fp(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_fp(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_numeral_float(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_numeral_float(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_float(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_numeral_double(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_numeral_double(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_double(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_numeral_int(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_numeral_int(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_int(1),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_numeral_int_uint(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_numeral_int_uint(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_int(2),
    in.get_uint(3),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_numeral_int64_uint64(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_numeral_int64_uint64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    in.get_bool(1),
    in.get_int64(2),
    in.get_uint64(3),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_abs(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_abs(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_neg(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_neg(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_add(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_add(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sub(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_sub(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_mul(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_mul(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_div(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_div(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_fma(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_fma(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_sqrt(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_sqrt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_rem(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_rem(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_round_to_integral(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_round_to_integral(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_min(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_min(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_max(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_max(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_leq(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_leq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_lt(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_lt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_geq(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_geq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_gt(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_gt(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_eq(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_eq(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_normal(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_normal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_subnormal(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_subnormal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_zero(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_zero(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_infinite(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_infinite(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_nan(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_nan(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_negative(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_negative(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_is_positive(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_is_positive(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_fp_bv(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_fp_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_sort>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_fp_float(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_fp_float(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_sort>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_fp_real(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_fp_real(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_sort>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_fp_signed(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_fp_signed(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_sort>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_fp_unsigned(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_fp_unsigned(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_sort>(in.get_obj(3)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_ubv(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_ubv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_sbv(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_sbv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_uint(3));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_real(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_real(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fpa_get_ebits(z3_replayer & in) {
  Z3_fpa_get_ebits(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_fpa_get_sbits(z3_replayer & in) {
  Z3_fpa_get_sbits(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_sort>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_nan(z3_replayer & in) {
  Z3_fpa_is_numeral_nan(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_inf(z3_replayer & in) {
  Z3_fpa_is_numeral_inf(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_zero(z3_replayer & in) {
  Z3_fpa_is_numeral_zero(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_normal(z3_replayer & in) {
  Z3_fpa_is_numeral_normal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_subnormal(z3_replayer & in) {
  Z3_fpa_is_numeral_subnormal(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_positive(z3_replayer & in) {
  Z3_fpa_is_numeral_positive(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_is_numeral_negative(z3_replayer & in) {
  Z3_fpa_is_numeral_negative(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_get_numeral_sign_bv(z3_replayer & in) {
  Z3_ast result = Z3_fpa_get_numeral_sign_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fpa_get_numeral_significand_bv(z3_replayer & in) {
  Z3_ast result = Z3_fpa_get_numeral_significand_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fpa_get_numeral_sign(z3_replayer & in) {
  Z3_fpa_get_numeral_sign(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_int_addr(2));
}
void exec_Z3_fpa_get_numeral_significand_string(z3_replayer & in) {
  Z3_fpa_get_numeral_significand_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
}
void exec_Z3_fpa_get_numeral_significand_uint64(z3_replayer & in) {
  Z3_fpa_get_numeral_significand_uint64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_uint64_addr(2));
}
void exec_Z3_fpa_get_numeral_exponent_string(z3_replayer & in) {
  Z3_fpa_get_numeral_exponent_string(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_bool(2));
}
void exec_Z3_fpa_get_numeral_exponent_int64(z3_replayer & in) {
  Z3_fpa_get_numeral_exponent_int64(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_int64_addr(2),
    in.get_bool(3));
}
void exec_Z3_fpa_get_numeral_exponent_bv(z3_replayer & in) {
  Z3_ast result = Z3_fpa_get_numeral_exponent_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    in.get_bool(2));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_ieee_bv(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_ieee_bv(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_mk_fpa_to_fp_int_real(z3_replayer & in) {
  Z3_ast result = Z3_mk_fpa_to_fp_int_real(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)),
    reinterpret_cast<Z3_sort>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_query_from_lvl(z3_replayer & in) {
  Z3_fixedpoint_query_from_lvl(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)),
    in.get_uint(3));
}
void exec_Z3_fixedpoint_get_ground_sat_answer(z3_replayer & in) {
  Z3_ast result = Z3_fixedpoint_get_ground_sat_answer(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_get_rules_along_trace(z3_replayer & in) {
  Z3_ast_vector result = Z3_fixedpoint_get_rules_along_trace(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
  in.store_result(result);
}
void exec_Z3_fixedpoint_get_rule_names_along_trace(z3_replayer & in) {
  Z3_fixedpoint_get_rule_names_along_trace(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)));
}
void exec_Z3_fixedpoint_add_invariant(z3_replayer & in) {
  Z3_fixedpoint_add_invariant(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)),
    reinterpret_cast<Z3_ast>(in.get_obj(3)));
}
void exec_Z3_fixedpoint_get_reachable(z3_replayer & in) {
  Z3_ast result = Z3_fixedpoint_get_reachable(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_fixedpoint>(in.get_obj(1)),
    reinterpret_cast<Z3_func_decl>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_qe_model_project(z3_replayer & in) {
  Z3_ast result = Z3_qe_model_project(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_app*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)));
  in.store_result(result);
}
void exec_Z3_qe_model_project_skolem(z3_replayer & in) {
  Z3_ast result = Z3_qe_model_project_skolem(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_app*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(5)));
  in.store_result(result);
}
void exec_Z3_qe_model_project_with_witness(z3_replayer & in) {
  Z3_ast result = Z3_qe_model_project_with_witness(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    in.get_uint(2),
    reinterpret_cast<Z3_app*>(in.get_obj_array(3)),
    reinterpret_cast<Z3_ast>(in.get_obj(4)),
    reinterpret_cast<Z3_ast_map>(in.get_obj(5)));
  in.store_result(result);
}
void exec_Z3_model_extrapolate(z3_replayer & in) {
  Z3_ast result = Z3_model_extrapolate(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_model>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void exec_Z3_qe_lite(z3_replayer & in) {
  Z3_ast result = Z3_qe_lite(
    reinterpret_cast<Z3_context>(in.get_obj(0)),
    reinterpret_cast<Z3_ast_vector>(in.get_obj(1)),
    reinterpret_cast<Z3_ast>(in.get_obj(2)));
  in.store_result(result);
}
void register_z3_replayer_cmds(z3_replayer & in) {
  in.register_cmd(0, exec_Z3_global_param_set, "Z3_global_param_set");
  in.register_cmd(1, exec_Z3_global_param_reset_all, "Z3_global_param_reset_all");
  in.register_cmd(2, exec_Z3_global_param_get, "Z3_global_param_get");
  in.register_cmd(3, exec_Z3_mk_config, "Z3_mk_config");
  in.register_cmd(4, exec_Z3_del_config, "Z3_del_config");
  in.register_cmd(5, exec_Z3_set_param_value, "Z3_set_param_value");
  in.register_cmd(6, exec_Z3_mk_context, "Z3_mk_context");
  in.register_cmd(7, exec_Z3_mk_context_rc, "Z3_mk_context_rc");
  in.register_cmd(8, exec_Z3_del_context, "Z3_del_context");
  in.register_cmd(9, exec_Z3_inc_ref, "Z3_inc_ref");
  in.register_cmd(10, exec_Z3_dec_ref, "Z3_dec_ref");
  in.register_cmd(11, exec_Z3_update_param_value, "Z3_update_param_value");
  in.register_cmd(12, exec_Z3_get_global_param_descrs, "Z3_get_global_param_descrs");
  in.register_cmd(13, exec_Z3_interrupt, "Z3_interrupt");
  in.register_cmd(14, exec_Z3_enable_concurrent_dec_ref, "Z3_enable_concurrent_dec_ref");
  in.register_cmd(15, exec_Z3_mk_params, "Z3_mk_params");
  in.register_cmd(16, exec_Z3_params_inc_ref, "Z3_params_inc_ref");
  in.register_cmd(17, exec_Z3_params_dec_ref, "Z3_params_dec_ref");
  in.register_cmd(18, exec_Z3_params_set_bool, "Z3_params_set_bool");
  in.register_cmd(19, exec_Z3_params_set_uint, "Z3_params_set_uint");
  in.register_cmd(20, exec_Z3_params_set_double, "Z3_params_set_double");
  in.register_cmd(21, exec_Z3_params_set_symbol, "Z3_params_set_symbol");
  in.register_cmd(22, exec_Z3_params_to_string, "Z3_params_to_string");
  in.register_cmd(23, exec_Z3_params_validate, "Z3_params_validate");
  in.register_cmd(24, exec_Z3_param_descrs_inc_ref, "Z3_param_descrs_inc_ref");
  in.register_cmd(25, exec_Z3_param_descrs_dec_ref, "Z3_param_descrs_dec_ref");
  in.register_cmd(26, exec_Z3_param_descrs_get_kind, "Z3_param_descrs_get_kind");
  in.register_cmd(27, exec_Z3_param_descrs_size, "Z3_param_descrs_size");
  in.register_cmd(28, exec_Z3_param_descrs_get_name, "Z3_param_descrs_get_name");
  in.register_cmd(29, exec_Z3_param_descrs_get_documentation, "Z3_param_descrs_get_documentation");
  in.register_cmd(30, exec_Z3_param_descrs_to_string, "Z3_param_descrs_to_string");
  in.register_cmd(31, exec_Z3_mk_int_symbol, "Z3_mk_int_symbol");
  in.register_cmd(32, exec_Z3_mk_string_symbol, "Z3_mk_string_symbol");
  in.register_cmd(33, exec_Z3_mk_uninterpreted_sort, "Z3_mk_uninterpreted_sort");
  in.register_cmd(34, exec_Z3_mk_type_variable, "Z3_mk_type_variable");
  in.register_cmd(35, exec_Z3_mk_bool_sort, "Z3_mk_bool_sort");
  in.register_cmd(36, exec_Z3_mk_int_sort, "Z3_mk_int_sort");
  in.register_cmd(37, exec_Z3_mk_real_sort, "Z3_mk_real_sort");
  in.register_cmd(38, exec_Z3_mk_bv_sort, "Z3_mk_bv_sort");
  in.register_cmd(39, exec_Z3_mk_finite_domain_sort, "Z3_mk_finite_domain_sort");
  in.register_cmd(40, exec_Z3_mk_array_sort, "Z3_mk_array_sort");
  in.register_cmd(41, exec_Z3_mk_array_sort_n, "Z3_mk_array_sort_n");
  in.register_cmd(42, exec_Z3_mk_tuple_sort, "Z3_mk_tuple_sort");
  in.register_cmd(43, exec_Z3_mk_enumeration_sort, "Z3_mk_enumeration_sort");
  in.register_cmd(44, exec_Z3_mk_list_sort, "Z3_mk_list_sort");
  in.register_cmd(45, exec_Z3_mk_constructor, "Z3_mk_constructor");
  in.register_cmd(46, exec_Z3_constructor_num_fields, "Z3_constructor_num_fields");
  in.register_cmd(47, exec_Z3_del_constructor, "Z3_del_constructor");
  in.register_cmd(48, exec_Z3_mk_datatype, "Z3_mk_datatype");
  in.register_cmd(49, exec_Z3_mk_datatype_sort, "Z3_mk_datatype_sort");
  in.register_cmd(50, exec_Z3_mk_constructor_list, "Z3_mk_constructor_list");
  in.register_cmd(51, exec_Z3_del_constructor_list, "Z3_del_constructor_list");
  in.register_cmd(52, exec_Z3_mk_datatypes, "Z3_mk_datatypes");
  in.register_cmd(53, exec_Z3_query_constructor, "Z3_query_constructor");
  in.register_cmd(54, exec_Z3_mk_func_decl, "Z3_mk_func_decl");
  in.register_cmd(55, exec_Z3_mk_app, "Z3_mk_app");
  in.register_cmd(56, exec_Z3_mk_const, "Z3_mk_const");
  in.register_cmd(57, exec_Z3_mk_fresh_func_decl, "Z3_mk_fresh_func_decl");
  in.register_cmd(58, exec_Z3_mk_fresh_const, "Z3_mk_fresh_const");
  in.register_cmd(59, exec_Z3_mk_rec_func_decl, "Z3_mk_rec_func_decl");
  in.register_cmd(60, exec_Z3_add_rec_def, "Z3_add_rec_def");
  in.register_cmd(61, exec_Z3_mk_true, "Z3_mk_true");
  in.register_cmd(62, exec_Z3_mk_false, "Z3_mk_false");
  in.register_cmd(63, exec_Z3_mk_eq, "Z3_mk_eq");
  in.register_cmd(64, exec_Z3_mk_distinct, "Z3_mk_distinct");
  in.register_cmd(65, exec_Z3_mk_not, "Z3_mk_not");
  in.register_cmd(66, exec_Z3_mk_ite, "Z3_mk_ite");
  in.register_cmd(67, exec_Z3_mk_iff, "Z3_mk_iff");
  in.register_cmd(68, exec_Z3_mk_implies, "Z3_mk_implies");
  in.register_cmd(69, exec_Z3_mk_xor, "Z3_mk_xor");
  in.register_cmd(70, exec_Z3_mk_and, "Z3_mk_and");
  in.register_cmd(71, exec_Z3_mk_or, "Z3_mk_or");
  in.register_cmd(72, exec_Z3_mk_add, "Z3_mk_add");
  in.register_cmd(73, exec_Z3_mk_mul, "Z3_mk_mul");
  in.register_cmd(74, exec_Z3_mk_sub, "Z3_mk_sub");
  in.register_cmd(75, exec_Z3_mk_unary_minus, "Z3_mk_unary_minus");
  in.register_cmd(76, exec_Z3_mk_div, "Z3_mk_div");
  in.register_cmd(77, exec_Z3_mk_mod, "Z3_mk_mod");
  in.register_cmd(78, exec_Z3_mk_rem, "Z3_mk_rem");
  in.register_cmd(79, exec_Z3_mk_power, "Z3_mk_power");
  in.register_cmd(80, exec_Z3_mk_abs, "Z3_mk_abs");
  in.register_cmd(81, exec_Z3_mk_lt, "Z3_mk_lt");
  in.register_cmd(82, exec_Z3_mk_le, "Z3_mk_le");
  in.register_cmd(83, exec_Z3_mk_gt, "Z3_mk_gt");
  in.register_cmd(84, exec_Z3_mk_ge, "Z3_mk_ge");
  in.register_cmd(85, exec_Z3_mk_divides, "Z3_mk_divides");
  in.register_cmd(86, exec_Z3_mk_int2real, "Z3_mk_int2real");
  in.register_cmd(87, exec_Z3_mk_real2int, "Z3_mk_real2int");
  in.register_cmd(88, exec_Z3_mk_is_int, "Z3_mk_is_int");
  in.register_cmd(89, exec_Z3_mk_bvnot, "Z3_mk_bvnot");
  in.register_cmd(90, exec_Z3_mk_bvredand, "Z3_mk_bvredand");
  in.register_cmd(91, exec_Z3_mk_bvredor, "Z3_mk_bvredor");
  in.register_cmd(92, exec_Z3_mk_bvand, "Z3_mk_bvand");
  in.register_cmd(93, exec_Z3_mk_bvor, "Z3_mk_bvor");
  in.register_cmd(94, exec_Z3_mk_bvxor, "Z3_mk_bvxor");
  in.register_cmd(95, exec_Z3_mk_bvnand, "Z3_mk_bvnand");
  in.register_cmd(96, exec_Z3_mk_bvnor, "Z3_mk_bvnor");
  in.register_cmd(97, exec_Z3_mk_bvxnor, "Z3_mk_bvxnor");
  in.register_cmd(98, exec_Z3_mk_bvneg, "Z3_mk_bvneg");
  in.register_cmd(99, exec_Z3_mk_bvadd, "Z3_mk_bvadd");
  in.register_cmd(100, exec_Z3_mk_bvsub, "Z3_mk_bvsub");
  in.register_cmd(101, exec_Z3_mk_bvmul, "Z3_mk_bvmul");
  in.register_cmd(102, exec_Z3_mk_bvudiv, "Z3_mk_bvudiv");
  in.register_cmd(103, exec_Z3_mk_bvsdiv, "Z3_mk_bvsdiv");
  in.register_cmd(104, exec_Z3_mk_bvurem, "Z3_mk_bvurem");
  in.register_cmd(105, exec_Z3_mk_bvsrem, "Z3_mk_bvsrem");
  in.register_cmd(106, exec_Z3_mk_bvsmod, "Z3_mk_bvsmod");
  in.register_cmd(107, exec_Z3_mk_bvult, "Z3_mk_bvult");
  in.register_cmd(108, exec_Z3_mk_bvslt, "Z3_mk_bvslt");
  in.register_cmd(109, exec_Z3_mk_bvule, "Z3_mk_bvule");
  in.register_cmd(110, exec_Z3_mk_bvsle, "Z3_mk_bvsle");
  in.register_cmd(111, exec_Z3_mk_bvuge, "Z3_mk_bvuge");
  in.register_cmd(112, exec_Z3_mk_bvsge, "Z3_mk_bvsge");
  in.register_cmd(113, exec_Z3_mk_bvugt, "Z3_mk_bvugt");
  in.register_cmd(114, exec_Z3_mk_bvsgt, "Z3_mk_bvsgt");
  in.register_cmd(115, exec_Z3_mk_concat, "Z3_mk_concat");
  in.register_cmd(116, exec_Z3_mk_extract, "Z3_mk_extract");
  in.register_cmd(117, exec_Z3_mk_sign_ext, "Z3_mk_sign_ext");
  in.register_cmd(118, exec_Z3_mk_zero_ext, "Z3_mk_zero_ext");
  in.register_cmd(119, exec_Z3_mk_repeat, "Z3_mk_repeat");
  in.register_cmd(120, exec_Z3_mk_bit2bool, "Z3_mk_bit2bool");
  in.register_cmd(121, exec_Z3_mk_bvshl, "Z3_mk_bvshl");
  in.register_cmd(122, exec_Z3_mk_bvlshr, "Z3_mk_bvlshr");
  in.register_cmd(123, exec_Z3_mk_bvashr, "Z3_mk_bvashr");
  in.register_cmd(124, exec_Z3_mk_rotate_left, "Z3_mk_rotate_left");
  in.register_cmd(125, exec_Z3_mk_rotate_right, "Z3_mk_rotate_right");
  in.register_cmd(126, exec_Z3_mk_ext_rotate_left, "Z3_mk_ext_rotate_left");
  in.register_cmd(127, exec_Z3_mk_ext_rotate_right, "Z3_mk_ext_rotate_right");
  in.register_cmd(128, exec_Z3_mk_int2bv, "Z3_mk_int2bv");
  in.register_cmd(129, exec_Z3_mk_bv2int, "Z3_mk_bv2int");
  in.register_cmd(130, exec_Z3_mk_bvadd_no_overflow, "Z3_mk_bvadd_no_overflow");
  in.register_cmd(131, exec_Z3_mk_bvadd_no_underflow, "Z3_mk_bvadd_no_underflow");
  in.register_cmd(132, exec_Z3_mk_bvsub_no_overflow, "Z3_mk_bvsub_no_overflow");
  in.register_cmd(133, exec_Z3_mk_bvsub_no_underflow, "Z3_mk_bvsub_no_underflow");
  in.register_cmd(134, exec_Z3_mk_bvsdiv_no_overflow, "Z3_mk_bvsdiv_no_overflow");
  in.register_cmd(135, exec_Z3_mk_bvneg_no_overflow, "Z3_mk_bvneg_no_overflow");
  in.register_cmd(136, exec_Z3_mk_bvmul_no_overflow, "Z3_mk_bvmul_no_overflow");
  in.register_cmd(137, exec_Z3_mk_bvmul_no_underflow, "Z3_mk_bvmul_no_underflow");
  in.register_cmd(138, exec_Z3_mk_select, "Z3_mk_select");
  in.register_cmd(139, exec_Z3_mk_select_n, "Z3_mk_select_n");
  in.register_cmd(140, exec_Z3_mk_store, "Z3_mk_store");
  in.register_cmd(141, exec_Z3_mk_store_n, "Z3_mk_store_n");
  in.register_cmd(142, exec_Z3_mk_const_array, "Z3_mk_const_array");
  in.register_cmd(143, exec_Z3_mk_map, "Z3_mk_map");
  in.register_cmd(144, exec_Z3_mk_array_default, "Z3_mk_array_default");
  in.register_cmd(145, exec_Z3_mk_as_array, "Z3_mk_as_array");
  in.register_cmd(146, exec_Z3_mk_set_has_size, "Z3_mk_set_has_size");
  in.register_cmd(147, exec_Z3_mk_set_sort, "Z3_mk_set_sort");
  in.register_cmd(148, exec_Z3_mk_empty_set, "Z3_mk_empty_set");
  in.register_cmd(149, exec_Z3_mk_full_set, "Z3_mk_full_set");
  in.register_cmd(150, exec_Z3_mk_set_add, "Z3_mk_set_add");
  in.register_cmd(151, exec_Z3_mk_set_del, "Z3_mk_set_del");
  in.register_cmd(152, exec_Z3_mk_set_union, "Z3_mk_set_union");
  in.register_cmd(153, exec_Z3_mk_set_intersect, "Z3_mk_set_intersect");
  in.register_cmd(154, exec_Z3_mk_set_difference, "Z3_mk_set_difference");
  in.register_cmd(155, exec_Z3_mk_set_complement, "Z3_mk_set_complement");
  in.register_cmd(156, exec_Z3_mk_set_member, "Z3_mk_set_member");
  in.register_cmd(157, exec_Z3_mk_set_subset, "Z3_mk_set_subset");
  in.register_cmd(158, exec_Z3_mk_array_ext, "Z3_mk_array_ext");
  in.register_cmd(159, exec_Z3_mk_numeral, "Z3_mk_numeral");
  in.register_cmd(160, exec_Z3_mk_real, "Z3_mk_real");
  in.register_cmd(161, exec_Z3_mk_real_int64, "Z3_mk_real_int64");
  in.register_cmd(162, exec_Z3_mk_int, "Z3_mk_int");
  in.register_cmd(163, exec_Z3_mk_unsigned_int, "Z3_mk_unsigned_int");
  in.register_cmd(164, exec_Z3_mk_int64, "Z3_mk_int64");
  in.register_cmd(165, exec_Z3_mk_unsigned_int64, "Z3_mk_unsigned_int64");
  in.register_cmd(166, exec_Z3_mk_bv_numeral, "Z3_mk_bv_numeral");
  in.register_cmd(167, exec_Z3_mk_seq_sort, "Z3_mk_seq_sort");
  in.register_cmd(168, exec_Z3_is_seq_sort, "Z3_is_seq_sort");
  in.register_cmd(169, exec_Z3_get_seq_sort_basis, "Z3_get_seq_sort_basis");
  in.register_cmd(170, exec_Z3_mk_re_sort, "Z3_mk_re_sort");
  in.register_cmd(171, exec_Z3_is_re_sort, "Z3_is_re_sort");
  in.register_cmd(172, exec_Z3_get_re_sort_basis, "Z3_get_re_sort_basis");
  in.register_cmd(173, exec_Z3_mk_string_sort, "Z3_mk_string_sort");
  in.register_cmd(174, exec_Z3_mk_char_sort, "Z3_mk_char_sort");
  in.register_cmd(175, exec_Z3_is_string_sort, "Z3_is_string_sort");
  in.register_cmd(176, exec_Z3_is_char_sort, "Z3_is_char_sort");
  in.register_cmd(177, exec_Z3_mk_string, "Z3_mk_string");
  in.register_cmd(178, exec_Z3_mk_lstring, "Z3_mk_lstring");
  in.register_cmd(179, exec_Z3_mk_u32string, "Z3_mk_u32string");
  in.register_cmd(180, exec_Z3_is_string, "Z3_is_string");
  in.register_cmd(181, exec_Z3_get_string, "Z3_get_string");
  in.register_cmd(182, exec_Z3_get_lstring, "Z3_get_lstring");
  in.register_cmd(183, exec_Z3_get_string_length, "Z3_get_string_length");
  in.register_cmd(184, exec_Z3_get_string_contents, "Z3_get_string_contents");
  in.register_cmd(185, exec_Z3_mk_seq_empty, "Z3_mk_seq_empty");
  in.register_cmd(186, exec_Z3_mk_seq_unit, "Z3_mk_seq_unit");
  in.register_cmd(187, exec_Z3_mk_seq_concat, "Z3_mk_seq_concat");
  in.register_cmd(188, exec_Z3_mk_seq_prefix, "Z3_mk_seq_prefix");
  in.register_cmd(189, exec_Z3_mk_seq_suffix, "Z3_mk_seq_suffix");
  in.register_cmd(190, exec_Z3_mk_seq_contains, "Z3_mk_seq_contains");
  in.register_cmd(191, exec_Z3_mk_str_lt, "Z3_mk_str_lt");
  in.register_cmd(192, exec_Z3_mk_str_le, "Z3_mk_str_le");
  in.register_cmd(193, exec_Z3_mk_seq_extract, "Z3_mk_seq_extract");
  in.register_cmd(194, exec_Z3_mk_seq_replace, "Z3_mk_seq_replace");
  in.register_cmd(195, exec_Z3_mk_seq_at, "Z3_mk_seq_at");
  in.register_cmd(196, exec_Z3_mk_seq_nth, "Z3_mk_seq_nth");
  in.register_cmd(197, exec_Z3_mk_seq_length, "Z3_mk_seq_length");
  in.register_cmd(198, exec_Z3_mk_seq_index, "Z3_mk_seq_index");
  in.register_cmd(199, exec_Z3_mk_seq_last_index, "Z3_mk_seq_last_index");
  in.register_cmd(200, exec_Z3_mk_seq_map, "Z3_mk_seq_map");
  in.register_cmd(201, exec_Z3_mk_seq_mapi, "Z3_mk_seq_mapi");
  in.register_cmd(202, exec_Z3_mk_seq_foldl, "Z3_mk_seq_foldl");
  in.register_cmd(203, exec_Z3_mk_seq_foldli, "Z3_mk_seq_foldli");
  in.register_cmd(204, exec_Z3_mk_str_to_int, "Z3_mk_str_to_int");
  in.register_cmd(205, exec_Z3_mk_int_to_str, "Z3_mk_int_to_str");
  in.register_cmd(206, exec_Z3_mk_string_to_code, "Z3_mk_string_to_code");
  in.register_cmd(207, exec_Z3_mk_string_from_code, "Z3_mk_string_from_code");
  in.register_cmd(208, exec_Z3_mk_ubv_to_str, "Z3_mk_ubv_to_str");
  in.register_cmd(209, exec_Z3_mk_sbv_to_str, "Z3_mk_sbv_to_str");
  in.register_cmd(210, exec_Z3_mk_seq_to_re, "Z3_mk_seq_to_re");
  in.register_cmd(211, exec_Z3_mk_seq_in_re, "Z3_mk_seq_in_re");
  in.register_cmd(212, exec_Z3_mk_re_plus, "Z3_mk_re_plus");
  in.register_cmd(213, exec_Z3_mk_re_star, "Z3_mk_re_star");
  in.register_cmd(214, exec_Z3_mk_re_option, "Z3_mk_re_option");
  in.register_cmd(215, exec_Z3_mk_re_union, "Z3_mk_re_union");
  in.register_cmd(216, exec_Z3_mk_re_concat, "Z3_mk_re_concat");
  in.register_cmd(217, exec_Z3_mk_re_range, "Z3_mk_re_range");
  in.register_cmd(218, exec_Z3_mk_re_allchar, "Z3_mk_re_allchar");
  in.register_cmd(219, exec_Z3_mk_re_loop, "Z3_mk_re_loop");
  in.register_cmd(220, exec_Z3_mk_re_power, "Z3_mk_re_power");
  in.register_cmd(221, exec_Z3_mk_re_intersect, "Z3_mk_re_intersect");
  in.register_cmd(222, exec_Z3_mk_re_complement, "Z3_mk_re_complement");
  in.register_cmd(223, exec_Z3_mk_re_diff, "Z3_mk_re_diff");
  in.register_cmd(224, exec_Z3_mk_re_empty, "Z3_mk_re_empty");
  in.register_cmd(225, exec_Z3_mk_re_full, "Z3_mk_re_full");
  in.register_cmd(226, exec_Z3_mk_char, "Z3_mk_char");
  in.register_cmd(227, exec_Z3_mk_char_le, "Z3_mk_char_le");
  in.register_cmd(228, exec_Z3_mk_char_to_int, "Z3_mk_char_to_int");
  in.register_cmd(229, exec_Z3_mk_char_to_bv, "Z3_mk_char_to_bv");
  in.register_cmd(230, exec_Z3_mk_char_from_bv, "Z3_mk_char_from_bv");
  in.register_cmd(231, exec_Z3_mk_char_is_digit, "Z3_mk_char_is_digit");
  in.register_cmd(232, exec_Z3_mk_linear_order, "Z3_mk_linear_order");
  in.register_cmd(233, exec_Z3_mk_partial_order, "Z3_mk_partial_order");
  in.register_cmd(234, exec_Z3_mk_piecewise_linear_order, "Z3_mk_piecewise_linear_order");
  in.register_cmd(235, exec_Z3_mk_tree_order, "Z3_mk_tree_order");
  in.register_cmd(236, exec_Z3_mk_transitive_closure, "Z3_mk_transitive_closure");
  in.register_cmd(237, exec_Z3_mk_pattern, "Z3_mk_pattern");
  in.register_cmd(238, exec_Z3_mk_bound, "Z3_mk_bound");
  in.register_cmd(239, exec_Z3_mk_forall, "Z3_mk_forall");
  in.register_cmd(240, exec_Z3_mk_exists, "Z3_mk_exists");
  in.register_cmd(241, exec_Z3_mk_quantifier, "Z3_mk_quantifier");
  in.register_cmd(242, exec_Z3_mk_quantifier_ex, "Z3_mk_quantifier_ex");
  in.register_cmd(243, exec_Z3_mk_forall_const, "Z3_mk_forall_const");
  in.register_cmd(244, exec_Z3_mk_exists_const, "Z3_mk_exists_const");
  in.register_cmd(245, exec_Z3_mk_quantifier_const, "Z3_mk_quantifier_const");
  in.register_cmd(246, exec_Z3_mk_quantifier_const_ex, "Z3_mk_quantifier_const_ex");
  in.register_cmd(247, exec_Z3_mk_lambda, "Z3_mk_lambda");
  in.register_cmd(248, exec_Z3_mk_lambda_const, "Z3_mk_lambda_const");
  in.register_cmd(249, exec_Z3_get_symbol_kind, "Z3_get_symbol_kind");
  in.register_cmd(250, exec_Z3_get_symbol_int, "Z3_get_symbol_int");
  in.register_cmd(251, exec_Z3_get_symbol_string, "Z3_get_symbol_string");
  in.register_cmd(252, exec_Z3_get_sort_name, "Z3_get_sort_name");
  in.register_cmd(253, exec_Z3_get_sort_id, "Z3_get_sort_id");
  in.register_cmd(254, exec_Z3_sort_to_ast, "Z3_sort_to_ast");
  in.register_cmd(255, exec_Z3_is_eq_sort, "Z3_is_eq_sort");
  in.register_cmd(256, exec_Z3_get_sort_kind, "Z3_get_sort_kind");
  in.register_cmd(257, exec_Z3_get_bv_sort_size, "Z3_get_bv_sort_size");
  in.register_cmd(258, exec_Z3_get_finite_domain_sort_size, "Z3_get_finite_domain_sort_size");
  in.register_cmd(259, exec_Z3_get_array_arity, "Z3_get_array_arity");
  in.register_cmd(260, exec_Z3_get_array_sort_domain, "Z3_get_array_sort_domain");
  in.register_cmd(261, exec_Z3_get_array_sort_domain_n, "Z3_get_array_sort_domain_n");
  in.register_cmd(262, exec_Z3_get_array_sort_range, "Z3_get_array_sort_range");
  in.register_cmd(263, exec_Z3_get_tuple_sort_mk_decl, "Z3_get_tuple_sort_mk_decl");
  in.register_cmd(264, exec_Z3_get_tuple_sort_num_fields, "Z3_get_tuple_sort_num_fields");
  in.register_cmd(265, exec_Z3_get_tuple_sort_field_decl, "Z3_get_tuple_sort_field_decl");
  in.register_cmd(266, exec_Z3_is_recursive_datatype_sort, "Z3_is_recursive_datatype_sort");
  in.register_cmd(267, exec_Z3_get_datatype_sort_num_constructors, "Z3_get_datatype_sort_num_constructors");
  in.register_cmd(268, exec_Z3_get_datatype_sort_constructor, "Z3_get_datatype_sort_constructor");
  in.register_cmd(269, exec_Z3_get_datatype_sort_recognizer, "Z3_get_datatype_sort_recognizer");
  in.register_cmd(270, exec_Z3_get_datatype_sort_constructor_accessor, "Z3_get_datatype_sort_constructor_accessor");
  in.register_cmd(271, exec_Z3_datatype_update_field, "Z3_datatype_update_field");
  in.register_cmd(272, exec_Z3_get_relation_arity, "Z3_get_relation_arity");
  in.register_cmd(273, exec_Z3_get_relation_column, "Z3_get_relation_column");
  in.register_cmd(274, exec_Z3_mk_atmost, "Z3_mk_atmost");
  in.register_cmd(275, exec_Z3_mk_atleast, "Z3_mk_atleast");
  in.register_cmd(276, exec_Z3_mk_pble, "Z3_mk_pble");
  in.register_cmd(277, exec_Z3_mk_pbge, "Z3_mk_pbge");
  in.register_cmd(278, exec_Z3_mk_pbeq, "Z3_mk_pbeq");
  in.register_cmd(279, exec_Z3_func_decl_to_ast, "Z3_func_decl_to_ast");
  in.register_cmd(280, exec_Z3_is_eq_func_decl, "Z3_is_eq_func_decl");
  in.register_cmd(281, exec_Z3_get_func_decl_id, "Z3_get_func_decl_id");
  in.register_cmd(282, exec_Z3_get_decl_name, "Z3_get_decl_name");
  in.register_cmd(283, exec_Z3_get_decl_kind, "Z3_get_decl_kind");
  in.register_cmd(284, exec_Z3_get_domain_size, "Z3_get_domain_size");
  in.register_cmd(285, exec_Z3_get_arity, "Z3_get_arity");
  in.register_cmd(286, exec_Z3_get_domain, "Z3_get_domain");
  in.register_cmd(287, exec_Z3_get_range, "Z3_get_range");
  in.register_cmd(288, exec_Z3_get_decl_num_parameters, "Z3_get_decl_num_parameters");
  in.register_cmd(289, exec_Z3_get_decl_parameter_kind, "Z3_get_decl_parameter_kind");
  in.register_cmd(290, exec_Z3_get_decl_int_parameter, "Z3_get_decl_int_parameter");
  in.register_cmd(291, exec_Z3_get_decl_double_parameter, "Z3_get_decl_double_parameter");
  in.register_cmd(292, exec_Z3_get_decl_symbol_parameter, "Z3_get_decl_symbol_parameter");
  in.register_cmd(293, exec_Z3_get_decl_sort_parameter, "Z3_get_decl_sort_parameter");
  in.register_cmd(294, exec_Z3_get_decl_ast_parameter, "Z3_get_decl_ast_parameter");
  in.register_cmd(295, exec_Z3_get_decl_func_decl_parameter, "Z3_get_decl_func_decl_parameter");
  in.register_cmd(296, exec_Z3_get_decl_rational_parameter, "Z3_get_decl_rational_parameter");
  in.register_cmd(297, exec_Z3_app_to_ast, "Z3_app_to_ast");
  in.register_cmd(298, exec_Z3_get_app_decl, "Z3_get_app_decl");
  in.register_cmd(299, exec_Z3_get_app_num_args, "Z3_get_app_num_args");
  in.register_cmd(300, exec_Z3_get_app_arg, "Z3_get_app_arg");
  in.register_cmd(301, exec_Z3_is_eq_ast, "Z3_is_eq_ast");
  in.register_cmd(302, exec_Z3_get_ast_id, "Z3_get_ast_id");
  in.register_cmd(303, exec_Z3_get_ast_hash, "Z3_get_ast_hash");
  in.register_cmd(304, exec_Z3_get_sort, "Z3_get_sort");
  in.register_cmd(305, exec_Z3_is_well_sorted, "Z3_is_well_sorted");
  in.register_cmd(306, exec_Z3_get_bool_value, "Z3_get_bool_value");
  in.register_cmd(307, exec_Z3_get_ast_kind, "Z3_get_ast_kind");
  in.register_cmd(308, exec_Z3_is_app, "Z3_is_app");
  in.register_cmd(309, exec_Z3_is_ground, "Z3_is_ground");
  in.register_cmd(310, exec_Z3_get_depth, "Z3_get_depth");
  in.register_cmd(311, exec_Z3_is_numeral_ast, "Z3_is_numeral_ast");
  in.register_cmd(312, exec_Z3_is_algebraic_number, "Z3_is_algebraic_number");
  in.register_cmd(313, exec_Z3_to_app, "Z3_to_app");
  in.register_cmd(314, exec_Z3_to_func_decl, "Z3_to_func_decl");
  in.register_cmd(315, exec_Z3_get_numeral_string, "Z3_get_numeral_string");
  in.register_cmd(316, exec_Z3_get_numeral_binary_string, "Z3_get_numeral_binary_string");
  in.register_cmd(317, exec_Z3_get_numeral_decimal_string, "Z3_get_numeral_decimal_string");
  in.register_cmd(318, exec_Z3_get_numeral_double, "Z3_get_numeral_double");
  in.register_cmd(319, exec_Z3_get_numerator, "Z3_get_numerator");
  in.register_cmd(320, exec_Z3_get_denominator, "Z3_get_denominator");
  in.register_cmd(321, exec_Z3_get_numeral_small, "Z3_get_numeral_small");
  in.register_cmd(322, exec_Z3_get_numeral_int, "Z3_get_numeral_int");
  in.register_cmd(323, exec_Z3_get_numeral_uint, "Z3_get_numeral_uint");
  in.register_cmd(324, exec_Z3_get_numeral_uint64, "Z3_get_numeral_uint64");
  in.register_cmd(325, exec_Z3_get_numeral_int64, "Z3_get_numeral_int64");
  in.register_cmd(326, exec_Z3_get_numeral_rational_int64, "Z3_get_numeral_rational_int64");
  in.register_cmd(327, exec_Z3_get_algebraic_number_lower, "Z3_get_algebraic_number_lower");
  in.register_cmd(328, exec_Z3_get_algebraic_number_upper, "Z3_get_algebraic_number_upper");
  in.register_cmd(329, exec_Z3_pattern_to_ast, "Z3_pattern_to_ast");
  in.register_cmd(330, exec_Z3_get_pattern_num_terms, "Z3_get_pattern_num_terms");
  in.register_cmd(331, exec_Z3_get_pattern, "Z3_get_pattern");
  in.register_cmd(332, exec_Z3_get_index_value, "Z3_get_index_value");
  in.register_cmd(333, exec_Z3_is_quantifier_forall, "Z3_is_quantifier_forall");
  in.register_cmd(334, exec_Z3_is_quantifier_exists, "Z3_is_quantifier_exists");
  in.register_cmd(335, exec_Z3_is_lambda, "Z3_is_lambda");
  in.register_cmd(336, exec_Z3_get_quantifier_weight, "Z3_get_quantifier_weight");
  in.register_cmd(337, exec_Z3_get_quantifier_skolem_id, "Z3_get_quantifier_skolem_id");
  in.register_cmd(338, exec_Z3_get_quantifier_id, "Z3_get_quantifier_id");
  in.register_cmd(339, exec_Z3_get_quantifier_num_patterns, "Z3_get_quantifier_num_patterns");
  in.register_cmd(340, exec_Z3_get_quantifier_pattern_ast, "Z3_get_quantifier_pattern_ast");
  in.register_cmd(341, exec_Z3_get_quantifier_num_no_patterns, "Z3_get_quantifier_num_no_patterns");
  in.register_cmd(342, exec_Z3_get_quantifier_no_pattern_ast, "Z3_get_quantifier_no_pattern_ast");
  in.register_cmd(343, exec_Z3_get_quantifier_num_bound, "Z3_get_quantifier_num_bound");
  in.register_cmd(344, exec_Z3_get_quantifier_bound_name, "Z3_get_quantifier_bound_name");
  in.register_cmd(345, exec_Z3_get_quantifier_bound_sort, "Z3_get_quantifier_bound_sort");
  in.register_cmd(346, exec_Z3_get_quantifier_body, "Z3_get_quantifier_body");
  in.register_cmd(347, exec_Z3_simplify, "Z3_simplify");
  in.register_cmd(348, exec_Z3_simplify_ex, "Z3_simplify_ex");
  in.register_cmd(349, exec_Z3_simplify_get_help, "Z3_simplify_get_help");
  in.register_cmd(350, exec_Z3_simplify_get_param_descrs, "Z3_simplify_get_param_descrs");
  in.register_cmd(351, exec_Z3_update_term, "Z3_update_term");
  in.register_cmd(352, exec_Z3_substitute, "Z3_substitute");
  in.register_cmd(353, exec_Z3_substitute_vars, "Z3_substitute_vars");
  in.register_cmd(354, exec_Z3_substitute_funs, "Z3_substitute_funs");
  in.register_cmd(355, exec_Z3_translate, "Z3_translate");
  in.register_cmd(356, exec_Z3_mk_model, "Z3_mk_model");
  in.register_cmd(357, exec_Z3_model_inc_ref, "Z3_model_inc_ref");
  in.register_cmd(358, exec_Z3_model_dec_ref, "Z3_model_dec_ref");
  in.register_cmd(359, exec_Z3_model_eval, "Z3_model_eval");
  in.register_cmd(360, exec_Z3_model_get_const_interp, "Z3_model_get_const_interp");
  in.register_cmd(361, exec_Z3_model_has_interp, "Z3_model_has_interp");
  in.register_cmd(362, exec_Z3_model_get_func_interp, "Z3_model_get_func_interp");
  in.register_cmd(363, exec_Z3_model_get_num_consts, "Z3_model_get_num_consts");
  in.register_cmd(364, exec_Z3_model_get_const_decl, "Z3_model_get_const_decl");
  in.register_cmd(365, exec_Z3_model_get_num_funcs, "Z3_model_get_num_funcs");
  in.register_cmd(366, exec_Z3_model_get_func_decl, "Z3_model_get_func_decl");
  in.register_cmd(367, exec_Z3_model_get_num_sorts, "Z3_model_get_num_sorts");
  in.register_cmd(368, exec_Z3_model_get_sort, "Z3_model_get_sort");
  in.register_cmd(369, exec_Z3_model_get_sort_universe, "Z3_model_get_sort_universe");
  in.register_cmd(370, exec_Z3_model_translate, "Z3_model_translate");
  in.register_cmd(371, exec_Z3_is_as_array, "Z3_is_as_array");
  in.register_cmd(372, exec_Z3_get_as_array_func_decl, "Z3_get_as_array_func_decl");
  in.register_cmd(373, exec_Z3_add_func_interp, "Z3_add_func_interp");
  in.register_cmd(374, exec_Z3_add_const_interp, "Z3_add_const_interp");
  in.register_cmd(375, exec_Z3_func_interp_inc_ref, "Z3_func_interp_inc_ref");
  in.register_cmd(376, exec_Z3_func_interp_dec_ref, "Z3_func_interp_dec_ref");
  in.register_cmd(377, exec_Z3_func_interp_get_num_entries, "Z3_func_interp_get_num_entries");
  in.register_cmd(378, exec_Z3_func_interp_get_entry, "Z3_func_interp_get_entry");
  in.register_cmd(379, exec_Z3_func_interp_get_else, "Z3_func_interp_get_else");
  in.register_cmd(380, exec_Z3_func_interp_set_else, "Z3_func_interp_set_else");
  in.register_cmd(381, exec_Z3_func_interp_get_arity, "Z3_func_interp_get_arity");
  in.register_cmd(382, exec_Z3_func_interp_add_entry, "Z3_func_interp_add_entry");
  in.register_cmd(383, exec_Z3_func_entry_inc_ref, "Z3_func_entry_inc_ref");
  in.register_cmd(384, exec_Z3_func_entry_dec_ref, "Z3_func_entry_dec_ref");
  in.register_cmd(385, exec_Z3_func_entry_get_value, "Z3_func_entry_get_value");
  in.register_cmd(386, exec_Z3_func_entry_get_num_args, "Z3_func_entry_get_num_args");
  in.register_cmd(387, exec_Z3_func_entry_get_arg, "Z3_func_entry_get_arg");
  in.register_cmd(388, exec_Z3_toggle_warning_messages, "Z3_toggle_warning_messages");
  in.register_cmd(389, exec_Z3_set_ast_print_mode, "Z3_set_ast_print_mode");
  in.register_cmd(390, exec_Z3_ast_to_string, "Z3_ast_to_string");
  in.register_cmd(391, exec_Z3_pattern_to_string, "Z3_pattern_to_string");
  in.register_cmd(392, exec_Z3_sort_to_string, "Z3_sort_to_string");
  in.register_cmd(393, exec_Z3_func_decl_to_string, "Z3_func_decl_to_string");
  in.register_cmd(394, exec_Z3_model_to_string, "Z3_model_to_string");
  in.register_cmd(395, exec_Z3_benchmark_to_smtlib_string, "Z3_benchmark_to_smtlib_string");
  in.register_cmd(396, exec_Z3_parse_smtlib2_string, "Z3_parse_smtlib2_string");
  in.register_cmd(397, exec_Z3_parse_smtlib2_file, "Z3_parse_smtlib2_file");
  in.register_cmd(398, exec_Z3_eval_smtlib2_string, "Z3_eval_smtlib2_string");
  in.register_cmd(399, exec_Z3_mk_parser_context, "Z3_mk_parser_context");
  in.register_cmd(400, exec_Z3_parser_context_inc_ref, "Z3_parser_context_inc_ref");
  in.register_cmd(401, exec_Z3_parser_context_dec_ref, "Z3_parser_context_dec_ref");
  in.register_cmd(402, exec_Z3_parser_context_add_sort, "Z3_parser_context_add_sort");
  in.register_cmd(403, exec_Z3_parser_context_add_decl, "Z3_parser_context_add_decl");
  in.register_cmd(404, exec_Z3_parser_context_from_string, "Z3_parser_context_from_string");
  in.register_cmd(405, exec_Z3_get_error_code, "Z3_get_error_code");
  in.register_cmd(406, exec_Z3_set_error, "Z3_set_error");
  in.register_cmd(407, exec_Z3_get_error_msg, "Z3_get_error_msg");
  in.register_cmd(408, exec_Z3_get_version, "Z3_get_version");
  in.register_cmd(409, exec_Z3_get_full_version, "Z3_get_full_version");
  in.register_cmd(410, exec_Z3_enable_trace, "Z3_enable_trace");
  in.register_cmd(411, exec_Z3_disable_trace, "Z3_disable_trace");
  in.register_cmd(412, exec_Z3_reset_memory, "Z3_reset_memory");
  in.register_cmd(413, exec_Z3_finalize_memory, "Z3_finalize_memory");
  in.register_cmd(414, exec_Z3_mk_goal, "Z3_mk_goal");
  in.register_cmd(415, exec_Z3_goal_inc_ref, "Z3_goal_inc_ref");
  in.register_cmd(416, exec_Z3_goal_dec_ref, "Z3_goal_dec_ref");
  in.register_cmd(417, exec_Z3_goal_precision, "Z3_goal_precision");
  in.register_cmd(418, exec_Z3_goal_assert, "Z3_goal_assert");
  in.register_cmd(419, exec_Z3_goal_inconsistent, "Z3_goal_inconsistent");
  in.register_cmd(420, exec_Z3_goal_depth, "Z3_goal_depth");
  in.register_cmd(421, exec_Z3_goal_reset, "Z3_goal_reset");
  in.register_cmd(422, exec_Z3_goal_size, "Z3_goal_size");
  in.register_cmd(423, exec_Z3_goal_formula, "Z3_goal_formula");
  in.register_cmd(424, exec_Z3_goal_num_exprs, "Z3_goal_num_exprs");
  in.register_cmd(425, exec_Z3_goal_is_decided_sat, "Z3_goal_is_decided_sat");
  in.register_cmd(426, exec_Z3_goal_is_decided_unsat, "Z3_goal_is_decided_unsat");
  in.register_cmd(427, exec_Z3_goal_translate, "Z3_goal_translate");
  in.register_cmd(428, exec_Z3_goal_convert_model, "Z3_goal_convert_model");
  in.register_cmd(429, exec_Z3_goal_to_string, "Z3_goal_to_string");
  in.register_cmd(430, exec_Z3_goal_to_dimacs_string, "Z3_goal_to_dimacs_string");
  in.register_cmd(431, exec_Z3_mk_tactic, "Z3_mk_tactic");
  in.register_cmd(432, exec_Z3_tactic_inc_ref, "Z3_tactic_inc_ref");
  in.register_cmd(433, exec_Z3_tactic_dec_ref, "Z3_tactic_dec_ref");
  in.register_cmd(434, exec_Z3_mk_probe, "Z3_mk_probe");
  in.register_cmd(435, exec_Z3_probe_inc_ref, "Z3_probe_inc_ref");
  in.register_cmd(436, exec_Z3_probe_dec_ref, "Z3_probe_dec_ref");
  in.register_cmd(437, exec_Z3_tactic_and_then, "Z3_tactic_and_then");
  in.register_cmd(438, exec_Z3_tactic_or_else, "Z3_tactic_or_else");
  in.register_cmd(439, exec_Z3_tactic_par_or, "Z3_tactic_par_or");
  in.register_cmd(440, exec_Z3_tactic_par_and_then, "Z3_tactic_par_and_then");
  in.register_cmd(441, exec_Z3_tactic_try_for, "Z3_tactic_try_for");
  in.register_cmd(442, exec_Z3_tactic_when, "Z3_tactic_when");
  in.register_cmd(443, exec_Z3_tactic_cond, "Z3_tactic_cond");
  in.register_cmd(444, exec_Z3_tactic_repeat, "Z3_tactic_repeat");
  in.register_cmd(445, exec_Z3_tactic_skip, "Z3_tactic_skip");
  in.register_cmd(446, exec_Z3_tactic_fail, "Z3_tactic_fail");
  in.register_cmd(447, exec_Z3_tactic_fail_if, "Z3_tactic_fail_if");
  in.register_cmd(448, exec_Z3_tactic_fail_if_not_decided, "Z3_tactic_fail_if_not_decided");
  in.register_cmd(449, exec_Z3_tactic_using_params, "Z3_tactic_using_params");
  in.register_cmd(450, exec_Z3_mk_simplifier, "Z3_mk_simplifier");
  in.register_cmd(451, exec_Z3_simplifier_inc_ref, "Z3_simplifier_inc_ref");
  in.register_cmd(452, exec_Z3_simplifier_dec_ref, "Z3_simplifier_dec_ref");
  in.register_cmd(453, exec_Z3_solver_add_simplifier, "Z3_solver_add_simplifier");
  in.register_cmd(454, exec_Z3_simplifier_and_then, "Z3_simplifier_and_then");
  in.register_cmd(455, exec_Z3_simplifier_using_params, "Z3_simplifier_using_params");
  in.register_cmd(456, exec_Z3_get_num_simplifiers, "Z3_get_num_simplifiers");
  in.register_cmd(457, exec_Z3_get_simplifier_name, "Z3_get_simplifier_name");
  in.register_cmd(458, exec_Z3_simplifier_get_help, "Z3_simplifier_get_help");
  in.register_cmd(459, exec_Z3_simplifier_get_param_descrs, "Z3_simplifier_get_param_descrs");
  in.register_cmd(460, exec_Z3_simplifier_get_descr, "Z3_simplifier_get_descr");
  in.register_cmd(461, exec_Z3_probe_const, "Z3_probe_const");
  in.register_cmd(462, exec_Z3_probe_lt, "Z3_probe_lt");
  in.register_cmd(463, exec_Z3_probe_gt, "Z3_probe_gt");
  in.register_cmd(464, exec_Z3_probe_le, "Z3_probe_le");
  in.register_cmd(465, exec_Z3_probe_ge, "Z3_probe_ge");
  in.register_cmd(466, exec_Z3_probe_eq, "Z3_probe_eq");
  in.register_cmd(467, exec_Z3_probe_and, "Z3_probe_and");
  in.register_cmd(468, exec_Z3_probe_or, "Z3_probe_or");
  in.register_cmd(469, exec_Z3_probe_not, "Z3_probe_not");
  in.register_cmd(470, exec_Z3_get_num_tactics, "Z3_get_num_tactics");
  in.register_cmd(471, exec_Z3_get_tactic_name, "Z3_get_tactic_name");
  in.register_cmd(472, exec_Z3_get_num_probes, "Z3_get_num_probes");
  in.register_cmd(473, exec_Z3_get_probe_name, "Z3_get_probe_name");
  in.register_cmd(474, exec_Z3_tactic_get_help, "Z3_tactic_get_help");
  in.register_cmd(475, exec_Z3_tactic_get_param_descrs, "Z3_tactic_get_param_descrs");
  in.register_cmd(476, exec_Z3_tactic_get_descr, "Z3_tactic_get_descr");
  in.register_cmd(477, exec_Z3_probe_get_descr, "Z3_probe_get_descr");
  in.register_cmd(478, exec_Z3_probe_apply, "Z3_probe_apply");
  in.register_cmd(479, exec_Z3_tactic_apply, "Z3_tactic_apply");
  in.register_cmd(480, exec_Z3_tactic_apply_ex, "Z3_tactic_apply_ex");
  in.register_cmd(481, exec_Z3_apply_result_inc_ref, "Z3_apply_result_inc_ref");
  in.register_cmd(482, exec_Z3_apply_result_dec_ref, "Z3_apply_result_dec_ref");
  in.register_cmd(483, exec_Z3_apply_result_to_string, "Z3_apply_result_to_string");
  in.register_cmd(484, exec_Z3_apply_result_get_num_subgoals, "Z3_apply_result_get_num_subgoals");
  in.register_cmd(485, exec_Z3_apply_result_get_subgoal, "Z3_apply_result_get_subgoal");
  in.register_cmd(486, exec_Z3_mk_solver, "Z3_mk_solver");
  in.register_cmd(487, exec_Z3_mk_simple_solver, "Z3_mk_simple_solver");
  in.register_cmd(488, exec_Z3_mk_solver_for_logic, "Z3_mk_solver_for_logic");
  in.register_cmd(489, exec_Z3_mk_solver_from_tactic, "Z3_mk_solver_from_tactic");
  in.register_cmd(490, exec_Z3_solver_translate, "Z3_solver_translate");
  in.register_cmd(491, exec_Z3_solver_import_model_converter, "Z3_solver_import_model_converter");
  in.register_cmd(492, exec_Z3_solver_get_help, "Z3_solver_get_help");
  in.register_cmd(493, exec_Z3_solver_get_param_descrs, "Z3_solver_get_param_descrs");
  in.register_cmd(494, exec_Z3_solver_set_params, "Z3_solver_set_params");
  in.register_cmd(495, exec_Z3_solver_inc_ref, "Z3_solver_inc_ref");
  in.register_cmd(496, exec_Z3_solver_dec_ref, "Z3_solver_dec_ref");
  in.register_cmd(497, exec_Z3_solver_interrupt, "Z3_solver_interrupt");
  in.register_cmd(498, exec_Z3_solver_push, "Z3_solver_push");
  in.register_cmd(499, exec_Z3_solver_pop, "Z3_solver_pop");
  in.register_cmd(500, exec_Z3_solver_reset, "Z3_solver_reset");
  in.register_cmd(501, exec_Z3_solver_get_num_scopes, "Z3_solver_get_num_scopes");
  in.register_cmd(502, exec_Z3_solver_assert, "Z3_solver_assert");
  in.register_cmd(503, exec_Z3_solver_assert_and_track, "Z3_solver_assert_and_track");
  in.register_cmd(504, exec_Z3_solver_from_file, "Z3_solver_from_file");
  in.register_cmd(505, exec_Z3_solver_from_string, "Z3_solver_from_string");
  in.register_cmd(506, exec_Z3_solver_get_assertions, "Z3_solver_get_assertions");
  in.register_cmd(507, exec_Z3_solver_get_units, "Z3_solver_get_units");
  in.register_cmd(508, exec_Z3_solver_get_trail, "Z3_solver_get_trail");
  in.register_cmd(509, exec_Z3_solver_get_non_units, "Z3_solver_get_non_units");
  in.register_cmd(510, exec_Z3_solver_get_levels, "Z3_solver_get_levels");
  in.register_cmd(511, exec_Z3_solver_congruence_root, "Z3_solver_congruence_root");
  in.register_cmd(512, exec_Z3_solver_congruence_next, "Z3_solver_congruence_next");
  in.register_cmd(513, exec_Z3_solver_congruence_explain, "Z3_solver_congruence_explain");
  in.register_cmd(514, exec_Z3_solver_solve_for, "Z3_solver_solve_for");
  in.register_cmd(515, exec_Z3_solver_register_on_clause, "Z3_solver_register_on_clause");
  in.register_cmd(516, exec_Z3_solver_propagate_init, "Z3_solver_propagate_init");
  in.register_cmd(517, exec_Z3_solver_propagate_fixed, "Z3_solver_propagate_fixed");
  in.register_cmd(518, exec_Z3_solver_propagate_final, "Z3_solver_propagate_final");
  in.register_cmd(519, exec_Z3_solver_propagate_eq, "Z3_solver_propagate_eq");
  in.register_cmd(520, exec_Z3_solver_propagate_diseq, "Z3_solver_propagate_diseq");
  in.register_cmd(521, exec_Z3_solver_propagate_created, "Z3_solver_propagate_created");
  in.register_cmd(522, exec_Z3_solver_propagate_decide, "Z3_solver_propagate_decide");
  in.register_cmd(523, exec_Z3_solver_next_split, "Z3_solver_next_split");
  in.register_cmd(524, exec_Z3_solver_propagate_declare, "Z3_solver_propagate_declare");
  in.register_cmd(525, exec_Z3_solver_propagate_register, "Z3_solver_propagate_register");
  in.register_cmd(526, exec_Z3_solver_propagate_register_cb, "Z3_solver_propagate_register_cb");
  in.register_cmd(527, exec_Z3_solver_propagate_consequence, "Z3_solver_propagate_consequence");
  in.register_cmd(528, exec_Z3_solver_set_initial_value, "Z3_solver_set_initial_value");
  in.register_cmd(529, exec_Z3_solver_check, "Z3_solver_check");
  in.register_cmd(530, exec_Z3_solver_check_assumptions, "Z3_solver_check_assumptions");
  in.register_cmd(531, exec_Z3_get_implied_equalities, "Z3_get_implied_equalities");
  in.register_cmd(532, exec_Z3_solver_get_consequences, "Z3_solver_get_consequences");
  in.register_cmd(533, exec_Z3_solver_cube, "Z3_solver_cube");
  in.register_cmd(534, exec_Z3_solver_get_model, "Z3_solver_get_model");
  in.register_cmd(535, exec_Z3_solver_get_proof, "Z3_solver_get_proof");
  in.register_cmd(536, exec_Z3_solver_get_unsat_core, "Z3_solver_get_unsat_core");
  in.register_cmd(537, exec_Z3_solver_get_reason_unknown, "Z3_solver_get_reason_unknown");
  in.register_cmd(538, exec_Z3_solver_get_statistics, "Z3_solver_get_statistics");
  in.register_cmd(539, exec_Z3_solver_to_string, "Z3_solver_to_string");
  in.register_cmd(540, exec_Z3_solver_to_dimacs_string, "Z3_solver_to_dimacs_string");
  in.register_cmd(541, exec_Z3_stats_to_string, "Z3_stats_to_string");
  in.register_cmd(542, exec_Z3_stats_inc_ref, "Z3_stats_inc_ref");
  in.register_cmd(543, exec_Z3_stats_dec_ref, "Z3_stats_dec_ref");
  in.register_cmd(544, exec_Z3_stats_size, "Z3_stats_size");
  in.register_cmd(545, exec_Z3_stats_get_key, "Z3_stats_get_key");
  in.register_cmd(546, exec_Z3_stats_is_uint, "Z3_stats_is_uint");
  in.register_cmd(547, exec_Z3_stats_is_double, "Z3_stats_is_double");
  in.register_cmd(548, exec_Z3_stats_get_uint_value, "Z3_stats_get_uint_value");
  in.register_cmd(549, exec_Z3_stats_get_double_value, "Z3_stats_get_double_value");
  in.register_cmd(550, exec_Z3_get_estimated_alloc_size, "Z3_get_estimated_alloc_size");
  in.register_cmd(551, exec_Z3_mk_ast_vector, "Z3_mk_ast_vector");
  in.register_cmd(552, exec_Z3_ast_vector_inc_ref, "Z3_ast_vector_inc_ref");
  in.register_cmd(553, exec_Z3_ast_vector_dec_ref, "Z3_ast_vector_dec_ref");
  in.register_cmd(554, exec_Z3_ast_vector_size, "Z3_ast_vector_size");
  in.register_cmd(555, exec_Z3_ast_vector_get, "Z3_ast_vector_get");
  in.register_cmd(556, exec_Z3_ast_vector_set, "Z3_ast_vector_set");
  in.register_cmd(557, exec_Z3_ast_vector_resize, "Z3_ast_vector_resize");
  in.register_cmd(558, exec_Z3_ast_vector_push, "Z3_ast_vector_push");
  in.register_cmd(559, exec_Z3_ast_vector_translate, "Z3_ast_vector_translate");
  in.register_cmd(560, exec_Z3_ast_vector_to_string, "Z3_ast_vector_to_string");
  in.register_cmd(561, exec_Z3_mk_ast_map, "Z3_mk_ast_map");
  in.register_cmd(562, exec_Z3_ast_map_inc_ref, "Z3_ast_map_inc_ref");
  in.register_cmd(563, exec_Z3_ast_map_dec_ref, "Z3_ast_map_dec_ref");
  in.register_cmd(564, exec_Z3_ast_map_contains, "Z3_ast_map_contains");
  in.register_cmd(565, exec_Z3_ast_map_find, "Z3_ast_map_find");
  in.register_cmd(566, exec_Z3_ast_map_insert, "Z3_ast_map_insert");
  in.register_cmd(567, exec_Z3_ast_map_erase, "Z3_ast_map_erase");
  in.register_cmd(568, exec_Z3_ast_map_reset, "Z3_ast_map_reset");
  in.register_cmd(569, exec_Z3_ast_map_size, "Z3_ast_map_size");
  in.register_cmd(570, exec_Z3_ast_map_keys, "Z3_ast_map_keys");
  in.register_cmd(571, exec_Z3_ast_map_to_string, "Z3_ast_map_to_string");
  in.register_cmd(572, exec_Z3_algebraic_is_value, "Z3_algebraic_is_value");
  in.register_cmd(573, exec_Z3_algebraic_is_pos, "Z3_algebraic_is_pos");
  in.register_cmd(574, exec_Z3_algebraic_is_neg, "Z3_algebraic_is_neg");
  in.register_cmd(575, exec_Z3_algebraic_is_zero, "Z3_algebraic_is_zero");
  in.register_cmd(576, exec_Z3_algebraic_sign, "Z3_algebraic_sign");
  in.register_cmd(577, exec_Z3_algebraic_add, "Z3_algebraic_add");
  in.register_cmd(578, exec_Z3_algebraic_sub, "Z3_algebraic_sub");
  in.register_cmd(579, exec_Z3_algebraic_mul, "Z3_algebraic_mul");
  in.register_cmd(580, exec_Z3_algebraic_div, "Z3_algebraic_div");
  in.register_cmd(581, exec_Z3_algebraic_root, "Z3_algebraic_root");
  in.register_cmd(582, exec_Z3_algebraic_power, "Z3_algebraic_power");
  in.register_cmd(583, exec_Z3_algebraic_lt, "Z3_algebraic_lt");
  in.register_cmd(584, exec_Z3_algebraic_gt, "Z3_algebraic_gt");
  in.register_cmd(585, exec_Z3_algebraic_le, "Z3_algebraic_le");
  in.register_cmd(586, exec_Z3_algebraic_ge, "Z3_algebraic_ge");
  in.register_cmd(587, exec_Z3_algebraic_eq, "Z3_algebraic_eq");
  in.register_cmd(588, exec_Z3_algebraic_neq, "Z3_algebraic_neq");
  in.register_cmd(589, exec_Z3_algebraic_roots, "Z3_algebraic_roots");
  in.register_cmd(590, exec_Z3_algebraic_eval, "Z3_algebraic_eval");
  in.register_cmd(591, exec_Z3_algebraic_get_poly, "Z3_algebraic_get_poly");
  in.register_cmd(592, exec_Z3_algebraic_get_i, "Z3_algebraic_get_i");
  in.register_cmd(593, exec_Z3_polynomial_subresultants, "Z3_polynomial_subresultants");
  in.register_cmd(594, exec_Z3_rcf_del, "Z3_rcf_del");
  in.register_cmd(595, exec_Z3_rcf_mk_rational, "Z3_rcf_mk_rational");
  in.register_cmd(596, exec_Z3_rcf_mk_small_int, "Z3_rcf_mk_small_int");
  in.register_cmd(597, exec_Z3_rcf_mk_pi, "Z3_rcf_mk_pi");
  in.register_cmd(598, exec_Z3_rcf_mk_e, "Z3_rcf_mk_e");
  in.register_cmd(599, exec_Z3_rcf_mk_infinitesimal, "Z3_rcf_mk_infinitesimal");
  in.register_cmd(600, exec_Z3_rcf_mk_roots, "Z3_rcf_mk_roots");
  in.register_cmd(601, exec_Z3_rcf_add, "Z3_rcf_add");
  in.register_cmd(602, exec_Z3_rcf_sub, "Z3_rcf_sub");
  in.register_cmd(603, exec_Z3_rcf_mul, "Z3_rcf_mul");
  in.register_cmd(604, exec_Z3_rcf_div, "Z3_rcf_div");
  in.register_cmd(605, exec_Z3_rcf_neg, "Z3_rcf_neg");
  in.register_cmd(606, exec_Z3_rcf_inv, "Z3_rcf_inv");
  in.register_cmd(607, exec_Z3_rcf_power, "Z3_rcf_power");
  in.register_cmd(608, exec_Z3_rcf_lt, "Z3_rcf_lt");
  in.register_cmd(609, exec_Z3_rcf_gt, "Z3_rcf_gt");
  in.register_cmd(610, exec_Z3_rcf_le, "Z3_rcf_le");
  in.register_cmd(611, exec_Z3_rcf_ge, "Z3_rcf_ge");
  in.register_cmd(612, exec_Z3_rcf_eq, "Z3_rcf_eq");
  in.register_cmd(613, exec_Z3_rcf_neq, "Z3_rcf_neq");
  in.register_cmd(614, exec_Z3_rcf_num_to_string, "Z3_rcf_num_to_string");
  in.register_cmd(615, exec_Z3_rcf_num_to_decimal_string, "Z3_rcf_num_to_decimal_string");
  in.register_cmd(616, exec_Z3_rcf_get_numerator_denominator, "Z3_rcf_get_numerator_denominator");
  in.register_cmd(617, exec_Z3_rcf_is_rational, "Z3_rcf_is_rational");
  in.register_cmd(618, exec_Z3_rcf_is_algebraic, "Z3_rcf_is_algebraic");
  in.register_cmd(619, exec_Z3_rcf_is_infinitesimal, "Z3_rcf_is_infinitesimal");
  in.register_cmd(620, exec_Z3_rcf_is_transcendental, "Z3_rcf_is_transcendental");
  in.register_cmd(621, exec_Z3_rcf_extension_index, "Z3_rcf_extension_index");
  in.register_cmd(622, exec_Z3_rcf_transcendental_name, "Z3_rcf_transcendental_name");
  in.register_cmd(623, exec_Z3_rcf_infinitesimal_name, "Z3_rcf_infinitesimal_name");
  in.register_cmd(624, exec_Z3_rcf_num_coefficients, "Z3_rcf_num_coefficients");
  in.register_cmd(625, exec_Z3_rcf_coefficient, "Z3_rcf_coefficient");
  in.register_cmd(626, exec_Z3_rcf_interval, "Z3_rcf_interval");
  in.register_cmd(627, exec_Z3_rcf_num_sign_conditions, "Z3_rcf_num_sign_conditions");
  in.register_cmd(628, exec_Z3_rcf_sign_condition_sign, "Z3_rcf_sign_condition_sign");
  in.register_cmd(629, exec_Z3_rcf_num_sign_condition_coefficients, "Z3_rcf_num_sign_condition_coefficients");
  in.register_cmd(630, exec_Z3_rcf_sign_condition_coefficient, "Z3_rcf_sign_condition_coefficient");
  in.register_cmd(631, exec_Z3_mk_fixedpoint, "Z3_mk_fixedpoint");
  in.register_cmd(632, exec_Z3_fixedpoint_inc_ref, "Z3_fixedpoint_inc_ref");
  in.register_cmd(633, exec_Z3_fixedpoint_dec_ref, "Z3_fixedpoint_dec_ref");
  in.register_cmd(634, exec_Z3_fixedpoint_add_rule, "Z3_fixedpoint_add_rule");
  in.register_cmd(635, exec_Z3_fixedpoint_add_fact, "Z3_fixedpoint_add_fact");
  in.register_cmd(636, exec_Z3_fixedpoint_assert, "Z3_fixedpoint_assert");
  in.register_cmd(637, exec_Z3_fixedpoint_query, "Z3_fixedpoint_query");
  in.register_cmd(638, exec_Z3_fixedpoint_query_relations, "Z3_fixedpoint_query_relations");
  in.register_cmd(639, exec_Z3_fixedpoint_get_answer, "Z3_fixedpoint_get_answer");
  in.register_cmd(640, exec_Z3_fixedpoint_get_reason_unknown, "Z3_fixedpoint_get_reason_unknown");
  in.register_cmd(641, exec_Z3_fixedpoint_update_rule, "Z3_fixedpoint_update_rule");
  in.register_cmd(642, exec_Z3_fixedpoint_get_num_levels, "Z3_fixedpoint_get_num_levels");
  in.register_cmd(643, exec_Z3_fixedpoint_get_cover_delta, "Z3_fixedpoint_get_cover_delta");
  in.register_cmd(644, exec_Z3_fixedpoint_add_cover, "Z3_fixedpoint_add_cover");
  in.register_cmd(645, exec_Z3_fixedpoint_get_statistics, "Z3_fixedpoint_get_statistics");
  in.register_cmd(646, exec_Z3_fixedpoint_register_relation, "Z3_fixedpoint_register_relation");
  in.register_cmd(647, exec_Z3_fixedpoint_set_predicate_representation, "Z3_fixedpoint_set_predicate_representation");
  in.register_cmd(648, exec_Z3_fixedpoint_get_rules, "Z3_fixedpoint_get_rules");
  in.register_cmd(649, exec_Z3_fixedpoint_get_assertions, "Z3_fixedpoint_get_assertions");
  in.register_cmd(650, exec_Z3_fixedpoint_set_params, "Z3_fixedpoint_set_params");
  in.register_cmd(651, exec_Z3_fixedpoint_get_help, "Z3_fixedpoint_get_help");
  in.register_cmd(652, exec_Z3_fixedpoint_get_param_descrs, "Z3_fixedpoint_get_param_descrs");
  in.register_cmd(653, exec_Z3_fixedpoint_to_string, "Z3_fixedpoint_to_string");
  in.register_cmd(654, exec_Z3_fixedpoint_from_string, "Z3_fixedpoint_from_string");
  in.register_cmd(655, exec_Z3_fixedpoint_from_file, "Z3_fixedpoint_from_file");
  in.register_cmd(656, exec_Z3_mk_optimize, "Z3_mk_optimize");
  in.register_cmd(657, exec_Z3_optimize_inc_ref, "Z3_optimize_inc_ref");
  in.register_cmd(658, exec_Z3_optimize_dec_ref, "Z3_optimize_dec_ref");
  in.register_cmd(659, exec_Z3_optimize_assert, "Z3_optimize_assert");
  in.register_cmd(660, exec_Z3_optimize_assert_and_track, "Z3_optimize_assert_and_track");
  in.register_cmd(661, exec_Z3_optimize_assert_soft, "Z3_optimize_assert_soft");
  in.register_cmd(662, exec_Z3_optimize_maximize, "Z3_optimize_maximize");
  in.register_cmd(663, exec_Z3_optimize_minimize, "Z3_optimize_minimize");
  in.register_cmd(664, exec_Z3_optimize_push, "Z3_optimize_push");
  in.register_cmd(665, exec_Z3_optimize_pop, "Z3_optimize_pop");
  in.register_cmd(666, exec_Z3_optimize_set_initial_value, "Z3_optimize_set_initial_value");
  in.register_cmd(667, exec_Z3_optimize_check, "Z3_optimize_check");
  in.register_cmd(668, exec_Z3_optimize_get_reason_unknown, "Z3_optimize_get_reason_unknown");
  in.register_cmd(669, exec_Z3_optimize_get_model, "Z3_optimize_get_model");
  in.register_cmd(670, exec_Z3_optimize_get_unsat_core, "Z3_optimize_get_unsat_core");
  in.register_cmd(671, exec_Z3_optimize_set_params, "Z3_optimize_set_params");
  in.register_cmd(672, exec_Z3_optimize_get_param_descrs, "Z3_optimize_get_param_descrs");
  in.register_cmd(673, exec_Z3_optimize_get_lower, "Z3_optimize_get_lower");
  in.register_cmd(674, exec_Z3_optimize_get_upper, "Z3_optimize_get_upper");
  in.register_cmd(675, exec_Z3_optimize_get_lower_as_vector, "Z3_optimize_get_lower_as_vector");
  in.register_cmd(676, exec_Z3_optimize_get_upper_as_vector, "Z3_optimize_get_upper_as_vector");
  in.register_cmd(677, exec_Z3_optimize_to_string, "Z3_optimize_to_string");
  in.register_cmd(678, exec_Z3_optimize_from_string, "Z3_optimize_from_string");
  in.register_cmd(679, exec_Z3_optimize_from_file, "Z3_optimize_from_file");
  in.register_cmd(680, exec_Z3_optimize_get_help, "Z3_optimize_get_help");
  in.register_cmd(681, exec_Z3_optimize_get_statistics, "Z3_optimize_get_statistics");
  in.register_cmd(682, exec_Z3_optimize_get_assertions, "Z3_optimize_get_assertions");
  in.register_cmd(683, exec_Z3_optimize_get_objectives, "Z3_optimize_get_objectives");
  in.register_cmd(684, exec_Z3_mk_fpa_rounding_mode_sort, "Z3_mk_fpa_rounding_mode_sort");
  in.register_cmd(685, exec_Z3_mk_fpa_round_nearest_ties_to_even, "Z3_mk_fpa_round_nearest_ties_to_even");
  in.register_cmd(686, exec_Z3_mk_fpa_rne, "Z3_mk_fpa_rne");
  in.register_cmd(687, exec_Z3_mk_fpa_round_nearest_ties_to_away, "Z3_mk_fpa_round_nearest_ties_to_away");
  in.register_cmd(688, exec_Z3_mk_fpa_rna, "Z3_mk_fpa_rna");
  in.register_cmd(689, exec_Z3_mk_fpa_round_toward_positive, "Z3_mk_fpa_round_toward_positive");
  in.register_cmd(690, exec_Z3_mk_fpa_rtp, "Z3_mk_fpa_rtp");
  in.register_cmd(691, exec_Z3_mk_fpa_round_toward_negative, "Z3_mk_fpa_round_toward_negative");
  in.register_cmd(692, exec_Z3_mk_fpa_rtn, "Z3_mk_fpa_rtn");
  in.register_cmd(693, exec_Z3_mk_fpa_round_toward_zero, "Z3_mk_fpa_round_toward_zero");
  in.register_cmd(694, exec_Z3_mk_fpa_rtz, "Z3_mk_fpa_rtz");
  in.register_cmd(695, exec_Z3_mk_fpa_sort, "Z3_mk_fpa_sort");
  in.register_cmd(696, exec_Z3_mk_fpa_sort_half, "Z3_mk_fpa_sort_half");
  in.register_cmd(697, exec_Z3_mk_fpa_sort_16, "Z3_mk_fpa_sort_16");
  in.register_cmd(698, exec_Z3_mk_fpa_sort_single, "Z3_mk_fpa_sort_single");
  in.register_cmd(699, exec_Z3_mk_fpa_sort_32, "Z3_mk_fpa_sort_32");
  in.register_cmd(700, exec_Z3_mk_fpa_sort_double, "Z3_mk_fpa_sort_double");
  in.register_cmd(701, exec_Z3_mk_fpa_sort_64, "Z3_mk_fpa_sort_64");
  in.register_cmd(702, exec_Z3_mk_fpa_sort_quadruple, "Z3_mk_fpa_sort_quadruple");
  in.register_cmd(703, exec_Z3_mk_fpa_sort_128, "Z3_mk_fpa_sort_128");
  in.register_cmd(704, exec_Z3_mk_fpa_nan, "Z3_mk_fpa_nan");
  in.register_cmd(705, exec_Z3_mk_fpa_inf, "Z3_mk_fpa_inf");
  in.register_cmd(706, exec_Z3_mk_fpa_zero, "Z3_mk_fpa_zero");
  in.register_cmd(707, exec_Z3_mk_fpa_fp, "Z3_mk_fpa_fp");
  in.register_cmd(708, exec_Z3_mk_fpa_numeral_float, "Z3_mk_fpa_numeral_float");
  in.register_cmd(709, exec_Z3_mk_fpa_numeral_double, "Z3_mk_fpa_numeral_double");
  in.register_cmd(710, exec_Z3_mk_fpa_numeral_int, "Z3_mk_fpa_numeral_int");
  in.register_cmd(711, exec_Z3_mk_fpa_numeral_int_uint, "Z3_mk_fpa_numeral_int_uint");
  in.register_cmd(712, exec_Z3_mk_fpa_numeral_int64_uint64, "Z3_mk_fpa_numeral_int64_uint64");
  in.register_cmd(713, exec_Z3_mk_fpa_abs, "Z3_mk_fpa_abs");
  in.register_cmd(714, exec_Z3_mk_fpa_neg, "Z3_mk_fpa_neg");
  in.register_cmd(715, exec_Z3_mk_fpa_add, "Z3_mk_fpa_add");
  in.register_cmd(716, exec_Z3_mk_fpa_sub, "Z3_mk_fpa_sub");
  in.register_cmd(717, exec_Z3_mk_fpa_mul, "Z3_mk_fpa_mul");
  in.register_cmd(718, exec_Z3_mk_fpa_div, "Z3_mk_fpa_div");
  in.register_cmd(719, exec_Z3_mk_fpa_fma, "Z3_mk_fpa_fma");
  in.register_cmd(720, exec_Z3_mk_fpa_sqrt, "Z3_mk_fpa_sqrt");
  in.register_cmd(721, exec_Z3_mk_fpa_rem, "Z3_mk_fpa_rem");
  in.register_cmd(722, exec_Z3_mk_fpa_round_to_integral, "Z3_mk_fpa_round_to_integral");
  in.register_cmd(723, exec_Z3_mk_fpa_min, "Z3_mk_fpa_min");
  in.register_cmd(724, exec_Z3_mk_fpa_max, "Z3_mk_fpa_max");
  in.register_cmd(725, exec_Z3_mk_fpa_leq, "Z3_mk_fpa_leq");
  in.register_cmd(726, exec_Z3_mk_fpa_lt, "Z3_mk_fpa_lt");
  in.register_cmd(727, exec_Z3_mk_fpa_geq, "Z3_mk_fpa_geq");
  in.register_cmd(728, exec_Z3_mk_fpa_gt, "Z3_mk_fpa_gt");
  in.register_cmd(729, exec_Z3_mk_fpa_eq, "Z3_mk_fpa_eq");
  in.register_cmd(730, exec_Z3_mk_fpa_is_normal, "Z3_mk_fpa_is_normal");
  in.register_cmd(731, exec_Z3_mk_fpa_is_subnormal, "Z3_mk_fpa_is_subnormal");
  in.register_cmd(732, exec_Z3_mk_fpa_is_zero, "Z3_mk_fpa_is_zero");
  in.register_cmd(733, exec_Z3_mk_fpa_is_infinite, "Z3_mk_fpa_is_infinite");
  in.register_cmd(734, exec_Z3_mk_fpa_is_nan, "Z3_mk_fpa_is_nan");
  in.register_cmd(735, exec_Z3_mk_fpa_is_negative, "Z3_mk_fpa_is_negative");
  in.register_cmd(736, exec_Z3_mk_fpa_is_positive, "Z3_mk_fpa_is_positive");
  in.register_cmd(737, exec_Z3_mk_fpa_to_fp_bv, "Z3_mk_fpa_to_fp_bv");
  in.register_cmd(738, exec_Z3_mk_fpa_to_fp_float, "Z3_mk_fpa_to_fp_float");
  in.register_cmd(739, exec_Z3_mk_fpa_to_fp_real, "Z3_mk_fpa_to_fp_real");
  in.register_cmd(740, exec_Z3_mk_fpa_to_fp_signed, "Z3_mk_fpa_to_fp_signed");
  in.register_cmd(741, exec_Z3_mk_fpa_to_fp_unsigned, "Z3_mk_fpa_to_fp_unsigned");
  in.register_cmd(742, exec_Z3_mk_fpa_to_ubv, "Z3_mk_fpa_to_ubv");
  in.register_cmd(743, exec_Z3_mk_fpa_to_sbv, "Z3_mk_fpa_to_sbv");
  in.register_cmd(744, exec_Z3_mk_fpa_to_real, "Z3_mk_fpa_to_real");
  in.register_cmd(745, exec_Z3_fpa_get_ebits, "Z3_fpa_get_ebits");
  in.register_cmd(746, exec_Z3_fpa_get_sbits, "Z3_fpa_get_sbits");
  in.register_cmd(747, exec_Z3_fpa_is_numeral_nan, "Z3_fpa_is_numeral_nan");
  in.register_cmd(748, exec_Z3_fpa_is_numeral_inf, "Z3_fpa_is_numeral_inf");
  in.register_cmd(749, exec_Z3_fpa_is_numeral_zero, "Z3_fpa_is_numeral_zero");
  in.register_cmd(750, exec_Z3_fpa_is_numeral_normal, "Z3_fpa_is_numeral_normal");
  in.register_cmd(751, exec_Z3_fpa_is_numeral_subnormal, "Z3_fpa_is_numeral_subnormal");
  in.register_cmd(752, exec_Z3_fpa_is_numeral_positive, "Z3_fpa_is_numeral_positive");
  in.register_cmd(753, exec_Z3_fpa_is_numeral_negative, "Z3_fpa_is_numeral_negative");
  in.register_cmd(754, exec_Z3_fpa_get_numeral_sign_bv, "Z3_fpa_get_numeral_sign_bv");
  in.register_cmd(755, exec_Z3_fpa_get_numeral_significand_bv, "Z3_fpa_get_numeral_significand_bv");
  in.register_cmd(756, exec_Z3_fpa_get_numeral_sign, "Z3_fpa_get_numeral_sign");
  in.register_cmd(757, exec_Z3_fpa_get_numeral_significand_string, "Z3_fpa_get_numeral_significand_string");
  in.register_cmd(758, exec_Z3_fpa_get_numeral_significand_uint64, "Z3_fpa_get_numeral_significand_uint64");
  in.register_cmd(759, exec_Z3_fpa_get_numeral_exponent_string, "Z3_fpa_get_numeral_exponent_string");
  in.register_cmd(760, exec_Z3_fpa_get_numeral_exponent_int64, "Z3_fpa_get_numeral_exponent_int64");
  in.register_cmd(761, exec_Z3_fpa_get_numeral_exponent_bv, "Z3_fpa_get_numeral_exponent_bv");
  in.register_cmd(762, exec_Z3_mk_fpa_to_ieee_bv, "Z3_mk_fpa_to_ieee_bv");
  in.register_cmd(763, exec_Z3_mk_fpa_to_fp_int_real, "Z3_mk_fpa_to_fp_int_real");
  in.register_cmd(764, exec_Z3_fixedpoint_query_from_lvl, "Z3_fixedpoint_query_from_lvl");
  in.register_cmd(765, exec_Z3_fixedpoint_get_ground_sat_answer, "Z3_fixedpoint_get_ground_sat_answer");
  in.register_cmd(766, exec_Z3_fixedpoint_get_rules_along_trace, "Z3_fixedpoint_get_rules_along_trace");
  in.register_cmd(767, exec_Z3_fixedpoint_get_rule_names_along_trace, "Z3_fixedpoint_get_rule_names_along_trace");
  in.register_cmd(768, exec_Z3_fixedpoint_add_invariant, "Z3_fixedpoint_add_invariant");
  in.register_cmd(769, exec_Z3_fixedpoint_get_reachable, "Z3_fixedpoint_get_reachable");
  in.register_cmd(770, exec_Z3_qe_model_project, "Z3_qe_model_project");
  in.register_cmd(771, exec_Z3_qe_model_project_skolem, "Z3_qe_model_project_skolem");
  in.register_cmd(772, exec_Z3_qe_model_project_with_witness, "Z3_qe_model_project_with_witness");
  in.register_cmd(773, exec_Z3_model_extrapolate, "Z3_model_extrapolate");
  in.register_cmd(774, exec_Z3_qe_lite, "Z3_qe_lite");
}

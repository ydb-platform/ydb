// Automatically generated file
#include"api/z3.h"
#include"api/api_log_macros.h"
#include"api/z3_logger.h"
void log_Z3_global_param_set(Z3_string a0, Z3_string a1) {
  R();
  S(a0);
  S(a1);
  C(0);
}
void log_Z3_global_param_reset_all() {
  R();
  C(1);
}
void log_Z3_global_param_get(Z3_string a0, Z3_string* a1) {
  R();
  S(a0);
  S("");
  C(2);
}
void log_Z3_mk_config() {
  R();
  C(3);
}
void log_Z3_del_config(Z3_config a0) {
  R();
  P(a0);
  C(4);
}
void log_Z3_set_param_value(Z3_config a0, Z3_string a1, Z3_string a2) {
  R();
  P(a0);
  S(a1);
  S(a2);
  C(5);
}
void log_Z3_mk_context(Z3_config a0) {
  R();
  P(a0);
  C(6);
}
void log_Z3_mk_context_rc(Z3_config a0) {
  R();
  P(a0);
  C(7);
}
void log_Z3_del_context(Z3_context a0) {
  R();
  P(a0);
  C(8);
}
void log_Z3_inc_ref(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(9);
}
void log_Z3_dec_ref(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(10);
}
void log_Z3_update_param_value(Z3_context a0, Z3_string a1, Z3_string a2) {
  R();
  P(a0);
  S(a1);
  S(a2);
  C(11);
}
void log_Z3_get_global_param_descrs(Z3_context a0) {
  R();
  P(a0);
  C(12);
}
void log_Z3_interrupt(Z3_context a0) {
  R();
  P(a0);
  C(13);
}
void log_Z3_enable_concurrent_dec_ref(Z3_context a0) {
  R();
  P(a0);
  C(14);
}
void log_Z3_mk_params(Z3_context a0) {
  R();
  P(a0);
  C(15);
}
void log_Z3_params_inc_ref(Z3_context a0, Z3_params a1) {
  R();
  P(a0);
  P(a1);
  C(16);
}
void log_Z3_params_dec_ref(Z3_context a0, Z3_params a1) {
  R();
  P(a0);
  P(a1);
  C(17);
}
void log_Z3_params_set_bool(Z3_context a0, Z3_params a1, Z3_symbol a2, bool a3) {
  R();
  P(a0);
  P(a1);
  Sy(a2);
  I(a3);
  C(18);
}
void log_Z3_params_set_uint(Z3_context a0, Z3_params a1, Z3_symbol a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  Sy(a2);
  U(a3);
  C(19);
}
void log_Z3_params_set_double(Z3_context a0, Z3_params a1, Z3_symbol a2, double a3) {
  R();
  P(a0);
  P(a1);
  Sy(a2);
  D(a3);
  C(20);
}
void log_Z3_params_set_symbol(Z3_context a0, Z3_params a1, Z3_symbol a2, Z3_symbol a3) {
  R();
  P(a0);
  P(a1);
  Sy(a2);
  Sy(a3);
  C(21);
}
void log_Z3_params_to_string(Z3_context a0, Z3_params a1) {
  R();
  P(a0);
  P(a1);
  C(22);
}
void log_Z3_params_validate(Z3_context a0, Z3_params a1, Z3_param_descrs a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(23);
}
void log_Z3_param_descrs_inc_ref(Z3_context a0, Z3_param_descrs a1) {
  R();
  P(a0);
  P(a1);
  C(24);
}
void log_Z3_param_descrs_dec_ref(Z3_context a0, Z3_param_descrs a1) {
  R();
  P(a0);
  P(a1);
  C(25);
}
void log_Z3_param_descrs_get_kind(Z3_context a0, Z3_param_descrs a1, Z3_symbol a2) {
  R();
  P(a0);
  P(a1);
  Sy(a2);
  C(26);
}
void log_Z3_param_descrs_size(Z3_context a0, Z3_param_descrs a1) {
  R();
  P(a0);
  P(a1);
  C(27);
}
void log_Z3_param_descrs_get_name(Z3_context a0, Z3_param_descrs a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(28);
}
void log_Z3_param_descrs_get_documentation(Z3_context a0, Z3_param_descrs a1, Z3_symbol a2) {
  R();
  P(a0);
  P(a1);
  Sy(a2);
  C(29);
}
void log_Z3_param_descrs_to_string(Z3_context a0, Z3_param_descrs a1) {
  R();
  P(a0);
  P(a1);
  C(30);
}
void log_Z3_mk_int_symbol(Z3_context a0, int a1) {
  R();
  P(a0);
  I(a1);
  C(31);
}
void log_Z3_mk_string_symbol(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(32);
}
void log_Z3_mk_uninterpreted_sort(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(33);
}
void log_Z3_mk_type_variable(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(34);
}
void log_Z3_mk_bool_sort(Z3_context a0) {
  R();
  P(a0);
  C(35);
}
void log_Z3_mk_int_sort(Z3_context a0) {
  R();
  P(a0);
  C(36);
}
void log_Z3_mk_real_sort(Z3_context a0) {
  R();
  P(a0);
  C(37);
}
void log_Z3_mk_bv_sort(Z3_context a0, unsigned a1) {
  R();
  P(a0);
  U(a1);
  C(38);
}
void log_Z3_mk_finite_domain_sort(Z3_context a0, Z3_symbol a1, uint64_t a2) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  C(39);
}
void log_Z3_mk_array_sort(Z3_context a0, Z3_sort a1, Z3_sort a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(40);
}
void log_Z3_mk_array_sort_n(Z3_context a0, unsigned a1, Z3_sort const * a2, Z3_sort a3) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  P(a3);
  C(41);
}
void log_Z3_mk_tuple_sort(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_symbol const * a3, Z3_sort const * a4, Z3_func_decl* a5, Z3_func_decl* a6) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { Sy(a3[i]); }
  Asy(a2);
  for (unsigned i = 0; i < a2; i++) { P(a4[i]); }
  Ap(a2);
  P(0);
  for (unsigned i = 0; i < a2; i++) { P(0); }
  Ap(a2);
  C(42);
}
void log_Z3_mk_enumeration_sort(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_symbol const * a3, Z3_func_decl* a4, Z3_func_decl* a5) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { Sy(a3[i]); }
  Asy(a2);
  for (unsigned i = 0; i < a2; i++) { P(0); }
  Ap(a2);
  for (unsigned i = 0; i < a2; i++) { P(0); }
  Ap(a2);
  C(43);
}
void log_Z3_mk_list_sort(Z3_context a0, Z3_symbol a1, Z3_sort a2, Z3_func_decl* a3, Z3_func_decl* a4, Z3_func_decl* a5, Z3_func_decl* a6, Z3_func_decl* a7, Z3_func_decl* a8) {
  R();
  P(a0);
  Sy(a1);
  P(a2);
  P(0);
  P(0);
  P(0);
  P(0);
  P(0);
  P(0);
  C(44);
}
void log_Z3_mk_constructor(Z3_context a0, Z3_symbol a1, Z3_symbol a2, unsigned a3, Z3_symbol const * a4, Z3_sort const * a5, unsigned const * a6) {
  R();
  P(a0);
  Sy(a1);
  Sy(a2);
  U(a3);
  for (unsigned i = 0; i < a3; i++) { Sy(a4[i]); }
  Asy(a3);
  for (unsigned i = 0; i < a3; i++) { P(a5[i]); }
  Ap(a3);
  for (unsigned i = 0; i < a3; i++) { U(a6[i]); }
  Au(a3);
  C(45);
}
void log_Z3_constructor_num_fields(Z3_context a0, Z3_constructor a1) {
  R();
  P(a0);
  P(a1);
  C(46);
}
void log_Z3_del_constructor(Z3_context a0, Z3_constructor a1) {
  R();
  P(a0);
  P(a1);
  C(47);
}
void log_Z3_mk_datatype(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_constructor* a3) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(48);
}
void log_Z3_mk_datatype_sort(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(49);
}
void log_Z3_mk_constructor_list(Z3_context a0, unsigned a1, Z3_constructor const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(50);
}
void log_Z3_del_constructor_list(Z3_context a0, Z3_constructor_list a1) {
  R();
  P(a0);
  P(a1);
  C(51);
}
void log_Z3_mk_datatypes(Z3_context a0, unsigned a1, Z3_symbol const * a2, Z3_sort* a3, Z3_constructor_list* a4) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { Sy(a2[i]); }
  Asy(a1);
  for (unsigned i = 0; i < a1; i++) { P(0); }
  Ap(a1);
  for (unsigned i = 0; i < a1; i++) { P(a4[i]); }
  Ap(a1);
  C(52);
}
void log_Z3_query_constructor(Z3_context a0, Z3_constructor a1, unsigned a2, Z3_func_decl* a3, Z3_func_decl* a4, Z3_func_decl* a5) {
  R();
  P(a0);
  P(a1);
  U(a2);
  P(0);
  P(0);
  for (unsigned i = 0; i < a2; i++) { P(0); }
  Ap(a2);
  C(53);
}
void log_Z3_mk_func_decl(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_sort const * a3, Z3_sort a4) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(54);
}
void log_Z3_mk_app(Z3_context a0, Z3_func_decl a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(55);
}
void log_Z3_mk_const(Z3_context a0, Z3_symbol a1, Z3_sort a2) {
  R();
  P(a0);
  Sy(a1);
  P(a2);
  C(56);
}
void log_Z3_mk_fresh_func_decl(Z3_context a0, Z3_string a1, unsigned a2, Z3_sort const * a3, Z3_sort a4) {
  R();
  P(a0);
  S(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(57);
}
void log_Z3_mk_fresh_const(Z3_context a0, Z3_string a1, Z3_sort a2) {
  R();
  P(a0);
  S(a1);
  P(a2);
  C(58);
}
void log_Z3_mk_rec_func_decl(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_sort const * a3, Z3_sort a4) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(59);
}
void log_Z3_add_rec_def(Z3_context a0, Z3_func_decl a1, unsigned a2, Z3_ast const * a3, Z3_ast a4) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(60);
}
void log_Z3_mk_true(Z3_context a0) {
  R();
  P(a0);
  C(61);
}
void log_Z3_mk_false(Z3_context a0) {
  R();
  P(a0);
  C(62);
}
void log_Z3_mk_eq(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(63);
}
void log_Z3_mk_distinct(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(64);
}
void log_Z3_mk_not(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(65);
}
void log_Z3_mk_ite(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(66);
}
void log_Z3_mk_iff(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(67);
}
void log_Z3_mk_implies(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(68);
}
void log_Z3_mk_xor(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(69);
}
void log_Z3_mk_and(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(70);
}
void log_Z3_mk_or(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(71);
}
void log_Z3_mk_add(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(72);
}
void log_Z3_mk_mul(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(73);
}
void log_Z3_mk_sub(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(74);
}
void log_Z3_mk_unary_minus(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(75);
}
void log_Z3_mk_div(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(76);
}
void log_Z3_mk_mod(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(77);
}
void log_Z3_mk_rem(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(78);
}
void log_Z3_mk_power(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(79);
}
void log_Z3_mk_abs(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(80);
}
void log_Z3_mk_lt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(81);
}
void log_Z3_mk_le(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(82);
}
void log_Z3_mk_gt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(83);
}
void log_Z3_mk_ge(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(84);
}
void log_Z3_mk_divides(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(85);
}
void log_Z3_mk_int2real(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(86);
}
void log_Z3_mk_real2int(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(87);
}
void log_Z3_mk_is_int(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(88);
}
void log_Z3_mk_bvnot(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(89);
}
void log_Z3_mk_bvredand(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(90);
}
void log_Z3_mk_bvredor(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(91);
}
void log_Z3_mk_bvand(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(92);
}
void log_Z3_mk_bvor(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(93);
}
void log_Z3_mk_bvxor(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(94);
}
void log_Z3_mk_bvnand(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(95);
}
void log_Z3_mk_bvnor(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(96);
}
void log_Z3_mk_bvxnor(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(97);
}
void log_Z3_mk_bvneg(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(98);
}
void log_Z3_mk_bvadd(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(99);
}
void log_Z3_mk_bvsub(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(100);
}
void log_Z3_mk_bvmul(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(101);
}
void log_Z3_mk_bvudiv(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(102);
}
void log_Z3_mk_bvsdiv(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(103);
}
void log_Z3_mk_bvurem(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(104);
}
void log_Z3_mk_bvsrem(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(105);
}
void log_Z3_mk_bvsmod(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(106);
}
void log_Z3_mk_bvult(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(107);
}
void log_Z3_mk_bvslt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(108);
}
void log_Z3_mk_bvule(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(109);
}
void log_Z3_mk_bvsle(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(110);
}
void log_Z3_mk_bvuge(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(111);
}
void log_Z3_mk_bvsge(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(112);
}
void log_Z3_mk_bvugt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(113);
}
void log_Z3_mk_bvsgt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(114);
}
void log_Z3_mk_concat(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(115);
}
void log_Z3_mk_extract(Z3_context a0, unsigned a1, unsigned a2, Z3_ast a3) {
  R();
  P(a0);
  U(a1);
  U(a2);
  P(a3);
  C(116);
}
void log_Z3_mk_sign_ext(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(117);
}
void log_Z3_mk_zero_ext(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(118);
}
void log_Z3_mk_repeat(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(119);
}
void log_Z3_mk_bit2bool(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(120);
}
void log_Z3_mk_bvshl(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(121);
}
void log_Z3_mk_bvlshr(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(122);
}
void log_Z3_mk_bvashr(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(123);
}
void log_Z3_mk_rotate_left(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(124);
}
void log_Z3_mk_rotate_right(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(125);
}
void log_Z3_mk_ext_rotate_left(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(126);
}
void log_Z3_mk_ext_rotate_right(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(127);
}
void log_Z3_mk_int2bv(Z3_context a0, unsigned a1, Z3_ast a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(128);
}
void log_Z3_mk_bv2int(Z3_context a0, Z3_ast a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(129);
}
void log_Z3_mk_bvadd_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2, bool a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  I(a3);
  C(130);
}
void log_Z3_mk_bvadd_no_underflow(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(131);
}
void log_Z3_mk_bvsub_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(132);
}
void log_Z3_mk_bvsub_no_underflow(Z3_context a0, Z3_ast a1, Z3_ast a2, bool a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  I(a3);
  C(133);
}
void log_Z3_mk_bvsdiv_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(134);
}
void log_Z3_mk_bvneg_no_overflow(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(135);
}
void log_Z3_mk_bvmul_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2, bool a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  I(a3);
  C(136);
}
void log_Z3_mk_bvmul_no_underflow(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(137);
}
void log_Z3_mk_select(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(138);
}
void log_Z3_mk_select_n(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(139);
}
void log_Z3_mk_store(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(140);
}
void log_Z3_mk_store_n(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3, Z3_ast a4) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(141);
}
void log_Z3_mk_const_array(Z3_context a0, Z3_sort a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(142);
}
void log_Z3_mk_map(Z3_context a0, Z3_func_decl a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(143);
}
void log_Z3_mk_array_default(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(144);
}
void log_Z3_mk_as_array(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(145);
}
void log_Z3_mk_set_has_size(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(146);
}
void log_Z3_mk_set_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(147);
}
void log_Z3_mk_empty_set(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(148);
}
void log_Z3_mk_full_set(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(149);
}
void log_Z3_mk_set_add(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(150);
}
void log_Z3_mk_set_del(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(151);
}
void log_Z3_mk_set_union(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(152);
}
void log_Z3_mk_set_intersect(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(153);
}
void log_Z3_mk_set_difference(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(154);
}
void log_Z3_mk_set_complement(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(155);
}
void log_Z3_mk_set_member(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(156);
}
void log_Z3_mk_set_subset(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(157);
}
void log_Z3_mk_array_ext(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(158);
}
void log_Z3_mk_numeral(Z3_context a0, Z3_string a1, Z3_sort a2) {
  R();
  P(a0);
  S(a1);
  P(a2);
  C(159);
}
void log_Z3_mk_real(Z3_context a0, int a1, int a2) {
  R();
  P(a0);
  I(a1);
  I(a2);
  C(160);
}
void log_Z3_mk_real_int64(Z3_context a0, int64_t a1, int64_t a2) {
  R();
  P(a0);
  I(a1);
  I(a2);
  C(161);
}
void log_Z3_mk_int(Z3_context a0, int a1, Z3_sort a2) {
  R();
  P(a0);
  I(a1);
  P(a2);
  C(162);
}
void log_Z3_mk_unsigned_int(Z3_context a0, unsigned a1, Z3_sort a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(163);
}
void log_Z3_mk_int64(Z3_context a0, int64_t a1, Z3_sort a2) {
  R();
  P(a0);
  I(a1);
  P(a2);
  C(164);
}
void log_Z3_mk_unsigned_int64(Z3_context a0, uint64_t a1, Z3_sort a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(165);
}
void log_Z3_mk_bv_numeral(Z3_context a0, unsigned a1, bool const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { U(a2[i]); }
  Au(a1);
  C(166);
}
void log_Z3_mk_seq_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(167);
}
void log_Z3_is_seq_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(168);
}
void log_Z3_get_seq_sort_basis(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(169);
}
void log_Z3_mk_re_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(170);
}
void log_Z3_is_re_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(171);
}
void log_Z3_get_re_sort_basis(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(172);
}
void log_Z3_mk_string_sort(Z3_context a0) {
  R();
  P(a0);
  C(173);
}
void log_Z3_mk_char_sort(Z3_context a0) {
  R();
  P(a0);
  C(174);
}
void log_Z3_is_string_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(175);
}
void log_Z3_is_char_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(176);
}
void log_Z3_mk_string(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(177);
}
void log_Z3_mk_lstring(Z3_context a0, unsigned a1, Z3_string a2) {
  R();
  P(a0);
  U(a1);
  S(a2);
  C(178);
}
void log_Z3_mk_u32string(Z3_context a0, unsigned a1, unsigned const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { U(a2[i]); }
  Au(a1);
  C(179);
}
void log_Z3_is_string(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(180);
}
void log_Z3_get_string(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(181);
}
void log_Z3_get_lstring(Z3_context a0, Z3_ast a1, unsigned* a2) {
  R();
  P(a0);
  P(a1);
  U(0);
  C(182);
}
void log_Z3_get_string_length(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(183);
}
void log_Z3_get_string_contents(Z3_context a0, Z3_ast a1, unsigned a2, unsigned* a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { U(0); }
  Au(a2);
  C(184);
}
void log_Z3_mk_seq_empty(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(185);
}
void log_Z3_mk_seq_unit(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(186);
}
void log_Z3_mk_seq_concat(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(187);
}
void log_Z3_mk_seq_prefix(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(188);
}
void log_Z3_mk_seq_suffix(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(189);
}
void log_Z3_mk_seq_contains(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(190);
}
void log_Z3_mk_str_lt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(191);
}
void log_Z3_mk_str_le(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(192);
}
void log_Z3_mk_seq_extract(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(193);
}
void log_Z3_mk_seq_replace(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(194);
}
void log_Z3_mk_seq_at(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(195);
}
void log_Z3_mk_seq_nth(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(196);
}
void log_Z3_mk_seq_length(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(197);
}
void log_Z3_mk_seq_index(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(198);
}
void log_Z3_mk_seq_last_index(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(199);
}
void log_Z3_mk_seq_map(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(200);
}
void log_Z3_mk_seq_mapi(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(201);
}
void log_Z3_mk_seq_foldl(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(202);
}
void log_Z3_mk_seq_foldli(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3, Z3_ast a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  P(a4);
  C(203);
}
void log_Z3_mk_str_to_int(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(204);
}
void log_Z3_mk_int_to_str(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(205);
}
void log_Z3_mk_string_to_code(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(206);
}
void log_Z3_mk_string_from_code(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(207);
}
void log_Z3_mk_ubv_to_str(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(208);
}
void log_Z3_mk_sbv_to_str(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(209);
}
void log_Z3_mk_seq_to_re(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(210);
}
void log_Z3_mk_seq_in_re(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(211);
}
void log_Z3_mk_re_plus(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(212);
}
void log_Z3_mk_re_star(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(213);
}
void log_Z3_mk_re_option(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(214);
}
void log_Z3_mk_re_union(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(215);
}
void log_Z3_mk_re_concat(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(216);
}
void log_Z3_mk_re_range(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(217);
}
void log_Z3_mk_re_allchar(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(218);
}
void log_Z3_mk_re_loop(Z3_context a0, Z3_ast a1, unsigned a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  U(a3);
  C(219);
}
void log_Z3_mk_re_power(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(220);
}
void log_Z3_mk_re_intersect(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(221);
}
void log_Z3_mk_re_complement(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(222);
}
void log_Z3_mk_re_diff(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(223);
}
void log_Z3_mk_re_empty(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(224);
}
void log_Z3_mk_re_full(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(225);
}
void log_Z3_mk_char(Z3_context a0, unsigned a1) {
  R();
  P(a0);
  U(a1);
  C(226);
}
void log_Z3_mk_char_le(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(227);
}
void log_Z3_mk_char_to_int(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(228);
}
void log_Z3_mk_char_to_bv(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(229);
}
void log_Z3_mk_char_from_bv(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(230);
}
void log_Z3_mk_char_is_digit(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(231);
}
void log_Z3_mk_linear_order(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(232);
}
void log_Z3_mk_partial_order(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(233);
}
void log_Z3_mk_piecewise_linear_order(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(234);
}
void log_Z3_mk_tree_order(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(235);
}
void log_Z3_mk_transitive_closure(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(236);
}
void log_Z3_mk_pattern(Z3_context a0, unsigned a1, Z3_ast const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(237);
}
void log_Z3_mk_bound(Z3_context a0, unsigned a1, Z3_sort a2) {
  R();
  P(a0);
  U(a1);
  P(a2);
  C(238);
}
void log_Z3_mk_forall(Z3_context a0, unsigned a1, unsigned a2, Z3_pattern const * a3, unsigned a4, Z3_sort const * a5, Z3_symbol const * a6, Z3_ast a7) {
  R();
  P(a0);
  U(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  U(a4);
  for (unsigned i = 0; i < a4; i++) { P(a5[i]); }
  Ap(a4);
  for (unsigned i = 0; i < a4; i++) { Sy(a6[i]); }
  Asy(a4);
  P(a7);
  C(239);
}
void log_Z3_mk_exists(Z3_context a0, unsigned a1, unsigned a2, Z3_pattern const * a3, unsigned a4, Z3_sort const * a5, Z3_symbol const * a6, Z3_ast a7) {
  R();
  P(a0);
  U(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  U(a4);
  for (unsigned i = 0; i < a4; i++) { P(a5[i]); }
  Ap(a4);
  for (unsigned i = 0; i < a4; i++) { Sy(a6[i]); }
  Asy(a4);
  P(a7);
  C(240);
}
void log_Z3_mk_quantifier(Z3_context a0, bool a1, unsigned a2, unsigned a3, Z3_pattern const * a4, unsigned a5, Z3_sort const * a6, Z3_symbol const * a7, Z3_ast a8) {
  R();
  P(a0);
  I(a1);
  U(a2);
  U(a3);
  for (unsigned i = 0; i < a3; i++) { P(a4[i]); }
  Ap(a3);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { P(a6[i]); }
  Ap(a5);
  for (unsigned i = 0; i < a5; i++) { Sy(a7[i]); }
  Asy(a5);
  P(a8);
  C(241);
}
void log_Z3_mk_quantifier_ex(Z3_context a0, bool a1, unsigned a2, Z3_symbol a3, Z3_symbol a4, unsigned a5, Z3_pattern const * a6, unsigned a7, Z3_ast const * a8, unsigned a9, Z3_sort const * a10, Z3_symbol const * a11, Z3_ast a12) {
  R();
  P(a0);
  I(a1);
  U(a2);
  Sy(a3);
  Sy(a4);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { P(a6[i]); }
  Ap(a5);
  U(a7);
  for (unsigned i = 0; i < a7; i++) { P(a8[i]); }
  Ap(a7);
  U(a9);
  for (unsigned i = 0; i < a9; i++) { P(a10[i]); }
  Ap(a9);
  for (unsigned i = 0; i < a9; i++) { Sy(a11[i]); }
  Asy(a9);
  P(a12);
  C(242);
}
void log_Z3_mk_forall_const(Z3_context a0, unsigned a1, unsigned a2, Z3_app const * a3, unsigned a4, Z3_pattern const * a5, Z3_ast a6) {
  R();
  P(a0);
  U(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  U(a4);
  for (unsigned i = 0; i < a4; i++) { P(a5[i]); }
  Ap(a4);
  P(a6);
  C(243);
}
void log_Z3_mk_exists_const(Z3_context a0, unsigned a1, unsigned a2, Z3_app const * a3, unsigned a4, Z3_pattern const * a5, Z3_ast a6) {
  R();
  P(a0);
  U(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  U(a4);
  for (unsigned i = 0; i < a4; i++) { P(a5[i]); }
  Ap(a4);
  P(a6);
  C(244);
}
void log_Z3_mk_quantifier_const(Z3_context a0, bool a1, unsigned a2, unsigned a3, Z3_app const * a4, unsigned a5, Z3_pattern const * a6, Z3_ast a7) {
  R();
  P(a0);
  I(a1);
  U(a2);
  U(a3);
  for (unsigned i = 0; i < a3; i++) { P(a4[i]); }
  Ap(a3);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { P(a6[i]); }
  Ap(a5);
  P(a7);
  C(245);
}
void log_Z3_mk_quantifier_const_ex(Z3_context a0, bool a1, unsigned a2, Z3_symbol a3, Z3_symbol a4, unsigned a5, Z3_app const * a6, unsigned a7, Z3_pattern const * a8, unsigned a9, Z3_ast const * a10, Z3_ast a11) {
  R();
  P(a0);
  I(a1);
  U(a2);
  Sy(a3);
  Sy(a4);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { P(a6[i]); }
  Ap(a5);
  U(a7);
  for (unsigned i = 0; i < a7; i++) { P(a8[i]); }
  Ap(a7);
  U(a9);
  for (unsigned i = 0; i < a9; i++) { P(a10[i]); }
  Ap(a9);
  P(a11);
  C(246);
}
void log_Z3_mk_lambda(Z3_context a0, unsigned a1, Z3_sort const * a2, Z3_symbol const * a3, Z3_ast a4) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  for (unsigned i = 0; i < a1; i++) { Sy(a3[i]); }
  Asy(a1);
  P(a4);
  C(247);
}
void log_Z3_mk_lambda_const(Z3_context a0, unsigned a1, Z3_app const * a2, Z3_ast a3) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  P(a3);
  C(248);
}
void log_Z3_get_symbol_kind(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(249);
}
void log_Z3_get_symbol_int(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(250);
}
void log_Z3_get_symbol_string(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(251);
}
void log_Z3_get_sort_name(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(252);
}
void log_Z3_get_sort_id(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(253);
}
void log_Z3_sort_to_ast(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(254);
}
void log_Z3_is_eq_sort(Z3_context a0, Z3_sort a1, Z3_sort a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(255);
}
void log_Z3_get_sort_kind(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(256);
}
void log_Z3_get_bv_sort_size(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(257);
}
void log_Z3_get_finite_domain_sort_size(Z3_context a0, Z3_sort a1, uint64_t* a2) {
  R();
  P(a0);
  P(a1);
  U(0);
  C(258);
}
void log_Z3_get_array_arity(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(259);
}
void log_Z3_get_array_sort_domain(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(260);
}
void log_Z3_get_array_sort_domain_n(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(261);
}
void log_Z3_get_array_sort_range(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(262);
}
void log_Z3_get_tuple_sort_mk_decl(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(263);
}
void log_Z3_get_tuple_sort_num_fields(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(264);
}
void log_Z3_get_tuple_sort_field_decl(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(265);
}
void log_Z3_is_recursive_datatype_sort(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(266);
}
void log_Z3_get_datatype_sort_num_constructors(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(267);
}
void log_Z3_get_datatype_sort_constructor(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(268);
}
void log_Z3_get_datatype_sort_recognizer(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(269);
}
void log_Z3_get_datatype_sort_constructor_accessor(Z3_context a0, Z3_sort a1, unsigned a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  U(a3);
  C(270);
}
void log_Z3_datatype_update_field(Z3_context a0, Z3_func_decl a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(271);
}
void log_Z3_get_relation_arity(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(272);
}
void log_Z3_get_relation_column(Z3_context a0, Z3_sort a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(273);
}
void log_Z3_mk_atmost(Z3_context a0, unsigned a1, Z3_ast const * a2, unsigned a3) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  U(a3);
  C(274);
}
void log_Z3_mk_atleast(Z3_context a0, unsigned a1, Z3_ast const * a2, unsigned a3) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  U(a3);
  C(275);
}
void log_Z3_mk_pble(Z3_context a0, unsigned a1, Z3_ast const * a2, int const * a3, int a4) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  for (unsigned i = 0; i < a1; i++) { I(a3[i]); }
  Ai(a1);
  I(a4);
  C(276);
}
void log_Z3_mk_pbge(Z3_context a0, unsigned a1, Z3_ast const * a2, int const * a3, int a4) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  for (unsigned i = 0; i < a1; i++) { I(a3[i]); }
  Ai(a1);
  I(a4);
  C(277);
}
void log_Z3_mk_pbeq(Z3_context a0, unsigned a1, Z3_ast const * a2, int const * a3, int a4) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  for (unsigned i = 0; i < a1; i++) { I(a3[i]); }
  Ai(a1);
  I(a4);
  C(278);
}
void log_Z3_func_decl_to_ast(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(279);
}
void log_Z3_is_eq_func_decl(Z3_context a0, Z3_func_decl a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(280);
}
void log_Z3_get_func_decl_id(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(281);
}
void log_Z3_get_decl_name(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(282);
}
void log_Z3_get_decl_kind(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(283);
}
void log_Z3_get_domain_size(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(284);
}
void log_Z3_get_arity(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(285);
}
void log_Z3_get_domain(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(286);
}
void log_Z3_get_range(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(287);
}
void log_Z3_get_decl_num_parameters(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(288);
}
void log_Z3_get_decl_parameter_kind(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(289);
}
void log_Z3_get_decl_int_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(290);
}
void log_Z3_get_decl_double_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(291);
}
void log_Z3_get_decl_symbol_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(292);
}
void log_Z3_get_decl_sort_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(293);
}
void log_Z3_get_decl_ast_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(294);
}
void log_Z3_get_decl_func_decl_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(295);
}
void log_Z3_get_decl_rational_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(296);
}
void log_Z3_app_to_ast(Z3_context a0, Z3_app a1) {
  R();
  P(a0);
  P(a1);
  C(297);
}
void log_Z3_get_app_decl(Z3_context a0, Z3_app a1) {
  R();
  P(a0);
  P(a1);
  C(298);
}
void log_Z3_get_app_num_args(Z3_context a0, Z3_app a1) {
  R();
  P(a0);
  P(a1);
  C(299);
}
void log_Z3_get_app_arg(Z3_context a0, Z3_app a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(300);
}
void log_Z3_is_eq_ast(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(301);
}
void log_Z3_get_ast_id(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(302);
}
void log_Z3_get_ast_hash(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(303);
}
void log_Z3_get_sort(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(304);
}
void log_Z3_is_well_sorted(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(305);
}
void log_Z3_get_bool_value(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(306);
}
void log_Z3_get_ast_kind(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(307);
}
void log_Z3_is_app(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(308);
}
void log_Z3_is_ground(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(309);
}
void log_Z3_get_depth(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(310);
}
void log_Z3_is_numeral_ast(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(311);
}
void log_Z3_is_algebraic_number(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(312);
}
void log_Z3_to_app(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(313);
}
void log_Z3_to_func_decl(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(314);
}
void log_Z3_get_numeral_string(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(315);
}
void log_Z3_get_numeral_binary_string(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(316);
}
void log_Z3_get_numeral_decimal_string(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(317);
}
void log_Z3_get_numeral_double(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(318);
}
void log_Z3_get_numerator(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(319);
}
void log_Z3_get_denominator(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(320);
}
void log_Z3_get_numeral_small(Z3_context a0, Z3_ast a1, int64_t* a2, int64_t* a3) {
  R();
  P(a0);
  P(a1);
  I(0);
  I(0);
  C(321);
}
void log_Z3_get_numeral_int(Z3_context a0, Z3_ast a1, int* a2) {
  R();
  P(a0);
  P(a1);
  I(0);
  C(322);
}
void log_Z3_get_numeral_uint(Z3_context a0, Z3_ast a1, unsigned* a2) {
  R();
  P(a0);
  P(a1);
  U(0);
  C(323);
}
void log_Z3_get_numeral_uint64(Z3_context a0, Z3_ast a1, uint64_t* a2) {
  R();
  P(a0);
  P(a1);
  U(0);
  C(324);
}
void log_Z3_get_numeral_int64(Z3_context a0, Z3_ast a1, int64_t* a2) {
  R();
  P(a0);
  P(a1);
  I(0);
  C(325);
}
void log_Z3_get_numeral_rational_int64(Z3_context a0, Z3_ast a1, int64_t* a2, int64_t* a3) {
  R();
  P(a0);
  P(a1);
  I(0);
  I(0);
  C(326);
}
void log_Z3_get_algebraic_number_lower(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(327);
}
void log_Z3_get_algebraic_number_upper(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(328);
}
void log_Z3_pattern_to_ast(Z3_context a0, Z3_pattern a1) {
  R();
  P(a0);
  P(a1);
  C(329);
}
void log_Z3_get_pattern_num_terms(Z3_context a0, Z3_pattern a1) {
  R();
  P(a0);
  P(a1);
  C(330);
}
void log_Z3_get_pattern(Z3_context a0, Z3_pattern a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(331);
}
void log_Z3_get_index_value(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(332);
}
void log_Z3_is_quantifier_forall(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(333);
}
void log_Z3_is_quantifier_exists(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(334);
}
void log_Z3_is_lambda(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(335);
}
void log_Z3_get_quantifier_weight(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(336);
}
void log_Z3_get_quantifier_skolem_id(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(337);
}
void log_Z3_get_quantifier_id(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(338);
}
void log_Z3_get_quantifier_num_patterns(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(339);
}
void log_Z3_get_quantifier_pattern_ast(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(340);
}
void log_Z3_get_quantifier_num_no_patterns(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(341);
}
void log_Z3_get_quantifier_no_pattern_ast(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(342);
}
void log_Z3_get_quantifier_num_bound(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(343);
}
void log_Z3_get_quantifier_bound_name(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(344);
}
void log_Z3_get_quantifier_bound_sort(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(345);
}
void log_Z3_get_quantifier_body(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(346);
}
void log_Z3_simplify(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(347);
}
void log_Z3_simplify_ex(Z3_context a0, Z3_ast a1, Z3_params a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(348);
}
void log_Z3_simplify_get_help(Z3_context a0) {
  R();
  P(a0);
  C(349);
}
void log_Z3_simplify_get_param_descrs(Z3_context a0) {
  R();
  P(a0);
  C(350);
}
void log_Z3_update_term(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(351);
}
void log_Z3_substitute(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3, Z3_ast const * a4) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  for (unsigned i = 0; i < a2; i++) { P(a4[i]); }
  Ap(a2);
  C(352);
}
void log_Z3_substitute_vars(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(353);
}
void log_Z3_substitute_funs(Z3_context a0, Z3_ast a1, unsigned a2, Z3_func_decl const * a3, Z3_ast const * a4) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  for (unsigned i = 0; i < a2; i++) { P(a4[i]); }
  Ap(a2);
  C(354);
}
void log_Z3_translate(Z3_context a0, Z3_ast a1, Z3_context a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(355);
}
void log_Z3_mk_model(Z3_context a0) {
  R();
  P(a0);
  C(356);
}
void log_Z3_model_inc_ref(Z3_context a0, Z3_model a1) {
  R();
  P(a0);
  P(a1);
  C(357);
}
void log_Z3_model_dec_ref(Z3_context a0, Z3_model a1) {
  R();
  P(a0);
  P(a1);
  C(358);
}
void log_Z3_model_eval(Z3_context a0, Z3_model a1, Z3_ast a2, bool a3, Z3_ast* a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  I(a3);
  P(0);
  C(359);
}
void log_Z3_model_get_const_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(360);
}
void log_Z3_model_has_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(361);
}
void log_Z3_model_get_func_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(362);
}
void log_Z3_model_get_num_consts(Z3_context a0, Z3_model a1) {
  R();
  P(a0);
  P(a1);
  C(363);
}
void log_Z3_model_get_const_decl(Z3_context a0, Z3_model a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(364);
}
void log_Z3_model_get_num_funcs(Z3_context a0, Z3_model a1) {
  R();
  P(a0);
  P(a1);
  C(365);
}
void log_Z3_model_get_func_decl(Z3_context a0, Z3_model a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(366);
}
void log_Z3_model_get_num_sorts(Z3_context a0, Z3_model a1) {
  R();
  P(a0);
  P(a1);
  C(367);
}
void log_Z3_model_get_sort(Z3_context a0, Z3_model a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(368);
}
void log_Z3_model_get_sort_universe(Z3_context a0, Z3_model a1, Z3_sort a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(369);
}
void log_Z3_model_translate(Z3_context a0, Z3_model a1, Z3_context a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(370);
}
void log_Z3_is_as_array(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(371);
}
void log_Z3_get_as_array_func_decl(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(372);
}
void log_Z3_add_func_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(373);
}
void log_Z3_add_const_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(374);
}
void log_Z3_func_interp_inc_ref(Z3_context a0, Z3_func_interp a1) {
  R();
  P(a0);
  P(a1);
  C(375);
}
void log_Z3_func_interp_dec_ref(Z3_context a0, Z3_func_interp a1) {
  R();
  P(a0);
  P(a1);
  C(376);
}
void log_Z3_func_interp_get_num_entries(Z3_context a0, Z3_func_interp a1) {
  R();
  P(a0);
  P(a1);
  C(377);
}
void log_Z3_func_interp_get_entry(Z3_context a0, Z3_func_interp a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(378);
}
void log_Z3_func_interp_get_else(Z3_context a0, Z3_func_interp a1) {
  R();
  P(a0);
  P(a1);
  C(379);
}
void log_Z3_func_interp_set_else(Z3_context a0, Z3_func_interp a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(380);
}
void log_Z3_func_interp_get_arity(Z3_context a0, Z3_func_interp a1) {
  R();
  P(a0);
  P(a1);
  C(381);
}
void log_Z3_func_interp_add_entry(Z3_context a0, Z3_func_interp a1, Z3_ast_vector a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(382);
}
void log_Z3_func_entry_inc_ref(Z3_context a0, Z3_func_entry a1) {
  R();
  P(a0);
  P(a1);
  C(383);
}
void log_Z3_func_entry_dec_ref(Z3_context a0, Z3_func_entry a1) {
  R();
  P(a0);
  P(a1);
  C(384);
}
void log_Z3_func_entry_get_value(Z3_context a0, Z3_func_entry a1) {
  R();
  P(a0);
  P(a1);
  C(385);
}
void log_Z3_func_entry_get_num_args(Z3_context a0, Z3_func_entry a1) {
  R();
  P(a0);
  P(a1);
  C(386);
}
void log_Z3_func_entry_get_arg(Z3_context a0, Z3_func_entry a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(387);
}
void log_Z3_toggle_warning_messages(bool a0) {
  R();
  I(a0);
  C(388);
}
void log_Z3_set_ast_print_mode(Z3_context a0, Z3_ast_print_mode a1) {
  R();
  P(a0);
  U(static_cast<unsigned>(a1));
  C(389);
}
void log_Z3_ast_to_string(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(390);
}
void log_Z3_pattern_to_string(Z3_context a0, Z3_pattern a1) {
  R();
  P(a0);
  P(a1);
  C(391);
}
void log_Z3_sort_to_string(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(392);
}
void log_Z3_func_decl_to_string(Z3_context a0, Z3_func_decl a1) {
  R();
  P(a0);
  P(a1);
  C(393);
}
void log_Z3_model_to_string(Z3_context a0, Z3_model a1) {
  R();
  P(a0);
  P(a1);
  C(394);
}
void log_Z3_benchmark_to_smtlib_string(Z3_context a0, Z3_string a1, Z3_string a2, Z3_string a3, Z3_string a4, unsigned a5, Z3_ast const * a6, Z3_ast a7) {
  R();
  P(a0);
  S(a1);
  S(a2);
  S(a3);
  S(a4);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { P(a6[i]); }
  Ap(a5);
  P(a7);
  C(395);
}
void log_Z3_parse_smtlib2_string(Z3_context a0, Z3_string a1, unsigned a2, Z3_symbol const * a3, Z3_sort const * a4, unsigned a5, Z3_symbol const * a6, Z3_func_decl const * a7) {
  R();
  P(a0);
  S(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { Sy(a3[i]); }
  Asy(a2);
  for (unsigned i = 0; i < a2; i++) { P(a4[i]); }
  Ap(a2);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { Sy(a6[i]); }
  Asy(a5);
  for (unsigned i = 0; i < a5; i++) { P(a7[i]); }
  Ap(a5);
  C(396);
}
void log_Z3_parse_smtlib2_file(Z3_context a0, Z3_string a1, unsigned a2, Z3_symbol const * a3, Z3_sort const * a4, unsigned a5, Z3_symbol const * a6, Z3_func_decl const * a7) {
  R();
  P(a0);
  S(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { Sy(a3[i]); }
  Asy(a2);
  for (unsigned i = 0; i < a2; i++) { P(a4[i]); }
  Ap(a2);
  U(a5);
  for (unsigned i = 0; i < a5; i++) { Sy(a6[i]); }
  Asy(a5);
  for (unsigned i = 0; i < a5; i++) { P(a7[i]); }
  Ap(a5);
  C(397);
}
void log_Z3_eval_smtlib2_string(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(398);
}
void log_Z3_mk_parser_context(Z3_context a0) {
  R();
  P(a0);
  C(399);
}
void log_Z3_parser_context_inc_ref(Z3_context a0, Z3_parser_context a1) {
  R();
  P(a0);
  P(a1);
  C(400);
}
void log_Z3_parser_context_dec_ref(Z3_context a0, Z3_parser_context a1) {
  R();
  P(a0);
  P(a1);
  C(401);
}
void log_Z3_parser_context_add_sort(Z3_context a0, Z3_parser_context a1, Z3_sort a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(402);
}
void log_Z3_parser_context_add_decl(Z3_context a0, Z3_parser_context a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(403);
}
void log_Z3_parser_context_from_string(Z3_context a0, Z3_parser_context a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(404);
}
void log_Z3_get_error_code(Z3_context a0) {
  R();
  P(a0);
  C(405);
}
void log_Z3_set_error(Z3_context a0, Z3_error_code a1) {
  R();
  P(a0);
  U(static_cast<unsigned>(a1));
  C(406);
}
void log_Z3_get_error_msg(Z3_context a0, Z3_error_code a1) {
  R();
  P(a0);
  U(static_cast<unsigned>(a1));
  C(407);
}
void log_Z3_get_version(unsigned* a0, unsigned* a1, unsigned* a2, unsigned* a3) {
  R();
  U(0);
  U(0);
  U(0);
  U(0);
  C(408);
}
void log_Z3_get_full_version() {
  R();
  C(409);
}
void log_Z3_enable_trace(Z3_string a0) {
  R();
  S(a0);
  C(410);
}
void log_Z3_disable_trace(Z3_string a0) {
  R();
  S(a0);
  C(411);
}
void log_Z3_reset_memory() {
  R();
  C(412);
}
void log_Z3_finalize_memory() {
  R();
  C(413);
}
void log_Z3_mk_goal(Z3_context a0, bool a1, bool a2, bool a3) {
  R();
  P(a0);
  I(a1);
  I(a2);
  I(a3);
  C(414);
}
void log_Z3_goal_inc_ref(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(415);
}
void log_Z3_goal_dec_ref(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(416);
}
void log_Z3_goal_precision(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(417);
}
void log_Z3_goal_assert(Z3_context a0, Z3_goal a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(418);
}
void log_Z3_goal_inconsistent(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(419);
}
void log_Z3_goal_depth(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(420);
}
void log_Z3_goal_reset(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(421);
}
void log_Z3_goal_size(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(422);
}
void log_Z3_goal_formula(Z3_context a0, Z3_goal a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(423);
}
void log_Z3_goal_num_exprs(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(424);
}
void log_Z3_goal_is_decided_sat(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(425);
}
void log_Z3_goal_is_decided_unsat(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(426);
}
void log_Z3_goal_translate(Z3_context a0, Z3_goal a1, Z3_context a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(427);
}
void log_Z3_goal_convert_model(Z3_context a0, Z3_goal a1, Z3_model a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(428);
}
void log_Z3_goal_to_string(Z3_context a0, Z3_goal a1) {
  R();
  P(a0);
  P(a1);
  C(429);
}
void log_Z3_goal_to_dimacs_string(Z3_context a0, Z3_goal a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(430);
}
void log_Z3_mk_tactic(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(431);
}
void log_Z3_tactic_inc_ref(Z3_context a0, Z3_tactic a1) {
  R();
  P(a0);
  P(a1);
  C(432);
}
void log_Z3_tactic_dec_ref(Z3_context a0, Z3_tactic a1) {
  R();
  P(a0);
  P(a1);
  C(433);
}
void log_Z3_mk_probe(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(434);
}
void log_Z3_probe_inc_ref(Z3_context a0, Z3_probe a1) {
  R();
  P(a0);
  P(a1);
  C(435);
}
void log_Z3_probe_dec_ref(Z3_context a0, Z3_probe a1) {
  R();
  P(a0);
  P(a1);
  C(436);
}
void log_Z3_tactic_and_then(Z3_context a0, Z3_tactic a1, Z3_tactic a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(437);
}
void log_Z3_tactic_or_else(Z3_context a0, Z3_tactic a1, Z3_tactic a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(438);
}
void log_Z3_tactic_par_or(Z3_context a0, unsigned a1, Z3_tactic const * a2) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  C(439);
}
void log_Z3_tactic_par_and_then(Z3_context a0, Z3_tactic a1, Z3_tactic a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(440);
}
void log_Z3_tactic_try_for(Z3_context a0, Z3_tactic a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(441);
}
void log_Z3_tactic_when(Z3_context a0, Z3_probe a1, Z3_tactic a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(442);
}
void log_Z3_tactic_cond(Z3_context a0, Z3_probe a1, Z3_tactic a2, Z3_tactic a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(443);
}
void log_Z3_tactic_repeat(Z3_context a0, Z3_tactic a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(444);
}
void log_Z3_tactic_skip(Z3_context a0) {
  R();
  P(a0);
  C(445);
}
void log_Z3_tactic_fail(Z3_context a0) {
  R();
  P(a0);
  C(446);
}
void log_Z3_tactic_fail_if(Z3_context a0, Z3_probe a1) {
  R();
  P(a0);
  P(a1);
  C(447);
}
void log_Z3_tactic_fail_if_not_decided(Z3_context a0) {
  R();
  P(a0);
  C(448);
}
void log_Z3_tactic_using_params(Z3_context a0, Z3_tactic a1, Z3_params a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(449);
}
void log_Z3_mk_simplifier(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(450);
}
void log_Z3_simplifier_inc_ref(Z3_context a0, Z3_simplifier a1) {
  R();
  P(a0);
  P(a1);
  C(451);
}
void log_Z3_simplifier_dec_ref(Z3_context a0, Z3_simplifier a1) {
  R();
  P(a0);
  P(a1);
  C(452);
}
void log_Z3_solver_add_simplifier(Z3_context a0, Z3_solver a1, Z3_simplifier a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(453);
}
void log_Z3_simplifier_and_then(Z3_context a0, Z3_simplifier a1, Z3_simplifier a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(454);
}
void log_Z3_simplifier_using_params(Z3_context a0, Z3_simplifier a1, Z3_params a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(455);
}
void log_Z3_get_num_simplifiers(Z3_context a0) {
  R();
  P(a0);
  C(456);
}
void log_Z3_get_simplifier_name(Z3_context a0, unsigned a1) {
  R();
  P(a0);
  U(a1);
  C(457);
}
void log_Z3_simplifier_get_help(Z3_context a0, Z3_simplifier a1) {
  R();
  P(a0);
  P(a1);
  C(458);
}
void log_Z3_simplifier_get_param_descrs(Z3_context a0, Z3_simplifier a1) {
  R();
  P(a0);
  P(a1);
  C(459);
}
void log_Z3_simplifier_get_descr(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(460);
}
void log_Z3_probe_const(Z3_context a0, double a1) {
  R();
  P(a0);
  D(a1);
  C(461);
}
void log_Z3_probe_lt(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(462);
}
void log_Z3_probe_gt(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(463);
}
void log_Z3_probe_le(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(464);
}
void log_Z3_probe_ge(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(465);
}
void log_Z3_probe_eq(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(466);
}
void log_Z3_probe_and(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(467);
}
void log_Z3_probe_or(Z3_context a0, Z3_probe a1, Z3_probe a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(468);
}
void log_Z3_probe_not(Z3_context a0, Z3_probe a1) {
  R();
  P(a0);
  P(a1);
  C(469);
}
void log_Z3_get_num_tactics(Z3_context a0) {
  R();
  P(a0);
  C(470);
}
void log_Z3_get_tactic_name(Z3_context a0, unsigned a1) {
  R();
  P(a0);
  U(a1);
  C(471);
}
void log_Z3_get_num_probes(Z3_context a0) {
  R();
  P(a0);
  C(472);
}
void log_Z3_get_probe_name(Z3_context a0, unsigned a1) {
  R();
  P(a0);
  U(a1);
  C(473);
}
void log_Z3_tactic_get_help(Z3_context a0, Z3_tactic a1) {
  R();
  P(a0);
  P(a1);
  C(474);
}
void log_Z3_tactic_get_param_descrs(Z3_context a0, Z3_tactic a1) {
  R();
  P(a0);
  P(a1);
  C(475);
}
void log_Z3_tactic_get_descr(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(476);
}
void log_Z3_probe_get_descr(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(477);
}
void log_Z3_probe_apply(Z3_context a0, Z3_probe a1, Z3_goal a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(478);
}
void log_Z3_tactic_apply(Z3_context a0, Z3_tactic a1, Z3_goal a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(479);
}
void log_Z3_tactic_apply_ex(Z3_context a0, Z3_tactic a1, Z3_goal a2, Z3_params a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(480);
}
void log_Z3_apply_result_inc_ref(Z3_context a0, Z3_apply_result a1) {
  R();
  P(a0);
  P(a1);
  C(481);
}
void log_Z3_apply_result_dec_ref(Z3_context a0, Z3_apply_result a1) {
  R();
  P(a0);
  P(a1);
  C(482);
}
void log_Z3_apply_result_to_string(Z3_context a0, Z3_apply_result a1) {
  R();
  P(a0);
  P(a1);
  C(483);
}
void log_Z3_apply_result_get_num_subgoals(Z3_context a0, Z3_apply_result a1) {
  R();
  P(a0);
  P(a1);
  C(484);
}
void log_Z3_apply_result_get_subgoal(Z3_context a0, Z3_apply_result a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(485);
}
void log_Z3_mk_solver(Z3_context a0) {
  R();
  P(a0);
  C(486);
}
void log_Z3_mk_simple_solver(Z3_context a0) {
  R();
  P(a0);
  C(487);
}
void log_Z3_mk_solver_for_logic(Z3_context a0, Z3_symbol a1) {
  R();
  P(a0);
  Sy(a1);
  C(488);
}
void log_Z3_mk_solver_from_tactic(Z3_context a0, Z3_tactic a1) {
  R();
  P(a0);
  P(a1);
  C(489);
}
void log_Z3_solver_translate(Z3_context a0, Z3_solver a1, Z3_context a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(490);
}
void log_Z3_solver_import_model_converter(Z3_context a0, Z3_solver a1, Z3_solver a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(491);
}
void log_Z3_solver_get_help(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(492);
}
void log_Z3_solver_get_param_descrs(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(493);
}
void log_Z3_solver_set_params(Z3_context a0, Z3_solver a1, Z3_params a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(494);
}
void log_Z3_solver_inc_ref(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(495);
}
void log_Z3_solver_dec_ref(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(496);
}
void log_Z3_solver_interrupt(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(497);
}
void log_Z3_solver_push(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(498);
}
void log_Z3_solver_pop(Z3_context a0, Z3_solver a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(499);
}
void log_Z3_solver_reset(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(500);
}
void log_Z3_solver_get_num_scopes(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(501);
}
void log_Z3_solver_assert(Z3_context a0, Z3_solver a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(502);
}
void log_Z3_solver_assert_and_track(Z3_context a0, Z3_solver a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(503);
}
void log_Z3_solver_from_file(Z3_context a0, Z3_solver a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(504);
}
void log_Z3_solver_from_string(Z3_context a0, Z3_solver a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(505);
}
void log_Z3_solver_get_assertions(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(506);
}
void log_Z3_solver_get_units(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(507);
}
void log_Z3_solver_get_trail(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(508);
}
void log_Z3_solver_get_non_units(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(509);
}
void log_Z3_solver_get_levels(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, unsigned a3, unsigned const * a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  for (unsigned i = 0; i < a3; i++) { U(a4[i]); }
  Au(a3);
  C(510);
}
void log_Z3_solver_congruence_root(Z3_context a0, Z3_solver a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(511);
}
void log_Z3_solver_congruence_next(Z3_context a0, Z3_solver a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(512);
}
void log_Z3_solver_congruence_explain(Z3_context a0, Z3_solver a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(513);
}
void log_Z3_solver_solve_for(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, Z3_ast_vector a3, Z3_ast_vector a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  P(a4);
  C(514);
}
void log_Z3_solver_register_on_clause(Z3_context a0, Z3_solver a1, void* a2, Z3_on_clause_eh* a3) {
  R();
  P(a0);
  P(a1);
  P(0);
//  P(a3);
  C(515);
}
void log_Z3_solver_propagate_init(Z3_context a0, Z3_solver a1, void* a2, Z3_push_eh* a3, Z3_pop_eh* a4, Z3_fresh_eh* a5) {
  R();
  P(a0);
  P(a1);
  P(0);
//  P(a3);
//  P(a4);
//  P(a5);
  C(516);
}
void log_Z3_solver_propagate_fixed(Z3_context a0, Z3_solver a1, Z3_fixed_eh* a2) {
  R();
  P(a0);
  P(a1);
//  P(a2);
  C(517);
}
void log_Z3_solver_propagate_final(Z3_context a0, Z3_solver a1, Z3_final_eh* a2) {
  R();
  P(a0);
  P(a1);
//  P(a2);
  C(518);
}
void log_Z3_solver_propagate_eq(Z3_context a0, Z3_solver a1, Z3_eq_eh* a2) {
  R();
  P(a0);
  P(a1);
//  P(a2);
  C(519);
}
void log_Z3_solver_propagate_diseq(Z3_context a0, Z3_solver a1, Z3_eq_eh* a2) {
  R();
  P(a0);
  P(a1);
//  P(a2);
  C(520);
}
void log_Z3_solver_propagate_created(Z3_context a0, Z3_solver a1, Z3_created_eh* a2) {
  R();
  P(a0);
  P(a1);
//  P(a2);
  C(521);
}
void log_Z3_solver_propagate_decide(Z3_context a0, Z3_solver a1, Z3_decide_eh* a2) {
  R();
  P(a0);
  P(a1);
//  P(a2);
  C(522);
}
void log_Z3_solver_next_split(Z3_context a0, Z3_solver_callback a1, Z3_ast a2, unsigned a3, Z3_lbool a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  I(static_cast<signed>(a4));
  C(523);
}
void log_Z3_solver_propagate_declare(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_sort const * a3, Z3_sort a4) {
  R();
  P(a0);
  Sy(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(524);
}
void log_Z3_solver_propagate_register(Z3_context a0, Z3_solver a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(525);
}
void log_Z3_solver_propagate_register_cb(Z3_context a0, Z3_solver_callback a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(526);
}
void log_Z3_solver_propagate_consequence(Z3_context a0, Z3_solver_callback a1, unsigned a2, Z3_ast const * a3, unsigned a4, Z3_ast const * a5, Z3_ast const * a6, Z3_ast a7) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  U(a4);
  for (unsigned i = 0; i < a4; i++) { P(a5[i]); }
  Ap(a4);
  for (unsigned i = 0; i < a4; i++) { P(a6[i]); }
  Ap(a4);
  P(a7);
  C(527);
}
void log_Z3_solver_set_initial_value(Z3_context a0, Z3_solver a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(528);
}
void log_Z3_solver_check(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(529);
}
void log_Z3_solver_check_assumptions(Z3_context a0, Z3_solver a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(530);
}
void log_Z3_get_implied_equalities(Z3_context a0, Z3_solver a1, unsigned a2, Z3_ast const * a3, unsigned* a4) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  for (unsigned i = 0; i < a2; i++) { U(0); }
  Au(a2);
  C(531);
}
void log_Z3_solver_get_consequences(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, Z3_ast_vector a3, Z3_ast_vector a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  P(a4);
  C(532);
}
void log_Z3_solver_cube(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  C(533);
}
void log_Z3_solver_get_model(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(534);
}
void log_Z3_solver_get_proof(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(535);
}
void log_Z3_solver_get_unsat_core(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(536);
}
void log_Z3_solver_get_reason_unknown(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(537);
}
void log_Z3_solver_get_statistics(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(538);
}
void log_Z3_solver_to_string(Z3_context a0, Z3_solver a1) {
  R();
  P(a0);
  P(a1);
  C(539);
}
void log_Z3_solver_to_dimacs_string(Z3_context a0, Z3_solver a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(540);
}
void log_Z3_stats_to_string(Z3_context a0, Z3_stats a1) {
  R();
  P(a0);
  P(a1);
  C(541);
}
void log_Z3_stats_inc_ref(Z3_context a0, Z3_stats a1) {
  R();
  P(a0);
  P(a1);
  C(542);
}
void log_Z3_stats_dec_ref(Z3_context a0, Z3_stats a1) {
  R();
  P(a0);
  P(a1);
  C(543);
}
void log_Z3_stats_size(Z3_context a0, Z3_stats a1) {
  R();
  P(a0);
  P(a1);
  C(544);
}
void log_Z3_stats_get_key(Z3_context a0, Z3_stats a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(545);
}
void log_Z3_stats_is_uint(Z3_context a0, Z3_stats a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(546);
}
void log_Z3_stats_is_double(Z3_context a0, Z3_stats a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(547);
}
void log_Z3_stats_get_uint_value(Z3_context a0, Z3_stats a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(548);
}
void log_Z3_stats_get_double_value(Z3_context a0, Z3_stats a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(549);
}
void log_Z3_get_estimated_alloc_size() {
  R();
  C(550);
}
void log_Z3_mk_ast_vector(Z3_context a0) {
  R();
  P(a0);
  C(551);
}
void log_Z3_ast_vector_inc_ref(Z3_context a0, Z3_ast_vector a1) {
  R();
  P(a0);
  P(a1);
  C(552);
}
void log_Z3_ast_vector_dec_ref(Z3_context a0, Z3_ast_vector a1) {
  R();
  P(a0);
  P(a1);
  C(553);
}
void log_Z3_ast_vector_size(Z3_context a0, Z3_ast_vector a1) {
  R();
  P(a0);
  P(a1);
  C(554);
}
void log_Z3_ast_vector_get(Z3_context a0, Z3_ast_vector a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(555);
}
void log_Z3_ast_vector_set(Z3_context a0, Z3_ast_vector a1, unsigned a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  P(a3);
  C(556);
}
void log_Z3_ast_vector_resize(Z3_context a0, Z3_ast_vector a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(557);
}
void log_Z3_ast_vector_push(Z3_context a0, Z3_ast_vector a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(558);
}
void log_Z3_ast_vector_translate(Z3_context a0, Z3_ast_vector a1, Z3_context a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(559);
}
void log_Z3_ast_vector_to_string(Z3_context a0, Z3_ast_vector a1) {
  R();
  P(a0);
  P(a1);
  C(560);
}
void log_Z3_mk_ast_map(Z3_context a0) {
  R();
  P(a0);
  C(561);
}
void log_Z3_ast_map_inc_ref(Z3_context a0, Z3_ast_map a1) {
  R();
  P(a0);
  P(a1);
  C(562);
}
void log_Z3_ast_map_dec_ref(Z3_context a0, Z3_ast_map a1) {
  R();
  P(a0);
  P(a1);
  C(563);
}
void log_Z3_ast_map_contains(Z3_context a0, Z3_ast_map a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(564);
}
void log_Z3_ast_map_find(Z3_context a0, Z3_ast_map a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(565);
}
void log_Z3_ast_map_insert(Z3_context a0, Z3_ast_map a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(566);
}
void log_Z3_ast_map_erase(Z3_context a0, Z3_ast_map a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(567);
}
void log_Z3_ast_map_reset(Z3_context a0, Z3_ast_map a1) {
  R();
  P(a0);
  P(a1);
  C(568);
}
void log_Z3_ast_map_size(Z3_context a0, Z3_ast_map a1) {
  R();
  P(a0);
  P(a1);
  C(569);
}
void log_Z3_ast_map_keys(Z3_context a0, Z3_ast_map a1) {
  R();
  P(a0);
  P(a1);
  C(570);
}
void log_Z3_ast_map_to_string(Z3_context a0, Z3_ast_map a1) {
  R();
  P(a0);
  P(a1);
  C(571);
}
void log_Z3_algebraic_is_value(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(572);
}
void log_Z3_algebraic_is_pos(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(573);
}
void log_Z3_algebraic_is_neg(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(574);
}
void log_Z3_algebraic_is_zero(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(575);
}
void log_Z3_algebraic_sign(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(576);
}
void log_Z3_algebraic_add(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(577);
}
void log_Z3_algebraic_sub(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(578);
}
void log_Z3_algebraic_mul(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(579);
}
void log_Z3_algebraic_div(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(580);
}
void log_Z3_algebraic_root(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(581);
}
void log_Z3_algebraic_power(Z3_context a0, Z3_ast a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(582);
}
void log_Z3_algebraic_lt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(583);
}
void log_Z3_algebraic_gt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(584);
}
void log_Z3_algebraic_le(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(585);
}
void log_Z3_algebraic_ge(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(586);
}
void log_Z3_algebraic_eq(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(587);
}
void log_Z3_algebraic_neq(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(588);
}
void log_Z3_algebraic_roots(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(589);
}
void log_Z3_algebraic_eval(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(590);
}
void log_Z3_algebraic_get_poly(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(591);
}
void log_Z3_algebraic_get_i(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(592);
}
void log_Z3_polynomial_subresultants(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(593);
}
void log_Z3_rcf_del(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(594);
}
void log_Z3_rcf_mk_rational(Z3_context a0, Z3_string a1) {
  R();
  P(a0);
  S(a1);
  C(595);
}
void log_Z3_rcf_mk_small_int(Z3_context a0, int a1) {
  R();
  P(a0);
  I(a1);
  C(596);
}
void log_Z3_rcf_mk_pi(Z3_context a0) {
  R();
  P(a0);
  C(597);
}
void log_Z3_rcf_mk_e(Z3_context a0) {
  R();
  P(a0);
  C(598);
}
void log_Z3_rcf_mk_infinitesimal(Z3_context a0) {
  R();
  P(a0);
  C(599);
}
void log_Z3_rcf_mk_roots(Z3_context a0, unsigned a1, Z3_rcf_num const * a2, Z3_rcf_num* a3) {
  R();
  P(a0);
  U(a1);
  for (unsigned i = 0; i < a1; i++) { P(a2[i]); }
  Ap(a1);
  for (unsigned i = 0; i < a1; i++) { P(0); }
  Ap(a1);
  C(600);
}
void log_Z3_rcf_add(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(601);
}
void log_Z3_rcf_sub(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(602);
}
void log_Z3_rcf_mul(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(603);
}
void log_Z3_rcf_div(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(604);
}
void log_Z3_rcf_neg(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(605);
}
void log_Z3_rcf_inv(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(606);
}
void log_Z3_rcf_power(Z3_context a0, Z3_rcf_num a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(607);
}
void log_Z3_rcf_lt(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(608);
}
void log_Z3_rcf_gt(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(609);
}
void log_Z3_rcf_le(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(610);
}
void log_Z3_rcf_ge(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(611);
}
void log_Z3_rcf_eq(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(612);
}
void log_Z3_rcf_neq(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(613);
}
void log_Z3_rcf_num_to_string(Z3_context a0, Z3_rcf_num a1, bool a2, bool a3) {
  R();
  P(a0);
  P(a1);
  I(a2);
  I(a3);
  C(614);
}
void log_Z3_rcf_num_to_decimal_string(Z3_context a0, Z3_rcf_num a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(615);
}
void log_Z3_rcf_get_numerator_denominator(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num* a2, Z3_rcf_num* a3) {
  R();
  P(a0);
  P(a1);
  P(0);
  P(0);
  C(616);
}
void log_Z3_rcf_is_rational(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(617);
}
void log_Z3_rcf_is_algebraic(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(618);
}
void log_Z3_rcf_is_infinitesimal(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(619);
}
void log_Z3_rcf_is_transcendental(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(620);
}
void log_Z3_rcf_extension_index(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(621);
}
void log_Z3_rcf_transcendental_name(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(622);
}
void log_Z3_rcf_infinitesimal_name(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(623);
}
void log_Z3_rcf_num_coefficients(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(624);
}
void log_Z3_rcf_coefficient(Z3_context a0, Z3_rcf_num a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(625);
}
void log_Z3_rcf_interval(Z3_context a0, Z3_rcf_num a1, int* a2, int* a3, Z3_rcf_num* a4, int* a5, int* a6, Z3_rcf_num* a7) {
  R();
  P(a0);
  P(a1);
  I(0);
  I(0);
  P(0);
  I(0);
  I(0);
  P(0);
  C(626);
}
void log_Z3_rcf_num_sign_conditions(Z3_context a0, Z3_rcf_num a1) {
  R();
  P(a0);
  P(a1);
  C(627);
}
void log_Z3_rcf_sign_condition_sign(Z3_context a0, Z3_rcf_num a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(628);
}
void log_Z3_rcf_num_sign_condition_coefficients(Z3_context a0, Z3_rcf_num a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(629);
}
void log_Z3_rcf_sign_condition_coefficient(Z3_context a0, Z3_rcf_num a1, unsigned a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  U(a3);
  C(630);
}
void log_Z3_mk_fixedpoint(Z3_context a0) {
  R();
  P(a0);
  C(631);
}
void log_Z3_fixedpoint_inc_ref(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(632);
}
void log_Z3_fixedpoint_dec_ref(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(633);
}
void log_Z3_fixedpoint_add_rule(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2, Z3_symbol a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  Sy(a3);
  C(634);
}
void log_Z3_fixedpoint_add_fact(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2, unsigned a3, unsigned const * a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  for (unsigned i = 0; i < a3; i++) { U(a4[i]); }
  Au(a3);
  C(635);
}
void log_Z3_fixedpoint_assert(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(636);
}
void log_Z3_fixedpoint_query(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(637);
}
void log_Z3_fixedpoint_query_relations(Z3_context a0, Z3_fixedpoint a1, unsigned a2, Z3_func_decl const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(638);
}
void log_Z3_fixedpoint_get_answer(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(639);
}
void log_Z3_fixedpoint_get_reason_unknown(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(640);
}
void log_Z3_fixedpoint_update_rule(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2, Z3_symbol a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  Sy(a3);
  C(641);
}
void log_Z3_fixedpoint_get_num_levels(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(642);
}
void log_Z3_fixedpoint_get_cover_delta(Z3_context a0, Z3_fixedpoint a1, int a2, Z3_func_decl a3) {
  R();
  P(a0);
  P(a1);
  I(a2);
  P(a3);
  C(643);
}
void log_Z3_fixedpoint_add_cover(Z3_context a0, Z3_fixedpoint a1, int a2, Z3_func_decl a3, Z3_ast a4) {
  R();
  P(a0);
  P(a1);
  I(a2);
  P(a3);
  P(a4);
  C(644);
}
void log_Z3_fixedpoint_get_statistics(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(645);
}
void log_Z3_fixedpoint_register_relation(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(646);
}
void log_Z3_fixedpoint_set_predicate_representation(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2, unsigned a3, Z3_symbol const * a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  for (unsigned i = 0; i < a3; i++) { Sy(a4[i]); }
  Asy(a3);
  C(647);
}
void log_Z3_fixedpoint_get_rules(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(648);
}
void log_Z3_fixedpoint_get_assertions(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(649);
}
void log_Z3_fixedpoint_set_params(Z3_context a0, Z3_fixedpoint a1, Z3_params a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(650);
}
void log_Z3_fixedpoint_get_help(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(651);
}
void log_Z3_fixedpoint_get_param_descrs(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(652);
}
void log_Z3_fixedpoint_to_string(Z3_context a0, Z3_fixedpoint a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(653);
}
void log_Z3_fixedpoint_from_string(Z3_context a0, Z3_fixedpoint a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(654);
}
void log_Z3_fixedpoint_from_file(Z3_context a0, Z3_fixedpoint a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(655);
}
void log_Z3_mk_optimize(Z3_context a0) {
  R();
  P(a0);
  C(656);
}
void log_Z3_optimize_inc_ref(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(657);
}
void log_Z3_optimize_dec_ref(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(658);
}
void log_Z3_optimize_assert(Z3_context a0, Z3_optimize a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(659);
}
void log_Z3_optimize_assert_and_track(Z3_context a0, Z3_optimize a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(660);
}
void log_Z3_optimize_assert_soft(Z3_context a0, Z3_optimize a1, Z3_ast a2, Z3_string a3, Z3_symbol a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  S(a3);
  Sy(a4);
  C(661);
}
void log_Z3_optimize_maximize(Z3_context a0, Z3_optimize a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(662);
}
void log_Z3_optimize_minimize(Z3_context a0, Z3_optimize a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(663);
}
void log_Z3_optimize_push(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(664);
}
void log_Z3_optimize_pop(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(665);
}
void log_Z3_optimize_set_initial_value(Z3_context a0, Z3_optimize a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(666);
}
void log_Z3_optimize_check(Z3_context a0, Z3_optimize a1, unsigned a2, Z3_ast const * a3) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  C(667);
}
void log_Z3_optimize_get_reason_unknown(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(668);
}
void log_Z3_optimize_get_model(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(669);
}
void log_Z3_optimize_get_unsat_core(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(670);
}
void log_Z3_optimize_set_params(Z3_context a0, Z3_optimize a1, Z3_params a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(671);
}
void log_Z3_optimize_get_param_descrs(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(672);
}
void log_Z3_optimize_get_lower(Z3_context a0, Z3_optimize a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(673);
}
void log_Z3_optimize_get_upper(Z3_context a0, Z3_optimize a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(674);
}
void log_Z3_optimize_get_lower_as_vector(Z3_context a0, Z3_optimize a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(675);
}
void log_Z3_optimize_get_upper_as_vector(Z3_context a0, Z3_optimize a1, unsigned a2) {
  R();
  P(a0);
  P(a1);
  U(a2);
  C(676);
}
void log_Z3_optimize_to_string(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(677);
}
void log_Z3_optimize_from_string(Z3_context a0, Z3_optimize a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(678);
}
void log_Z3_optimize_from_file(Z3_context a0, Z3_optimize a1, Z3_string a2) {
  R();
  P(a0);
  P(a1);
  S(a2);
  C(679);
}
void log_Z3_optimize_get_help(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(680);
}
void log_Z3_optimize_get_statistics(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(681);
}
void log_Z3_optimize_get_assertions(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(682);
}
void log_Z3_optimize_get_objectives(Z3_context a0, Z3_optimize a1) {
  R();
  P(a0);
  P(a1);
  C(683);
}
void log_Z3_mk_fpa_rounding_mode_sort(Z3_context a0) {
  R();
  P(a0);
  C(684);
}
void log_Z3_mk_fpa_round_nearest_ties_to_even(Z3_context a0) {
  R();
  P(a0);
  C(685);
}
void log_Z3_mk_fpa_rne(Z3_context a0) {
  R();
  P(a0);
  C(686);
}
void log_Z3_mk_fpa_round_nearest_ties_to_away(Z3_context a0) {
  R();
  P(a0);
  C(687);
}
void log_Z3_mk_fpa_rna(Z3_context a0) {
  R();
  P(a0);
  C(688);
}
void log_Z3_mk_fpa_round_toward_positive(Z3_context a0) {
  R();
  P(a0);
  C(689);
}
void log_Z3_mk_fpa_rtp(Z3_context a0) {
  R();
  P(a0);
  C(690);
}
void log_Z3_mk_fpa_round_toward_negative(Z3_context a0) {
  R();
  P(a0);
  C(691);
}
void log_Z3_mk_fpa_rtn(Z3_context a0) {
  R();
  P(a0);
  C(692);
}
void log_Z3_mk_fpa_round_toward_zero(Z3_context a0) {
  R();
  P(a0);
  C(693);
}
void log_Z3_mk_fpa_rtz(Z3_context a0) {
  R();
  P(a0);
  C(694);
}
void log_Z3_mk_fpa_sort(Z3_context a0, unsigned a1, unsigned a2) {
  R();
  P(a0);
  U(a1);
  U(a2);
  C(695);
}
void log_Z3_mk_fpa_sort_half(Z3_context a0) {
  R();
  P(a0);
  C(696);
}
void log_Z3_mk_fpa_sort_16(Z3_context a0) {
  R();
  P(a0);
  C(697);
}
void log_Z3_mk_fpa_sort_single(Z3_context a0) {
  R();
  P(a0);
  C(698);
}
void log_Z3_mk_fpa_sort_32(Z3_context a0) {
  R();
  P(a0);
  C(699);
}
void log_Z3_mk_fpa_sort_double(Z3_context a0) {
  R();
  P(a0);
  C(700);
}
void log_Z3_mk_fpa_sort_64(Z3_context a0) {
  R();
  P(a0);
  C(701);
}
void log_Z3_mk_fpa_sort_quadruple(Z3_context a0) {
  R();
  P(a0);
  C(702);
}
void log_Z3_mk_fpa_sort_128(Z3_context a0) {
  R();
  P(a0);
  C(703);
}
void log_Z3_mk_fpa_nan(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(704);
}
void log_Z3_mk_fpa_inf(Z3_context a0, Z3_sort a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(705);
}
void log_Z3_mk_fpa_zero(Z3_context a0, Z3_sort a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(706);
}
void log_Z3_mk_fpa_fp(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(707);
}
void log_Z3_mk_fpa_numeral_float(Z3_context a0, float a1, Z3_sort a2) {
  R();
  P(a0);
  D(a1);
  P(a2);
  C(708);
}
void log_Z3_mk_fpa_numeral_double(Z3_context a0, double a1, Z3_sort a2) {
  R();
  P(a0);
  D(a1);
  P(a2);
  C(709);
}
void log_Z3_mk_fpa_numeral_int(Z3_context a0, int a1, Z3_sort a2) {
  R();
  P(a0);
  I(a1);
  P(a2);
  C(710);
}
void log_Z3_mk_fpa_numeral_int_uint(Z3_context a0, bool a1, int a2, unsigned a3, Z3_sort a4) {
  R();
  P(a0);
  I(a1);
  I(a2);
  U(a3);
  P(a4);
  C(711);
}
void log_Z3_mk_fpa_numeral_int64_uint64(Z3_context a0, bool a1, int64_t a2, uint64_t a3, Z3_sort a4) {
  R();
  P(a0);
  I(a1);
  I(a2);
  U(a3);
  P(a4);
  C(712);
}
void log_Z3_mk_fpa_abs(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(713);
}
void log_Z3_mk_fpa_neg(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(714);
}
void log_Z3_mk_fpa_add(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(715);
}
void log_Z3_mk_fpa_sub(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(716);
}
void log_Z3_mk_fpa_mul(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(717);
}
void log_Z3_mk_fpa_div(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(718);
}
void log_Z3_mk_fpa_fma(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3, Z3_ast a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  P(a4);
  C(719);
}
void log_Z3_mk_fpa_sqrt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(720);
}
void log_Z3_mk_fpa_rem(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(721);
}
void log_Z3_mk_fpa_round_to_integral(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(722);
}
void log_Z3_mk_fpa_min(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(723);
}
void log_Z3_mk_fpa_max(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(724);
}
void log_Z3_mk_fpa_leq(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(725);
}
void log_Z3_mk_fpa_lt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(726);
}
void log_Z3_mk_fpa_geq(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(727);
}
void log_Z3_mk_fpa_gt(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(728);
}
void log_Z3_mk_fpa_eq(Z3_context a0, Z3_ast a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(729);
}
void log_Z3_mk_fpa_is_normal(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(730);
}
void log_Z3_mk_fpa_is_subnormal(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(731);
}
void log_Z3_mk_fpa_is_zero(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(732);
}
void log_Z3_mk_fpa_is_infinite(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(733);
}
void log_Z3_mk_fpa_is_nan(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(734);
}
void log_Z3_mk_fpa_is_negative(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(735);
}
void log_Z3_mk_fpa_is_positive(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(736);
}
void log_Z3_mk_fpa_to_fp_bv(Z3_context a0, Z3_ast a1, Z3_sort a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(737);
}
void log_Z3_mk_fpa_to_fp_float(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(738);
}
void log_Z3_mk_fpa_to_fp_real(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(739);
}
void log_Z3_mk_fpa_to_fp_signed(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(740);
}
void log_Z3_mk_fpa_to_fp_unsigned(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(741);
}
void log_Z3_mk_fpa_to_ubv(Z3_context a0, Z3_ast a1, Z3_ast a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  C(742);
}
void log_Z3_mk_fpa_to_sbv(Z3_context a0, Z3_ast a1, Z3_ast a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  C(743);
}
void log_Z3_mk_fpa_to_real(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(744);
}
void log_Z3_fpa_get_ebits(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(745);
}
void log_Z3_fpa_get_sbits(Z3_context a0, Z3_sort a1) {
  R();
  P(a0);
  P(a1);
  C(746);
}
void log_Z3_fpa_is_numeral_nan(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(747);
}
void log_Z3_fpa_is_numeral_inf(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(748);
}
void log_Z3_fpa_is_numeral_zero(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(749);
}
void log_Z3_fpa_is_numeral_normal(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(750);
}
void log_Z3_fpa_is_numeral_subnormal(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(751);
}
void log_Z3_fpa_is_numeral_positive(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(752);
}
void log_Z3_fpa_is_numeral_negative(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(753);
}
void log_Z3_fpa_get_numeral_sign_bv(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(754);
}
void log_Z3_fpa_get_numeral_significand_bv(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(755);
}
void log_Z3_fpa_get_numeral_sign(Z3_context a0, Z3_ast a1, int* a2) {
  R();
  P(a0);
  P(a1);
  I(0);
  C(756);
}
void log_Z3_fpa_get_numeral_significand_string(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(757);
}
void log_Z3_fpa_get_numeral_significand_uint64(Z3_context a0, Z3_ast a1, uint64_t* a2) {
  R();
  P(a0);
  P(a1);
  U(0);
  C(758);
}
void log_Z3_fpa_get_numeral_exponent_string(Z3_context a0, Z3_ast a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(759);
}
void log_Z3_fpa_get_numeral_exponent_int64(Z3_context a0, Z3_ast a1, int64_t* a2, bool a3) {
  R();
  P(a0);
  P(a1);
  I(0);
  I(a3);
  C(760);
}
void log_Z3_fpa_get_numeral_exponent_bv(Z3_context a0, Z3_ast a1, bool a2) {
  R();
  P(a0);
  P(a1);
  I(a2);
  C(761);
}
void log_Z3_mk_fpa_to_ieee_bv(Z3_context a0, Z3_ast a1) {
  R();
  P(a0);
  P(a1);
  C(762);
}
void log_Z3_mk_fpa_to_fp_int_real(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3, Z3_sort a4) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  P(a4);
  C(763);
}
void log_Z3_fixedpoint_query_from_lvl(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2, unsigned a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  U(a3);
  C(764);
}
void log_Z3_fixedpoint_get_ground_sat_answer(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(765);
}
void log_Z3_fixedpoint_get_rules_along_trace(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(766);
}
void log_Z3_fixedpoint_get_rule_names_along_trace(Z3_context a0, Z3_fixedpoint a1) {
  R();
  P(a0);
  P(a1);
  C(767);
}
void log_Z3_fixedpoint_add_invariant(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2, Z3_ast a3) {
  R();
  P(a0);
  P(a1);
  P(a2);
  P(a3);
  C(768);
}
void log_Z3_fixedpoint_get_reachable(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(769);
}
void log_Z3_qe_model_project(Z3_context a0, Z3_model a1, unsigned a2, Z3_app const * a3, Z3_ast a4) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  C(770);
}
void log_Z3_qe_model_project_skolem(Z3_context a0, Z3_model a1, unsigned a2, Z3_app const * a3, Z3_ast a4, Z3_ast_map a5) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  P(a5);
  C(771);
}
void log_Z3_qe_model_project_with_witness(Z3_context a0, Z3_model a1, unsigned a2, Z3_app const * a3, Z3_ast a4, Z3_ast_map a5) {
  R();
  P(a0);
  P(a1);
  U(a2);
  for (unsigned i = 0; i < a2; i++) { P(a3[i]); }
  Ap(a2);
  P(a4);
  P(a5);
  C(772);
}
void log_Z3_model_extrapolate(Z3_context a0, Z3_model a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(773);
}
void log_Z3_qe_lite(Z3_context a0, Z3_ast_vector a1, Z3_ast a2) {
  R();
  P(a0);
  P(a1);
  P(a2);
  C(774);
}

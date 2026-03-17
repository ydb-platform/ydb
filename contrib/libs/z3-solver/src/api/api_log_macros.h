// Automatically generated file
#include"api/z3.h"
#ifdef __GNUC__
#define _Z3_UNUSED __attribute__((unused))
#else
#define _Z3_UNUSED
#endif
#include "util/mutex.h"
extern atomic<bool> g_z3_log_enabled;
void ctx_enable_logging();
class z3_log_ctx { bool m_prev; public: z3_log_ctx() { ATOMIC_EXCHANGE(m_prev, g_z3_log_enabled, false); } ~z3_log_ctx() { if (m_prev) [[unlikely]] g_z3_log_enabled = true; } bool enabled() const { return m_prev; } };
void SetR(const void * obj);
void SetO(void * obj, unsigned pos);
void SetAO(void * obj, unsigned pos, unsigned idx);
#define RETURN_Z3(Z3RES) do { auto tmp_ret = Z3RES; if (_LOG_CTX.enabled()) [[unlikely]] { SetR(tmp_ret); } return tmp_ret; } while (0)
void log_Z3_global_param_set(Z3_string a0, Z3_string a1);
#define LOG_Z3_global_param_set(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_global_param_set(_ARG0, _ARG1); }
void log_Z3_global_param_reset_all();
#define LOG_Z3_global_param_reset_all() z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_global_param_reset_all(); }
void log_Z3_global_param_get(Z3_string a0, Z3_string* a1);
#define LOG_Z3_global_param_get(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_global_param_get(_ARG0, _ARG1); }
void log_Z3_mk_config();
#define LOG_Z3_mk_config() z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_config(); }
void log_Z3_del_config(Z3_config a0);
#define LOG_Z3_del_config(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_del_config(_ARG0); }
void log_Z3_set_param_value(Z3_config a0, Z3_string a1, Z3_string a2);
#define LOG_Z3_set_param_value(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_set_param_value(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_context(Z3_config a0);
#define LOG_Z3_mk_context(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_context(_ARG0); }
void log_Z3_mk_context_rc(Z3_config a0);
#define LOG_Z3_mk_context_rc(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_context_rc(_ARG0); }
void log_Z3_del_context(Z3_context a0);
#define LOG_Z3_del_context(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_del_context(_ARG0); }
void log_Z3_inc_ref(Z3_context a0, Z3_ast a1);
#define LOG_Z3_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_inc_ref(_ARG0, _ARG1); }
void log_Z3_dec_ref(Z3_context a0, Z3_ast a1);
#define LOG_Z3_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_dec_ref(_ARG0, _ARG1); }
void log_Z3_update_param_value(Z3_context a0, Z3_string a1, Z3_string a2);
#define LOG_Z3_update_param_value(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_update_param_value(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_global_param_descrs(Z3_context a0);
#define LOG_Z3_get_global_param_descrs(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_global_param_descrs(_ARG0); }
void log_Z3_interrupt(Z3_context a0);
#define LOG_Z3_interrupt(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_interrupt(_ARG0); }
void log_Z3_enable_concurrent_dec_ref(Z3_context a0);
#define LOG_Z3_enable_concurrent_dec_ref(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_enable_concurrent_dec_ref(_ARG0); }
void log_Z3_mk_params(Z3_context a0);
#define LOG_Z3_mk_params(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_params(_ARG0); }
void log_Z3_params_inc_ref(Z3_context a0, Z3_params a1);
#define LOG_Z3_params_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_inc_ref(_ARG0, _ARG1); }
void log_Z3_params_dec_ref(Z3_context a0, Z3_params a1);
#define LOG_Z3_params_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_dec_ref(_ARG0, _ARG1); }
void log_Z3_params_set_bool(Z3_context a0, Z3_params a1, Z3_symbol a2, bool a3);
#define LOG_Z3_params_set_bool(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_set_bool(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_params_set_uint(Z3_context a0, Z3_params a1, Z3_symbol a2, unsigned a3);
#define LOG_Z3_params_set_uint(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_set_uint(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_params_set_double(Z3_context a0, Z3_params a1, Z3_symbol a2, double a3);
#define LOG_Z3_params_set_double(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_set_double(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_params_set_symbol(Z3_context a0, Z3_params a1, Z3_symbol a2, Z3_symbol a3);
#define LOG_Z3_params_set_symbol(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_set_symbol(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_params_to_string(Z3_context a0, Z3_params a1);
#define LOG_Z3_params_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_to_string(_ARG0, _ARG1); }
void log_Z3_params_validate(Z3_context a0, Z3_params a1, Z3_param_descrs a2);
#define LOG_Z3_params_validate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_params_validate(_ARG0, _ARG1, _ARG2); }
void log_Z3_param_descrs_inc_ref(Z3_context a0, Z3_param_descrs a1);
#define LOG_Z3_param_descrs_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_inc_ref(_ARG0, _ARG1); }
void log_Z3_param_descrs_dec_ref(Z3_context a0, Z3_param_descrs a1);
#define LOG_Z3_param_descrs_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_dec_ref(_ARG0, _ARG1); }
void log_Z3_param_descrs_get_kind(Z3_context a0, Z3_param_descrs a1, Z3_symbol a2);
#define LOG_Z3_param_descrs_get_kind(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_get_kind(_ARG0, _ARG1, _ARG2); }
void log_Z3_param_descrs_size(Z3_context a0, Z3_param_descrs a1);
#define LOG_Z3_param_descrs_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_size(_ARG0, _ARG1); }
void log_Z3_param_descrs_get_name(Z3_context a0, Z3_param_descrs a1, unsigned a2);
#define LOG_Z3_param_descrs_get_name(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_get_name(_ARG0, _ARG1, _ARG2); }
void log_Z3_param_descrs_get_documentation(Z3_context a0, Z3_param_descrs a1, Z3_symbol a2);
#define LOG_Z3_param_descrs_get_documentation(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_get_documentation(_ARG0, _ARG1, _ARG2); }
void log_Z3_param_descrs_to_string(Z3_context a0, Z3_param_descrs a1);
#define LOG_Z3_param_descrs_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_param_descrs_to_string(_ARG0, _ARG1); }
void log_Z3_mk_int_symbol(Z3_context a0, int a1);
#define LOG_Z3_mk_int_symbol(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int_symbol(_ARG0, _ARG1); }
void log_Z3_mk_string_symbol(Z3_context a0, Z3_string a1);
#define LOG_Z3_mk_string_symbol(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_string_symbol(_ARG0, _ARG1); }
void log_Z3_mk_uninterpreted_sort(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_mk_uninterpreted_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_uninterpreted_sort(_ARG0, _ARG1); }
void log_Z3_mk_type_variable(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_mk_type_variable(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_type_variable(_ARG0, _ARG1); }
void log_Z3_mk_bool_sort(Z3_context a0);
#define LOG_Z3_mk_bool_sort(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bool_sort(_ARG0); }
void log_Z3_mk_int_sort(Z3_context a0);
#define LOG_Z3_mk_int_sort(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int_sort(_ARG0); }
void log_Z3_mk_real_sort(Z3_context a0);
#define LOG_Z3_mk_real_sort(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_real_sort(_ARG0); }
void log_Z3_mk_bv_sort(Z3_context a0, unsigned a1);
#define LOG_Z3_mk_bv_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bv_sort(_ARG0, _ARG1); }
void log_Z3_mk_finite_domain_sort(Z3_context a0, Z3_symbol a1, uint64_t a2);
#define LOG_Z3_mk_finite_domain_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_finite_domain_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_array_sort(Z3_context a0, Z3_sort a1, Z3_sort a2);
#define LOG_Z3_mk_array_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_array_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_array_sort_n(Z3_context a0, unsigned a1, Z3_sort const * a2, Z3_sort a3);
#define LOG_Z3_mk_array_sort_n(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_array_sort_n(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_tuple_sort(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_symbol const * a3, Z3_sort const * a4, Z3_func_decl* a5, Z3_func_decl* a6);
#define LOG_Z3_mk_tuple_sort(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6) z3_log_ctx _LOG_CTX; Z3_func_decl* _Z3_UNUSED Z3ARG5 = 0; unsigned _Z3_UNUSED Z3ARG2 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG6 = 0; if (_LOG_CTX.enabled()) { log_Z3_mk_tuple_sort(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6); Z3ARG5 = _ARG5; Z3ARG2 = _ARG2; Z3ARG6 = _ARG6; }
#define RETURN_Z3_mk_tuple_sort(Z3RES) if (_LOG_CTX.enabled()) { SetR(Z3RES); SetO((Z3ARG5 == 0 ? 0 : *Z3ARG5), 5); for (unsigned i = 0; i < Z3ARG2; i++) { SetAO(Z3ARG6[i], 6, i); } } return Z3RES
void log_Z3_mk_enumeration_sort(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_symbol const * a3, Z3_func_decl* a4, Z3_func_decl* a5);
#define LOG_Z3_mk_enumeration_sort(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5) z3_log_ctx _LOG_CTX; unsigned _Z3_UNUSED Z3ARG2 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG4 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG5 = 0; if (_LOG_CTX.enabled()) { log_Z3_mk_enumeration_sort(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5); Z3ARG2 = _ARG2; Z3ARG4 = _ARG4; Z3ARG5 = _ARG5; }
#define RETURN_Z3_mk_enumeration_sort(Z3RES) if (_LOG_CTX.enabled()) { SetR(Z3RES); for (unsigned i = 0; i < Z3ARG2; i++) { SetAO(Z3ARG4[i], 4, i); } for (unsigned i = 0; i < Z3ARG2; i++) { SetAO(Z3ARG5[i], 5, i); } } return Z3RES
void log_Z3_mk_list_sort(Z3_context a0, Z3_symbol a1, Z3_sort a2, Z3_func_decl* a3, Z3_func_decl* a4, Z3_func_decl* a5, Z3_func_decl* a6, Z3_func_decl* a7, Z3_func_decl* a8);
#define LOG_Z3_mk_list_sort(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8) z3_log_ctx _LOG_CTX; Z3_func_decl* _Z3_UNUSED Z3ARG3 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG4 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG5 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG6 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG7 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG8 = 0; if (_LOG_CTX.enabled()) { log_Z3_mk_list_sort(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8); Z3ARG3 = _ARG3; Z3ARG4 = _ARG4; Z3ARG5 = _ARG5; Z3ARG6 = _ARG6; Z3ARG7 = _ARG7; Z3ARG8 = _ARG8; }
#define RETURN_Z3_mk_list_sort(Z3RES) if (_LOG_CTX.enabled()) { SetR(Z3RES); SetO((Z3ARG3 == 0 ? 0 : *Z3ARG3), 3); SetO((Z3ARG4 == 0 ? 0 : *Z3ARG4), 4); SetO((Z3ARG5 == 0 ? 0 : *Z3ARG5), 5); SetO((Z3ARG6 == 0 ? 0 : *Z3ARG6), 6); SetO((Z3ARG7 == 0 ? 0 : *Z3ARG7), 7); SetO((Z3ARG8 == 0 ? 0 : *Z3ARG8), 8); } return Z3RES
void log_Z3_mk_constructor(Z3_context a0, Z3_symbol a1, Z3_symbol a2, unsigned a3, Z3_symbol const * a4, Z3_sort const * a5, unsigned const * a6);
#define LOG_Z3_mk_constructor(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_constructor(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6); }
void log_Z3_constructor_num_fields(Z3_context a0, Z3_constructor a1);
#define LOG_Z3_constructor_num_fields(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_constructor_num_fields(_ARG0, _ARG1); }
void log_Z3_del_constructor(Z3_context a0, Z3_constructor a1);
#define LOG_Z3_del_constructor(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_del_constructor(_ARG0, _ARG1); }
void log_Z3_mk_datatype(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_constructor* a3);
#define LOG_Z3_mk_datatype(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; unsigned _Z3_UNUSED Z3ARG2 = 0; Z3_constructor* _Z3_UNUSED Z3ARG3 = 0; if (_LOG_CTX.enabled()) { log_Z3_mk_datatype(_ARG0, _ARG1, _ARG2, _ARG3); Z3ARG2 = _ARG2; Z3ARG3 = _ARG3; }
#define RETURN_Z3_mk_datatype(Z3RES) if (_LOG_CTX.enabled()) { SetR(Z3RES); for (unsigned i = 0; i < Z3ARG2; i++) { SetAO(Z3ARG3[i], 3, i); } } return Z3RES
void log_Z3_mk_datatype_sort(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_mk_datatype_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_datatype_sort(_ARG0, _ARG1); }
void log_Z3_mk_constructor_list(Z3_context a0, unsigned a1, Z3_constructor const * a2);
#define LOG_Z3_mk_constructor_list(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_constructor_list(_ARG0, _ARG1, _ARG2); }
void log_Z3_del_constructor_list(Z3_context a0, Z3_constructor_list a1);
#define LOG_Z3_del_constructor_list(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_del_constructor_list(_ARG0, _ARG1); }
void log_Z3_mk_datatypes(Z3_context a0, unsigned a1, Z3_symbol const * a2, Z3_sort* a3, Z3_constructor_list* a4);
#define LOG_Z3_mk_datatypes(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; unsigned _Z3_UNUSED Z3ARG1 = 0; Z3_sort* _Z3_UNUSED Z3ARG3 = 0; Z3_constructor_list* _Z3_UNUSED Z3ARG4 = 0; if (_LOG_CTX.enabled()) { log_Z3_mk_datatypes(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); Z3ARG1 = _ARG1; Z3ARG3 = _ARG3; Z3ARG4 = _ARG4; }
#define RETURN_Z3_mk_datatypes if (_LOG_CTX.enabled()) { for (unsigned i = 0; i < Z3ARG1; i++) { SetAO(Z3ARG3[i], 3, i); } for (unsigned i = 0; i < Z3ARG1; i++) { SetAO(Z3ARG4[i], 4, i); } } return
void log_Z3_query_constructor(Z3_context a0, Z3_constructor a1, unsigned a2, Z3_func_decl* a3, Z3_func_decl* a4, Z3_func_decl* a5);
#define LOG_Z3_query_constructor(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5) z3_log_ctx _LOG_CTX; Z3_func_decl* _Z3_UNUSED Z3ARG3 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG4 = 0; unsigned _Z3_UNUSED Z3ARG2 = 0; Z3_func_decl* _Z3_UNUSED Z3ARG5 = 0; if (_LOG_CTX.enabled()) { log_Z3_query_constructor(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5); Z3ARG3 = _ARG3; Z3ARG4 = _ARG4; Z3ARG2 = _ARG2; Z3ARG5 = _ARG5; }
#define RETURN_Z3_query_constructor if (_LOG_CTX.enabled()) { SetO((Z3ARG3 == 0 ? 0 : *Z3ARG3), 3); SetO((Z3ARG4 == 0 ? 0 : *Z3ARG4), 4); for (unsigned i = 0; i < Z3ARG2; i++) { SetAO(Z3ARG5[i], 5, i); } } return
void log_Z3_mk_func_decl(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_sort const * a3, Z3_sort a4);
#define LOG_Z3_mk_func_decl(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_func_decl(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_app(Z3_context a0, Z3_func_decl a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_mk_app(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_app(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_const(Z3_context a0, Z3_symbol a1, Z3_sort a2);
#define LOG_Z3_mk_const(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_const(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fresh_func_decl(Z3_context a0, Z3_string a1, unsigned a2, Z3_sort const * a3, Z3_sort a4);
#define LOG_Z3_mk_fresh_func_decl(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fresh_func_decl(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_fresh_const(Z3_context a0, Z3_string a1, Z3_sort a2);
#define LOG_Z3_mk_fresh_const(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fresh_const(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_rec_func_decl(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_sort const * a3, Z3_sort a4);
#define LOG_Z3_mk_rec_func_decl(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_rec_func_decl(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_add_rec_def(Z3_context a0, Z3_func_decl a1, unsigned a2, Z3_ast const * a3, Z3_ast a4);
#define LOG_Z3_add_rec_def(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_add_rec_def(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_true(Z3_context a0);
#define LOG_Z3_mk_true(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_true(_ARG0); }
void log_Z3_mk_false(Z3_context a0);
#define LOG_Z3_mk_false(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_false(_ARG0); }
void log_Z3_mk_eq(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_eq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_eq(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_distinct(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_distinct(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_distinct(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_not(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_not(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_not(_ARG0, _ARG1); }
void log_Z3_mk_ite(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_ite(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ite(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_iff(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_iff(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_iff(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_implies(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_implies(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_implies(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_xor(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_xor(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_xor(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_and(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_and(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_and(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_or(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_or(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_or(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_add(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_add(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_add(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_mul(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_mul(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_mul(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_sub(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_sub(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_sub(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_unary_minus(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_unary_minus(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_unary_minus(_ARG0, _ARG1); }
void log_Z3_mk_div(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_div(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_div(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_mod(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_mod(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_mod(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_rem(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_rem(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_rem(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_power(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_power(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_power(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_abs(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_abs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_abs(_ARG0, _ARG1); }
void log_Z3_mk_lt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_lt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_lt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_le(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_le(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_le(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_gt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_gt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_gt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_ge(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_ge(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ge(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_divides(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_divides(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_divides(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_int2real(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_int2real(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int2real(_ARG0, _ARG1); }
void log_Z3_mk_real2int(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_real2int(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_real2int(_ARG0, _ARG1); }
void log_Z3_mk_is_int(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_is_int(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_is_int(_ARG0, _ARG1); }
void log_Z3_mk_bvnot(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_bvnot(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvnot(_ARG0, _ARG1); }
void log_Z3_mk_bvredand(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_bvredand(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvredand(_ARG0, _ARG1); }
void log_Z3_mk_bvredor(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_bvredor(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvredor(_ARG0, _ARG1); }
void log_Z3_mk_bvand(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvand(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvand(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvor(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvor(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvor(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvxor(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvxor(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvxor(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvnand(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvnand(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvnand(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvnor(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvnor(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvnor(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvxnor(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvxnor(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvxnor(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvneg(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_bvneg(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvneg(_ARG0, _ARG1); }
void log_Z3_mk_bvadd(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvadd(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvadd(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsub(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsub(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsub(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvmul(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvmul(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvmul(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvudiv(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvudiv(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvudiv(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsdiv(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsdiv(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsdiv(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvurem(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvurem(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvurem(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsrem(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsrem(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsrem(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsmod(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsmod(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsmod(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvult(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvult(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvult(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvslt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvslt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvslt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvule(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvule(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvule(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsle(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsle(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsle(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvuge(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvuge(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvuge(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsge(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsge(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsge(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvugt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvugt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvugt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsgt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsgt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsgt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_concat(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_concat(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_concat(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_extract(Z3_context a0, unsigned a1, unsigned a2, Z3_ast a3);
#define LOG_Z3_mk_extract(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_extract(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_sign_ext(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_sign_ext(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_sign_ext(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_zero_ext(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_zero_ext(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_zero_ext(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_repeat(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_repeat(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_repeat(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bit2bool(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_bit2bool(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bit2bool(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvshl(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvshl(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvshl(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvlshr(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvlshr(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvlshr(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvashr(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvashr(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvashr(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_rotate_left(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_rotate_left(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_rotate_left(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_rotate_right(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_rotate_right(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_rotate_right(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_ext_rotate_left(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_ext_rotate_left(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ext_rotate_left(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_ext_rotate_right(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_ext_rotate_right(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ext_rotate_right(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_int2bv(Z3_context a0, unsigned a1, Z3_ast a2);
#define LOG_Z3_mk_int2bv(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int2bv(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bv2int(Z3_context a0, Z3_ast a1, bool a2);
#define LOG_Z3_mk_bv2int(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bv2int(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvadd_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2, bool a3);
#define LOG_Z3_mk_bvadd_no_overflow(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvadd_no_overflow(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_bvadd_no_underflow(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvadd_no_underflow(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvadd_no_underflow(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsub_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsub_no_overflow(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsub_no_overflow(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvsub_no_underflow(Z3_context a0, Z3_ast a1, Z3_ast a2, bool a3);
#define LOG_Z3_mk_bvsub_no_underflow(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsub_no_underflow(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_bvsdiv_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvsdiv_no_overflow(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvsdiv_no_overflow(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bvneg_no_overflow(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_bvneg_no_overflow(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvneg_no_overflow(_ARG0, _ARG1); }
void log_Z3_mk_bvmul_no_overflow(Z3_context a0, Z3_ast a1, Z3_ast a2, bool a3);
#define LOG_Z3_mk_bvmul_no_overflow(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvmul_no_overflow(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_bvmul_no_underflow(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_bvmul_no_underflow(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bvmul_no_underflow(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_select(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_select(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_select(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_select_n(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_mk_select_n(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_select_n(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_store(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_store(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_store(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_store_n(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3, Z3_ast a4);
#define LOG_Z3_mk_store_n(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_store_n(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_const_array(Z3_context a0, Z3_sort a1, Z3_ast a2);
#define LOG_Z3_mk_const_array(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_const_array(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_map(Z3_context a0, Z3_func_decl a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_mk_map(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_map(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_array_default(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_array_default(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_array_default(_ARG0, _ARG1); }
void log_Z3_mk_as_array(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_mk_as_array(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_as_array(_ARG0, _ARG1); }
void log_Z3_mk_set_has_size(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_set_has_size(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_has_size(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_set_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_sort(_ARG0, _ARG1); }
void log_Z3_mk_empty_set(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_empty_set(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_empty_set(_ARG0, _ARG1); }
void log_Z3_mk_full_set(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_full_set(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_full_set(_ARG0, _ARG1); }
void log_Z3_mk_set_add(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_set_add(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_add(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_del(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_set_del(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_del(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_union(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_set_union(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_union(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_intersect(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_set_intersect(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_intersect(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_difference(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_set_difference(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_difference(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_complement(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_set_complement(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_complement(_ARG0, _ARG1); }
void log_Z3_mk_set_member(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_set_member(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_member(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_set_subset(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_set_subset(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_set_subset(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_array_ext(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_array_ext(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_array_ext(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_numeral(Z3_context a0, Z3_string a1, Z3_sort a2);
#define LOG_Z3_mk_numeral(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_numeral(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_real(Z3_context a0, int a1, int a2);
#define LOG_Z3_mk_real(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_real(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_real_int64(Z3_context a0, int64_t a1, int64_t a2);
#define LOG_Z3_mk_real_int64(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_real_int64(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_int(Z3_context a0, int a1, Z3_sort a2);
#define LOG_Z3_mk_int(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_unsigned_int(Z3_context a0, unsigned a1, Z3_sort a2);
#define LOG_Z3_mk_unsigned_int(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_unsigned_int(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_int64(Z3_context a0, int64_t a1, Z3_sort a2);
#define LOG_Z3_mk_int64(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int64(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_unsigned_int64(Z3_context a0, uint64_t a1, Z3_sort a2);
#define LOG_Z3_mk_unsigned_int64(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_unsigned_int64(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bv_numeral(Z3_context a0, unsigned a1, bool const * a2);
#define LOG_Z3_mk_bv_numeral(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bv_numeral(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_seq_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_sort(_ARG0, _ARG1); }
void log_Z3_is_seq_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_is_seq_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_seq_sort(_ARG0, _ARG1); }
void log_Z3_get_seq_sort_basis(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_seq_sort_basis(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_seq_sort_basis(_ARG0, _ARG1); }
void log_Z3_mk_re_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_re_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_sort(_ARG0, _ARG1); }
void log_Z3_is_re_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_is_re_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_re_sort(_ARG0, _ARG1); }
void log_Z3_get_re_sort_basis(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_re_sort_basis(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_re_sort_basis(_ARG0, _ARG1); }
void log_Z3_mk_string_sort(Z3_context a0);
#define LOG_Z3_mk_string_sort(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_string_sort(_ARG0); }
void log_Z3_mk_char_sort(Z3_context a0);
#define LOG_Z3_mk_char_sort(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char_sort(_ARG0); }
void log_Z3_is_string_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_is_string_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_string_sort(_ARG0, _ARG1); }
void log_Z3_is_char_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_is_char_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_char_sort(_ARG0, _ARG1); }
void log_Z3_mk_string(Z3_context a0, Z3_string a1);
#define LOG_Z3_mk_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_string(_ARG0, _ARG1); }
void log_Z3_mk_lstring(Z3_context a0, unsigned a1, Z3_string a2);
#define LOG_Z3_mk_lstring(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_lstring(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_u32string(Z3_context a0, unsigned a1, unsigned const * a2);
#define LOG_Z3_mk_u32string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_u32string(_ARG0, _ARG1, _ARG2); }
void log_Z3_is_string(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_string(_ARG0, _ARG1); }
void log_Z3_get_string(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_string(_ARG0, _ARG1); }
void log_Z3_get_lstring(Z3_context a0, Z3_ast a1, unsigned* a2);
#define LOG_Z3_get_lstring(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_lstring(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_string_length(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_string_length(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_string_length(_ARG0, _ARG1); }
void log_Z3_get_string_contents(Z3_context a0, Z3_ast a1, unsigned a2, unsigned* a3);
#define LOG_Z3_get_string_contents(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_string_contents(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_seq_empty(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_seq_empty(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_empty(_ARG0, _ARG1); }
void log_Z3_mk_seq_unit(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_seq_unit(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_unit(_ARG0, _ARG1); }
void log_Z3_mk_seq_concat(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_seq_concat(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_concat(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_prefix(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_prefix(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_prefix(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_suffix(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_suffix(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_suffix(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_contains(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_contains(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_contains(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_str_lt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_str_lt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_str_lt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_str_le(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_str_le(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_str_le(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_extract(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_seq_extract(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_extract(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_seq_replace(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_seq_replace(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_replace(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_seq_at(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_at(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_at(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_nth(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_nth(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_nth(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_length(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_seq_length(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_length(_ARG0, _ARG1); }
void log_Z3_mk_seq_index(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_seq_index(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_index(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_seq_last_index(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_last_index(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_last_index(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_map(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_map(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_map(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_seq_mapi(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_seq_mapi(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_mapi(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_seq_foldl(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_seq_foldl(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_foldl(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_seq_foldli(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3, Z3_ast a4);
#define LOG_Z3_mk_seq_foldli(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_foldli(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_str_to_int(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_str_to_int(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_str_to_int(_ARG0, _ARG1); }
void log_Z3_mk_int_to_str(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_int_to_str(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_int_to_str(_ARG0, _ARG1); }
void log_Z3_mk_string_to_code(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_string_to_code(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_string_to_code(_ARG0, _ARG1); }
void log_Z3_mk_string_from_code(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_string_from_code(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_string_from_code(_ARG0, _ARG1); }
void log_Z3_mk_ubv_to_str(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_ubv_to_str(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ubv_to_str(_ARG0, _ARG1); }
void log_Z3_mk_sbv_to_str(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_sbv_to_str(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_sbv_to_str(_ARG0, _ARG1); }
void log_Z3_mk_seq_to_re(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_seq_to_re(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_to_re(_ARG0, _ARG1); }
void log_Z3_mk_seq_in_re(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_seq_in_re(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_seq_in_re(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_plus(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_re_plus(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_plus(_ARG0, _ARG1); }
void log_Z3_mk_re_star(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_re_star(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_star(_ARG0, _ARG1); }
void log_Z3_mk_re_option(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_re_option(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_option(_ARG0, _ARG1); }
void log_Z3_mk_re_union(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_re_union(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_union(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_concat(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_re_concat(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_concat(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_range(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_re_range(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_range(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_allchar(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_re_allchar(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_allchar(_ARG0, _ARG1); }
void log_Z3_mk_re_loop(Z3_context a0, Z3_ast a1, unsigned a2, unsigned a3);
#define LOG_Z3_mk_re_loop(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_loop(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_re_power(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_mk_re_power(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_power(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_intersect(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_re_intersect(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_intersect(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_complement(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_re_complement(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_complement(_ARG0, _ARG1); }
void log_Z3_mk_re_diff(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_re_diff(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_diff(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_re_empty(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_re_empty(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_empty(_ARG0, _ARG1); }
void log_Z3_mk_re_full(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_re_full(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_re_full(_ARG0, _ARG1); }
void log_Z3_mk_char(Z3_context a0, unsigned a1);
#define LOG_Z3_mk_char(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char(_ARG0, _ARG1); }
void log_Z3_mk_char_le(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_char_le(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char_le(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_char_to_int(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_char_to_int(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char_to_int(_ARG0, _ARG1); }
void log_Z3_mk_char_to_bv(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_char_to_bv(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char_to_bv(_ARG0, _ARG1); }
void log_Z3_mk_char_from_bv(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_char_from_bv(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char_from_bv(_ARG0, _ARG1); }
void log_Z3_mk_char_is_digit(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_char_is_digit(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_char_is_digit(_ARG0, _ARG1); }
void log_Z3_mk_linear_order(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_mk_linear_order(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_linear_order(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_partial_order(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_mk_partial_order(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_partial_order(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_piecewise_linear_order(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_mk_piecewise_linear_order(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_piecewise_linear_order(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_tree_order(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_mk_tree_order(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_tree_order(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_transitive_closure(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_mk_transitive_closure(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_transitive_closure(_ARG0, _ARG1); }
void log_Z3_mk_pattern(Z3_context a0, unsigned a1, Z3_ast const * a2);
#define LOG_Z3_mk_pattern(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_pattern(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_bound(Z3_context a0, unsigned a1, Z3_sort a2);
#define LOG_Z3_mk_bound(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_bound(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_forall(Z3_context a0, unsigned a1, unsigned a2, Z3_pattern const * a3, unsigned a4, Z3_sort const * a5, Z3_symbol const * a6, Z3_ast a7);
#define LOG_Z3_mk_forall(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_forall(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_mk_exists(Z3_context a0, unsigned a1, unsigned a2, Z3_pattern const * a3, unsigned a4, Z3_sort const * a5, Z3_symbol const * a6, Z3_ast a7);
#define LOG_Z3_mk_exists(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_exists(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_mk_quantifier(Z3_context a0, bool a1, unsigned a2, unsigned a3, Z3_pattern const * a4, unsigned a5, Z3_sort const * a6, Z3_symbol const * a7, Z3_ast a8);
#define LOG_Z3_mk_quantifier(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_quantifier(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8); }
void log_Z3_mk_quantifier_ex(Z3_context a0, bool a1, unsigned a2, Z3_symbol a3, Z3_symbol a4, unsigned a5, Z3_pattern const * a6, unsigned a7, Z3_ast const * a8, unsigned a9, Z3_sort const * a10, Z3_symbol const * a11, Z3_ast a12);
#define LOG_Z3_mk_quantifier_ex(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8, _ARG9, _ARG10, _ARG11, _ARG12) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_quantifier_ex(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8, _ARG9, _ARG10, _ARG11, _ARG12); }
void log_Z3_mk_forall_const(Z3_context a0, unsigned a1, unsigned a2, Z3_app const * a3, unsigned a4, Z3_pattern const * a5, Z3_ast a6);
#define LOG_Z3_mk_forall_const(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_forall_const(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6); }
void log_Z3_mk_exists_const(Z3_context a0, unsigned a1, unsigned a2, Z3_app const * a3, unsigned a4, Z3_pattern const * a5, Z3_ast a6);
#define LOG_Z3_mk_exists_const(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_exists_const(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6); }
void log_Z3_mk_quantifier_const(Z3_context a0, bool a1, unsigned a2, unsigned a3, Z3_app const * a4, unsigned a5, Z3_pattern const * a6, Z3_ast a7);
#define LOG_Z3_mk_quantifier_const(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_quantifier_const(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_mk_quantifier_const_ex(Z3_context a0, bool a1, unsigned a2, Z3_symbol a3, Z3_symbol a4, unsigned a5, Z3_app const * a6, unsigned a7, Z3_pattern const * a8, unsigned a9, Z3_ast const * a10, Z3_ast a11);
#define LOG_Z3_mk_quantifier_const_ex(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8, _ARG9, _ARG10, _ARG11) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_quantifier_const_ex(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7, _ARG8, _ARG9, _ARG10, _ARG11); }
void log_Z3_mk_lambda(Z3_context a0, unsigned a1, Z3_sort const * a2, Z3_symbol const * a3, Z3_ast a4);
#define LOG_Z3_mk_lambda(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_lambda(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_lambda_const(Z3_context a0, unsigned a1, Z3_app const * a2, Z3_ast a3);
#define LOG_Z3_mk_lambda_const(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_lambda_const(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_get_symbol_kind(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_get_symbol_kind(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_symbol_kind(_ARG0, _ARG1); }
void log_Z3_get_symbol_int(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_get_symbol_int(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_symbol_int(_ARG0, _ARG1); }
void log_Z3_get_symbol_string(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_get_symbol_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_symbol_string(_ARG0, _ARG1); }
void log_Z3_get_sort_name(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_sort_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_sort_name(_ARG0, _ARG1); }
void log_Z3_get_sort_id(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_sort_id(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_sort_id(_ARG0, _ARG1); }
void log_Z3_sort_to_ast(Z3_context a0, Z3_sort a1);
#define LOG_Z3_sort_to_ast(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_sort_to_ast(_ARG0, _ARG1); }
void log_Z3_is_eq_sort(Z3_context a0, Z3_sort a1, Z3_sort a2);
#define LOG_Z3_is_eq_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_eq_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_sort_kind(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_sort_kind(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_sort_kind(_ARG0, _ARG1); }
void log_Z3_get_bv_sort_size(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_bv_sort_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_bv_sort_size(_ARG0, _ARG1); }
void log_Z3_get_finite_domain_sort_size(Z3_context a0, Z3_sort a1, uint64_t* a2);
#define LOG_Z3_get_finite_domain_sort_size(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_finite_domain_sort_size(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_array_arity(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_array_arity(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_array_arity(_ARG0, _ARG1); }
void log_Z3_get_array_sort_domain(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_array_sort_domain(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_array_sort_domain(_ARG0, _ARG1); }
void log_Z3_get_array_sort_domain_n(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_get_array_sort_domain_n(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_array_sort_domain_n(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_array_sort_range(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_array_sort_range(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_array_sort_range(_ARG0, _ARG1); }
void log_Z3_get_tuple_sort_mk_decl(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_tuple_sort_mk_decl(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_tuple_sort_mk_decl(_ARG0, _ARG1); }
void log_Z3_get_tuple_sort_num_fields(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_tuple_sort_num_fields(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_tuple_sort_num_fields(_ARG0, _ARG1); }
void log_Z3_get_tuple_sort_field_decl(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_get_tuple_sort_field_decl(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_tuple_sort_field_decl(_ARG0, _ARG1, _ARG2); }
void log_Z3_is_recursive_datatype_sort(Z3_context a0, Z3_sort a1);
#define LOG_Z3_is_recursive_datatype_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_recursive_datatype_sort(_ARG0, _ARG1); }
void log_Z3_get_datatype_sort_num_constructors(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_datatype_sort_num_constructors(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_datatype_sort_num_constructors(_ARG0, _ARG1); }
void log_Z3_get_datatype_sort_constructor(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_get_datatype_sort_constructor(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_datatype_sort_constructor(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_datatype_sort_recognizer(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_get_datatype_sort_recognizer(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_datatype_sort_recognizer(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_datatype_sort_constructor_accessor(Z3_context a0, Z3_sort a1, unsigned a2, unsigned a3);
#define LOG_Z3_get_datatype_sort_constructor_accessor(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_datatype_sort_constructor_accessor(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_datatype_update_field(Z3_context a0, Z3_func_decl a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_datatype_update_field(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_datatype_update_field(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_get_relation_arity(Z3_context a0, Z3_sort a1);
#define LOG_Z3_get_relation_arity(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_relation_arity(_ARG0, _ARG1); }
void log_Z3_get_relation_column(Z3_context a0, Z3_sort a1, unsigned a2);
#define LOG_Z3_get_relation_column(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_relation_column(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_atmost(Z3_context a0, unsigned a1, Z3_ast const * a2, unsigned a3);
#define LOG_Z3_mk_atmost(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_atmost(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_atleast(Z3_context a0, unsigned a1, Z3_ast const * a2, unsigned a3);
#define LOG_Z3_mk_atleast(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_atleast(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_pble(Z3_context a0, unsigned a1, Z3_ast const * a2, int const * a3, int a4);
#define LOG_Z3_mk_pble(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_pble(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_pbge(Z3_context a0, unsigned a1, Z3_ast const * a2, int const * a3, int a4);
#define LOG_Z3_mk_pbge(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_pbge(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_pbeq(Z3_context a0, unsigned a1, Z3_ast const * a2, int const * a3, int a4);
#define LOG_Z3_mk_pbeq(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_pbeq(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_func_decl_to_ast(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_func_decl_to_ast(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_decl_to_ast(_ARG0, _ARG1); }
void log_Z3_is_eq_func_decl(Z3_context a0, Z3_func_decl a1, Z3_func_decl a2);
#define LOG_Z3_is_eq_func_decl(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_eq_func_decl(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_func_decl_id(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_func_decl_id(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_func_decl_id(_ARG0, _ARG1); }
void log_Z3_get_decl_name(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_decl_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_name(_ARG0, _ARG1); }
void log_Z3_get_decl_kind(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_decl_kind(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_kind(_ARG0, _ARG1); }
void log_Z3_get_domain_size(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_domain_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_domain_size(_ARG0, _ARG1); }
void log_Z3_get_arity(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_arity(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_arity(_ARG0, _ARG1); }
void log_Z3_get_domain(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_domain(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_domain(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_range(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_range(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_range(_ARG0, _ARG1); }
void log_Z3_get_decl_num_parameters(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_get_decl_num_parameters(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_num_parameters(_ARG0, _ARG1); }
void log_Z3_get_decl_parameter_kind(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_parameter_kind(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_parameter_kind(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_int_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_int_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_int_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_double_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_double_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_double_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_symbol_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_symbol_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_symbol_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_sort_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_sort_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_sort_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_ast_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_ast_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_ast_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_func_decl_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_func_decl_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_func_decl_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_decl_rational_parameter(Z3_context a0, Z3_func_decl a1, unsigned a2);
#define LOG_Z3_get_decl_rational_parameter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_decl_rational_parameter(_ARG0, _ARG1, _ARG2); }
void log_Z3_app_to_ast(Z3_context a0, Z3_app a1);
#define LOG_Z3_app_to_ast(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_app_to_ast(_ARG0, _ARG1); }
void log_Z3_get_app_decl(Z3_context a0, Z3_app a1);
#define LOG_Z3_get_app_decl(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_app_decl(_ARG0, _ARG1); }
void log_Z3_get_app_num_args(Z3_context a0, Z3_app a1);
#define LOG_Z3_get_app_num_args(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_app_num_args(_ARG0, _ARG1); }
void log_Z3_get_app_arg(Z3_context a0, Z3_app a1, unsigned a2);
#define LOG_Z3_get_app_arg(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_app_arg(_ARG0, _ARG1, _ARG2); }
void log_Z3_is_eq_ast(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_is_eq_ast(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_eq_ast(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_ast_id(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_ast_id(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_ast_id(_ARG0, _ARG1); }
void log_Z3_get_ast_hash(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_ast_hash(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_ast_hash(_ARG0, _ARG1); }
void log_Z3_get_sort(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_sort(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_sort(_ARG0, _ARG1); }
void log_Z3_is_well_sorted(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_well_sorted(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_well_sorted(_ARG0, _ARG1); }
void log_Z3_get_bool_value(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_bool_value(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_bool_value(_ARG0, _ARG1); }
void log_Z3_get_ast_kind(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_ast_kind(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_ast_kind(_ARG0, _ARG1); }
void log_Z3_is_app(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_app(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_app(_ARG0, _ARG1); }
void log_Z3_is_ground(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_ground(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_ground(_ARG0, _ARG1); }
void log_Z3_get_depth(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_depth(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_depth(_ARG0, _ARG1); }
void log_Z3_is_numeral_ast(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_numeral_ast(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_numeral_ast(_ARG0, _ARG1); }
void log_Z3_is_algebraic_number(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_algebraic_number(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_algebraic_number(_ARG0, _ARG1); }
void log_Z3_to_app(Z3_context a0, Z3_ast a1);
#define LOG_Z3_to_app(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_to_app(_ARG0, _ARG1); }
void log_Z3_to_func_decl(Z3_context a0, Z3_ast a1);
#define LOG_Z3_to_func_decl(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_to_func_decl(_ARG0, _ARG1); }
void log_Z3_get_numeral_string(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_numeral_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_string(_ARG0, _ARG1); }
void log_Z3_get_numeral_binary_string(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_numeral_binary_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_binary_string(_ARG0, _ARG1); }
void log_Z3_get_numeral_decimal_string(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_numeral_decimal_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_decimal_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_numeral_double(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_numeral_double(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_double(_ARG0, _ARG1); }
void log_Z3_get_numerator(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_numerator(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numerator(_ARG0, _ARG1); }
void log_Z3_get_denominator(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_denominator(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_denominator(_ARG0, _ARG1); }
void log_Z3_get_numeral_small(Z3_context a0, Z3_ast a1, int64_t* a2, int64_t* a3);
#define LOG_Z3_get_numeral_small(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_small(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_get_numeral_int(Z3_context a0, Z3_ast a1, int* a2);
#define LOG_Z3_get_numeral_int(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_int(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_numeral_uint(Z3_context a0, Z3_ast a1, unsigned* a2);
#define LOG_Z3_get_numeral_uint(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_uint(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_numeral_uint64(Z3_context a0, Z3_ast a1, uint64_t* a2);
#define LOG_Z3_get_numeral_uint64(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_uint64(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_numeral_int64(Z3_context a0, Z3_ast a1, int64_t* a2);
#define LOG_Z3_get_numeral_int64(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_int64(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_numeral_rational_int64(Z3_context a0, Z3_ast a1, int64_t* a2, int64_t* a3);
#define LOG_Z3_get_numeral_rational_int64(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_numeral_rational_int64(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_get_algebraic_number_lower(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_algebraic_number_lower(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_algebraic_number_lower(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_algebraic_number_upper(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_algebraic_number_upper(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_algebraic_number_upper(_ARG0, _ARG1, _ARG2); }
void log_Z3_pattern_to_ast(Z3_context a0, Z3_pattern a1);
#define LOG_Z3_pattern_to_ast(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_pattern_to_ast(_ARG0, _ARG1); }
void log_Z3_get_pattern_num_terms(Z3_context a0, Z3_pattern a1);
#define LOG_Z3_get_pattern_num_terms(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_pattern_num_terms(_ARG0, _ARG1); }
void log_Z3_get_pattern(Z3_context a0, Z3_pattern a1, unsigned a2);
#define LOG_Z3_get_pattern(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_pattern(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_index_value(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_index_value(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_index_value(_ARG0, _ARG1); }
void log_Z3_is_quantifier_forall(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_quantifier_forall(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_quantifier_forall(_ARG0, _ARG1); }
void log_Z3_is_quantifier_exists(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_quantifier_exists(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_quantifier_exists(_ARG0, _ARG1); }
void log_Z3_is_lambda(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_lambda(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_lambda(_ARG0, _ARG1); }
void log_Z3_get_quantifier_weight(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_weight(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_weight(_ARG0, _ARG1); }
void log_Z3_get_quantifier_skolem_id(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_skolem_id(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_skolem_id(_ARG0, _ARG1); }
void log_Z3_get_quantifier_id(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_id(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_id(_ARG0, _ARG1); }
void log_Z3_get_quantifier_num_patterns(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_num_patterns(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_num_patterns(_ARG0, _ARG1); }
void log_Z3_get_quantifier_pattern_ast(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_quantifier_pattern_ast(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_pattern_ast(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_quantifier_num_no_patterns(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_num_no_patterns(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_num_no_patterns(_ARG0, _ARG1); }
void log_Z3_get_quantifier_no_pattern_ast(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_quantifier_no_pattern_ast(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_no_pattern_ast(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_quantifier_num_bound(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_num_bound(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_num_bound(_ARG0, _ARG1); }
void log_Z3_get_quantifier_bound_name(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_quantifier_bound_name(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_bound_name(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_quantifier_bound_sort(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_get_quantifier_bound_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_bound_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_quantifier_body(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_quantifier_body(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_quantifier_body(_ARG0, _ARG1); }
void log_Z3_simplify(Z3_context a0, Z3_ast a1);
#define LOG_Z3_simplify(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplify(_ARG0, _ARG1); }
void log_Z3_simplify_ex(Z3_context a0, Z3_ast a1, Z3_params a2);
#define LOG_Z3_simplify_ex(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplify_ex(_ARG0, _ARG1, _ARG2); }
void log_Z3_simplify_get_help(Z3_context a0);
#define LOG_Z3_simplify_get_help(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplify_get_help(_ARG0); }
void log_Z3_simplify_get_param_descrs(Z3_context a0);
#define LOG_Z3_simplify_get_param_descrs(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplify_get_param_descrs(_ARG0); }
void log_Z3_update_term(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_update_term(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_update_term(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_substitute(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3, Z3_ast const * a4);
#define LOG_Z3_substitute(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_substitute(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_substitute_vars(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_substitute_vars(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_substitute_vars(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_substitute_funs(Z3_context a0, Z3_ast a1, unsigned a2, Z3_func_decl const * a3, Z3_ast const * a4);
#define LOG_Z3_substitute_funs(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_substitute_funs(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_translate(Z3_context a0, Z3_ast a1, Z3_context a2);
#define LOG_Z3_translate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_translate(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_model(Z3_context a0);
#define LOG_Z3_mk_model(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_model(_ARG0); }
void log_Z3_model_inc_ref(Z3_context a0, Z3_model a1);
#define LOG_Z3_model_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_inc_ref(_ARG0, _ARG1); }
void log_Z3_model_dec_ref(Z3_context a0, Z3_model a1);
#define LOG_Z3_model_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_dec_ref(_ARG0, _ARG1); }
void log_Z3_model_eval(Z3_context a0, Z3_model a1, Z3_ast a2, bool a3, Z3_ast* a4);
#define LOG_Z3_model_eval(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; Z3_ast* _Z3_UNUSED Z3ARG4 = 0; if (_LOG_CTX.enabled()) { log_Z3_model_eval(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); Z3ARG4 = _ARG4; }
#define RETURN_Z3_model_eval if (_LOG_CTX.enabled()) { SetO((Z3ARG4 == 0 ? 0 : *Z3ARG4), 4); } return
void log_Z3_model_get_const_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2);
#define LOG_Z3_model_get_const_interp(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_const_interp(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_has_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2);
#define LOG_Z3_model_has_interp(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_has_interp(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_get_func_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2);
#define LOG_Z3_model_get_func_interp(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_func_interp(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_get_num_consts(Z3_context a0, Z3_model a1);
#define LOG_Z3_model_get_num_consts(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_num_consts(_ARG0, _ARG1); }
void log_Z3_model_get_const_decl(Z3_context a0, Z3_model a1, unsigned a2);
#define LOG_Z3_model_get_const_decl(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_const_decl(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_get_num_funcs(Z3_context a0, Z3_model a1);
#define LOG_Z3_model_get_num_funcs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_num_funcs(_ARG0, _ARG1); }
void log_Z3_model_get_func_decl(Z3_context a0, Z3_model a1, unsigned a2);
#define LOG_Z3_model_get_func_decl(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_func_decl(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_get_num_sorts(Z3_context a0, Z3_model a1);
#define LOG_Z3_model_get_num_sorts(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_num_sorts(_ARG0, _ARG1); }
void log_Z3_model_get_sort(Z3_context a0, Z3_model a1, unsigned a2);
#define LOG_Z3_model_get_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_get_sort_universe(Z3_context a0, Z3_model a1, Z3_sort a2);
#define LOG_Z3_model_get_sort_universe(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_get_sort_universe(_ARG0, _ARG1, _ARG2); }
void log_Z3_model_translate(Z3_context a0, Z3_model a1, Z3_context a2);
#define LOG_Z3_model_translate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_translate(_ARG0, _ARG1, _ARG2); }
void log_Z3_is_as_array(Z3_context a0, Z3_ast a1);
#define LOG_Z3_is_as_array(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_is_as_array(_ARG0, _ARG1); }
void log_Z3_get_as_array_func_decl(Z3_context a0, Z3_ast a1);
#define LOG_Z3_get_as_array_func_decl(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_as_array_func_decl(_ARG0, _ARG1); }
void log_Z3_add_func_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2, Z3_ast a3);
#define LOG_Z3_add_func_interp(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_add_func_interp(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_add_const_interp(Z3_context a0, Z3_model a1, Z3_func_decl a2, Z3_ast a3);
#define LOG_Z3_add_const_interp(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_add_const_interp(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_func_interp_inc_ref(Z3_context a0, Z3_func_interp a1);
#define LOG_Z3_func_interp_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_inc_ref(_ARG0, _ARG1); }
void log_Z3_func_interp_dec_ref(Z3_context a0, Z3_func_interp a1);
#define LOG_Z3_func_interp_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_dec_ref(_ARG0, _ARG1); }
void log_Z3_func_interp_get_num_entries(Z3_context a0, Z3_func_interp a1);
#define LOG_Z3_func_interp_get_num_entries(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_get_num_entries(_ARG0, _ARG1); }
void log_Z3_func_interp_get_entry(Z3_context a0, Z3_func_interp a1, unsigned a2);
#define LOG_Z3_func_interp_get_entry(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_get_entry(_ARG0, _ARG1, _ARG2); }
void log_Z3_func_interp_get_else(Z3_context a0, Z3_func_interp a1);
#define LOG_Z3_func_interp_get_else(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_get_else(_ARG0, _ARG1); }
void log_Z3_func_interp_set_else(Z3_context a0, Z3_func_interp a1, Z3_ast a2);
#define LOG_Z3_func_interp_set_else(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_set_else(_ARG0, _ARG1, _ARG2); }
void log_Z3_func_interp_get_arity(Z3_context a0, Z3_func_interp a1);
#define LOG_Z3_func_interp_get_arity(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_get_arity(_ARG0, _ARG1); }
void log_Z3_func_interp_add_entry(Z3_context a0, Z3_func_interp a1, Z3_ast_vector a2, Z3_ast a3);
#define LOG_Z3_func_interp_add_entry(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_interp_add_entry(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_func_entry_inc_ref(Z3_context a0, Z3_func_entry a1);
#define LOG_Z3_func_entry_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_entry_inc_ref(_ARG0, _ARG1); }
void log_Z3_func_entry_dec_ref(Z3_context a0, Z3_func_entry a1);
#define LOG_Z3_func_entry_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_entry_dec_ref(_ARG0, _ARG1); }
void log_Z3_func_entry_get_value(Z3_context a0, Z3_func_entry a1);
#define LOG_Z3_func_entry_get_value(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_entry_get_value(_ARG0, _ARG1); }
void log_Z3_func_entry_get_num_args(Z3_context a0, Z3_func_entry a1);
#define LOG_Z3_func_entry_get_num_args(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_entry_get_num_args(_ARG0, _ARG1); }
void log_Z3_func_entry_get_arg(Z3_context a0, Z3_func_entry a1, unsigned a2);
#define LOG_Z3_func_entry_get_arg(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_entry_get_arg(_ARG0, _ARG1, _ARG2); }
void log_Z3_toggle_warning_messages(bool a0);
#define LOG_Z3_toggle_warning_messages(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_toggle_warning_messages(_ARG0); }
void log_Z3_set_ast_print_mode(Z3_context a0, Z3_ast_print_mode a1);
#define LOG_Z3_set_ast_print_mode(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_set_ast_print_mode(_ARG0, _ARG1); }
void log_Z3_ast_to_string(Z3_context a0, Z3_ast a1);
#define LOG_Z3_ast_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_to_string(_ARG0, _ARG1); }
void log_Z3_pattern_to_string(Z3_context a0, Z3_pattern a1);
#define LOG_Z3_pattern_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_pattern_to_string(_ARG0, _ARG1); }
void log_Z3_sort_to_string(Z3_context a0, Z3_sort a1);
#define LOG_Z3_sort_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_sort_to_string(_ARG0, _ARG1); }
void log_Z3_func_decl_to_string(Z3_context a0, Z3_func_decl a1);
#define LOG_Z3_func_decl_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_func_decl_to_string(_ARG0, _ARG1); }
void log_Z3_model_to_string(Z3_context a0, Z3_model a1);
#define LOG_Z3_model_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_to_string(_ARG0, _ARG1); }
void log_Z3_benchmark_to_smtlib_string(Z3_context a0, Z3_string a1, Z3_string a2, Z3_string a3, Z3_string a4, unsigned a5, Z3_ast const * a6, Z3_ast a7);
#define LOG_Z3_benchmark_to_smtlib_string(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_benchmark_to_smtlib_string(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_parse_smtlib2_string(Z3_context a0, Z3_string a1, unsigned a2, Z3_symbol const * a3, Z3_sort const * a4, unsigned a5, Z3_symbol const * a6, Z3_func_decl const * a7);
#define LOG_Z3_parse_smtlib2_string(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parse_smtlib2_string(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_parse_smtlib2_file(Z3_context a0, Z3_string a1, unsigned a2, Z3_symbol const * a3, Z3_sort const * a4, unsigned a5, Z3_symbol const * a6, Z3_func_decl const * a7);
#define LOG_Z3_parse_smtlib2_file(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parse_smtlib2_file(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_eval_smtlib2_string(Z3_context a0, Z3_string a1);
#define LOG_Z3_eval_smtlib2_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_eval_smtlib2_string(_ARG0, _ARG1); }
void log_Z3_mk_parser_context(Z3_context a0);
#define LOG_Z3_mk_parser_context(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_parser_context(_ARG0); }
void log_Z3_parser_context_inc_ref(Z3_context a0, Z3_parser_context a1);
#define LOG_Z3_parser_context_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parser_context_inc_ref(_ARG0, _ARG1); }
void log_Z3_parser_context_dec_ref(Z3_context a0, Z3_parser_context a1);
#define LOG_Z3_parser_context_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parser_context_dec_ref(_ARG0, _ARG1); }
void log_Z3_parser_context_add_sort(Z3_context a0, Z3_parser_context a1, Z3_sort a2);
#define LOG_Z3_parser_context_add_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parser_context_add_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_parser_context_add_decl(Z3_context a0, Z3_parser_context a1, Z3_func_decl a2);
#define LOG_Z3_parser_context_add_decl(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parser_context_add_decl(_ARG0, _ARG1, _ARG2); }
void log_Z3_parser_context_from_string(Z3_context a0, Z3_parser_context a1, Z3_string a2);
#define LOG_Z3_parser_context_from_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_parser_context_from_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_error_code(Z3_context a0);
#define LOG_Z3_get_error_code(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_error_code(_ARG0); }
void log_Z3_set_error(Z3_context a0, Z3_error_code a1);
#define LOG_Z3_set_error(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_set_error(_ARG0, _ARG1); }
void log_Z3_get_error_msg(Z3_context a0, Z3_error_code a1);
#define LOG_Z3_get_error_msg(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_error_msg(_ARG0, _ARG1); }
void log_Z3_get_version(unsigned* a0, unsigned* a1, unsigned* a2, unsigned* a3);
#define LOG_Z3_get_version(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_version(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_get_full_version();
#define LOG_Z3_get_full_version() z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_full_version(); }
void log_Z3_enable_trace(Z3_string a0);
#define LOG_Z3_enable_trace(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_enable_trace(_ARG0); }
void log_Z3_disable_trace(Z3_string a0);
#define LOG_Z3_disable_trace(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_disable_trace(_ARG0); }
void log_Z3_reset_memory();
#define LOG_Z3_reset_memory() z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_reset_memory(); }
void log_Z3_finalize_memory();
#define LOG_Z3_finalize_memory() z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_finalize_memory(); }
void log_Z3_mk_goal(Z3_context a0, bool a1, bool a2, bool a3);
#define LOG_Z3_mk_goal(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_goal(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_goal_inc_ref(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_inc_ref(_ARG0, _ARG1); }
void log_Z3_goal_dec_ref(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_dec_ref(_ARG0, _ARG1); }
void log_Z3_goal_precision(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_precision(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_precision(_ARG0, _ARG1); }
void log_Z3_goal_assert(Z3_context a0, Z3_goal a1, Z3_ast a2);
#define LOG_Z3_goal_assert(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_assert(_ARG0, _ARG1, _ARG2); }
void log_Z3_goal_inconsistent(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_inconsistent(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_inconsistent(_ARG0, _ARG1); }
void log_Z3_goal_depth(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_depth(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_depth(_ARG0, _ARG1); }
void log_Z3_goal_reset(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_reset(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_reset(_ARG0, _ARG1); }
void log_Z3_goal_size(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_size(_ARG0, _ARG1); }
void log_Z3_goal_formula(Z3_context a0, Z3_goal a1, unsigned a2);
#define LOG_Z3_goal_formula(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_formula(_ARG0, _ARG1, _ARG2); }
void log_Z3_goal_num_exprs(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_num_exprs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_num_exprs(_ARG0, _ARG1); }
void log_Z3_goal_is_decided_sat(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_is_decided_sat(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_is_decided_sat(_ARG0, _ARG1); }
void log_Z3_goal_is_decided_unsat(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_is_decided_unsat(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_is_decided_unsat(_ARG0, _ARG1); }
void log_Z3_goal_translate(Z3_context a0, Z3_goal a1, Z3_context a2);
#define LOG_Z3_goal_translate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_translate(_ARG0, _ARG1, _ARG2); }
void log_Z3_goal_convert_model(Z3_context a0, Z3_goal a1, Z3_model a2);
#define LOG_Z3_goal_convert_model(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_convert_model(_ARG0, _ARG1, _ARG2); }
void log_Z3_goal_to_string(Z3_context a0, Z3_goal a1);
#define LOG_Z3_goal_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_to_string(_ARG0, _ARG1); }
void log_Z3_goal_to_dimacs_string(Z3_context a0, Z3_goal a1, bool a2);
#define LOG_Z3_goal_to_dimacs_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_goal_to_dimacs_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_tactic(Z3_context a0, Z3_string a1);
#define LOG_Z3_mk_tactic(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_tactic(_ARG0, _ARG1); }
void log_Z3_tactic_inc_ref(Z3_context a0, Z3_tactic a1);
#define LOG_Z3_tactic_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_inc_ref(_ARG0, _ARG1); }
void log_Z3_tactic_dec_ref(Z3_context a0, Z3_tactic a1);
#define LOG_Z3_tactic_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_dec_ref(_ARG0, _ARG1); }
void log_Z3_mk_probe(Z3_context a0, Z3_string a1);
#define LOG_Z3_mk_probe(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_probe(_ARG0, _ARG1); }
void log_Z3_probe_inc_ref(Z3_context a0, Z3_probe a1);
#define LOG_Z3_probe_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_inc_ref(_ARG0, _ARG1); }
void log_Z3_probe_dec_ref(Z3_context a0, Z3_probe a1);
#define LOG_Z3_probe_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_dec_ref(_ARG0, _ARG1); }
void log_Z3_tactic_and_then(Z3_context a0, Z3_tactic a1, Z3_tactic a2);
#define LOG_Z3_tactic_and_then(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_and_then(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_or_else(Z3_context a0, Z3_tactic a1, Z3_tactic a2);
#define LOG_Z3_tactic_or_else(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_or_else(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_par_or(Z3_context a0, unsigned a1, Z3_tactic const * a2);
#define LOG_Z3_tactic_par_or(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_par_or(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_par_and_then(Z3_context a0, Z3_tactic a1, Z3_tactic a2);
#define LOG_Z3_tactic_par_and_then(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_par_and_then(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_try_for(Z3_context a0, Z3_tactic a1, unsigned a2);
#define LOG_Z3_tactic_try_for(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_try_for(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_when(Z3_context a0, Z3_probe a1, Z3_tactic a2);
#define LOG_Z3_tactic_when(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_when(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_cond(Z3_context a0, Z3_probe a1, Z3_tactic a2, Z3_tactic a3);
#define LOG_Z3_tactic_cond(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_cond(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_tactic_repeat(Z3_context a0, Z3_tactic a1, unsigned a2);
#define LOG_Z3_tactic_repeat(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_repeat(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_skip(Z3_context a0);
#define LOG_Z3_tactic_skip(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_skip(_ARG0); }
void log_Z3_tactic_fail(Z3_context a0);
#define LOG_Z3_tactic_fail(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_fail(_ARG0); }
void log_Z3_tactic_fail_if(Z3_context a0, Z3_probe a1);
#define LOG_Z3_tactic_fail_if(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_fail_if(_ARG0, _ARG1); }
void log_Z3_tactic_fail_if_not_decided(Z3_context a0);
#define LOG_Z3_tactic_fail_if_not_decided(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_fail_if_not_decided(_ARG0); }
void log_Z3_tactic_using_params(Z3_context a0, Z3_tactic a1, Z3_params a2);
#define LOG_Z3_tactic_using_params(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_using_params(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_simplifier(Z3_context a0, Z3_string a1);
#define LOG_Z3_mk_simplifier(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_simplifier(_ARG0, _ARG1); }
void log_Z3_simplifier_inc_ref(Z3_context a0, Z3_simplifier a1);
#define LOG_Z3_simplifier_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_inc_ref(_ARG0, _ARG1); }
void log_Z3_simplifier_dec_ref(Z3_context a0, Z3_simplifier a1);
#define LOG_Z3_simplifier_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_dec_ref(_ARG0, _ARG1); }
void log_Z3_solver_add_simplifier(Z3_context a0, Z3_solver a1, Z3_simplifier a2);
#define LOG_Z3_solver_add_simplifier(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_add_simplifier(_ARG0, _ARG1, _ARG2); }
void log_Z3_simplifier_and_then(Z3_context a0, Z3_simplifier a1, Z3_simplifier a2);
#define LOG_Z3_simplifier_and_then(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_and_then(_ARG0, _ARG1, _ARG2); }
void log_Z3_simplifier_using_params(Z3_context a0, Z3_simplifier a1, Z3_params a2);
#define LOG_Z3_simplifier_using_params(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_using_params(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_num_simplifiers(Z3_context a0);
#define LOG_Z3_get_num_simplifiers(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_num_simplifiers(_ARG0); }
void log_Z3_get_simplifier_name(Z3_context a0, unsigned a1);
#define LOG_Z3_get_simplifier_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_simplifier_name(_ARG0, _ARG1); }
void log_Z3_simplifier_get_help(Z3_context a0, Z3_simplifier a1);
#define LOG_Z3_simplifier_get_help(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_get_help(_ARG0, _ARG1); }
void log_Z3_simplifier_get_param_descrs(Z3_context a0, Z3_simplifier a1);
#define LOG_Z3_simplifier_get_param_descrs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_get_param_descrs(_ARG0, _ARG1); }
void log_Z3_simplifier_get_descr(Z3_context a0, Z3_string a1);
#define LOG_Z3_simplifier_get_descr(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_simplifier_get_descr(_ARG0, _ARG1); }
void log_Z3_probe_const(Z3_context a0, double a1);
#define LOG_Z3_probe_const(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_const(_ARG0, _ARG1); }
void log_Z3_probe_lt(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_lt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_lt(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_gt(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_gt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_gt(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_le(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_le(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_le(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_ge(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_ge(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_ge(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_eq(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_eq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_eq(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_and(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_and(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_and(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_or(Z3_context a0, Z3_probe a1, Z3_probe a2);
#define LOG_Z3_probe_or(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_or(_ARG0, _ARG1, _ARG2); }
void log_Z3_probe_not(Z3_context a0, Z3_probe a1);
#define LOG_Z3_probe_not(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_not(_ARG0, _ARG1); }
void log_Z3_get_num_tactics(Z3_context a0);
#define LOG_Z3_get_num_tactics(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_num_tactics(_ARG0); }
void log_Z3_get_tactic_name(Z3_context a0, unsigned a1);
#define LOG_Z3_get_tactic_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_tactic_name(_ARG0, _ARG1); }
void log_Z3_get_num_probes(Z3_context a0);
#define LOG_Z3_get_num_probes(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_num_probes(_ARG0); }
void log_Z3_get_probe_name(Z3_context a0, unsigned a1);
#define LOG_Z3_get_probe_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_probe_name(_ARG0, _ARG1); }
void log_Z3_tactic_get_help(Z3_context a0, Z3_tactic a1);
#define LOG_Z3_tactic_get_help(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_get_help(_ARG0, _ARG1); }
void log_Z3_tactic_get_param_descrs(Z3_context a0, Z3_tactic a1);
#define LOG_Z3_tactic_get_param_descrs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_get_param_descrs(_ARG0, _ARG1); }
void log_Z3_tactic_get_descr(Z3_context a0, Z3_string a1);
#define LOG_Z3_tactic_get_descr(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_get_descr(_ARG0, _ARG1); }
void log_Z3_probe_get_descr(Z3_context a0, Z3_string a1);
#define LOG_Z3_probe_get_descr(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_get_descr(_ARG0, _ARG1); }
void log_Z3_probe_apply(Z3_context a0, Z3_probe a1, Z3_goal a2);
#define LOG_Z3_probe_apply(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_probe_apply(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_apply(Z3_context a0, Z3_tactic a1, Z3_goal a2);
#define LOG_Z3_tactic_apply(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_apply(_ARG0, _ARG1, _ARG2); }
void log_Z3_tactic_apply_ex(Z3_context a0, Z3_tactic a1, Z3_goal a2, Z3_params a3);
#define LOG_Z3_tactic_apply_ex(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_tactic_apply_ex(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_apply_result_inc_ref(Z3_context a0, Z3_apply_result a1);
#define LOG_Z3_apply_result_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_apply_result_inc_ref(_ARG0, _ARG1); }
void log_Z3_apply_result_dec_ref(Z3_context a0, Z3_apply_result a1);
#define LOG_Z3_apply_result_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_apply_result_dec_ref(_ARG0, _ARG1); }
void log_Z3_apply_result_to_string(Z3_context a0, Z3_apply_result a1);
#define LOG_Z3_apply_result_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_apply_result_to_string(_ARG0, _ARG1); }
void log_Z3_apply_result_get_num_subgoals(Z3_context a0, Z3_apply_result a1);
#define LOG_Z3_apply_result_get_num_subgoals(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_apply_result_get_num_subgoals(_ARG0, _ARG1); }
void log_Z3_apply_result_get_subgoal(Z3_context a0, Z3_apply_result a1, unsigned a2);
#define LOG_Z3_apply_result_get_subgoal(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_apply_result_get_subgoal(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_solver(Z3_context a0);
#define LOG_Z3_mk_solver(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_solver(_ARG0); }
void log_Z3_mk_simple_solver(Z3_context a0);
#define LOG_Z3_mk_simple_solver(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_simple_solver(_ARG0); }
void log_Z3_mk_solver_for_logic(Z3_context a0, Z3_symbol a1);
#define LOG_Z3_mk_solver_for_logic(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_solver_for_logic(_ARG0, _ARG1); }
void log_Z3_mk_solver_from_tactic(Z3_context a0, Z3_tactic a1);
#define LOG_Z3_mk_solver_from_tactic(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_solver_from_tactic(_ARG0, _ARG1); }
void log_Z3_solver_translate(Z3_context a0, Z3_solver a1, Z3_context a2);
#define LOG_Z3_solver_translate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_translate(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_import_model_converter(Z3_context a0, Z3_solver a1, Z3_solver a2);
#define LOG_Z3_solver_import_model_converter(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_import_model_converter(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_get_help(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_help(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_help(_ARG0, _ARG1); }
void log_Z3_solver_get_param_descrs(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_param_descrs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_param_descrs(_ARG0, _ARG1); }
void log_Z3_solver_set_params(Z3_context a0, Z3_solver a1, Z3_params a2);
#define LOG_Z3_solver_set_params(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_set_params(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_inc_ref(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_inc_ref(_ARG0, _ARG1); }
void log_Z3_solver_dec_ref(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_dec_ref(_ARG0, _ARG1); }
void log_Z3_solver_interrupt(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_interrupt(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_interrupt(_ARG0, _ARG1); }
void log_Z3_solver_push(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_push(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_push(_ARG0, _ARG1); }
void log_Z3_solver_pop(Z3_context a0, Z3_solver a1, unsigned a2);
#define LOG_Z3_solver_pop(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_pop(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_reset(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_reset(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_reset(_ARG0, _ARG1); }
void log_Z3_solver_get_num_scopes(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_num_scopes(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_num_scopes(_ARG0, _ARG1); }
void log_Z3_solver_assert(Z3_context a0, Z3_solver a1, Z3_ast a2);
#define LOG_Z3_solver_assert(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_assert(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_assert_and_track(Z3_context a0, Z3_solver a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_solver_assert_and_track(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_assert_and_track(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_solver_from_file(Z3_context a0, Z3_solver a1, Z3_string a2);
#define LOG_Z3_solver_from_file(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_from_file(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_from_string(Z3_context a0, Z3_solver a1, Z3_string a2);
#define LOG_Z3_solver_from_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_from_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_get_assertions(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_assertions(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_assertions(_ARG0, _ARG1); }
void log_Z3_solver_get_units(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_units(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_units(_ARG0, _ARG1); }
void log_Z3_solver_get_trail(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_trail(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_trail(_ARG0, _ARG1); }
void log_Z3_solver_get_non_units(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_non_units(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_non_units(_ARG0, _ARG1); }
void log_Z3_solver_get_levels(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, unsigned a3, unsigned const * a4);
#define LOG_Z3_solver_get_levels(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_levels(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_solver_congruence_root(Z3_context a0, Z3_solver a1, Z3_ast a2);
#define LOG_Z3_solver_congruence_root(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_congruence_root(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_congruence_next(Z3_context a0, Z3_solver a1, Z3_ast a2);
#define LOG_Z3_solver_congruence_next(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_congruence_next(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_congruence_explain(Z3_context a0, Z3_solver a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_solver_congruence_explain(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_congruence_explain(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_solver_solve_for(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, Z3_ast_vector a3, Z3_ast_vector a4);
#define LOG_Z3_solver_solve_for(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_solve_for(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_solver_register_on_clause(Z3_context a0, Z3_solver a1, void* a2, Z3_on_clause_eh* a3);
#define LOG_Z3_solver_register_on_clause(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_register_on_clause(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_solver_propagate_init(Z3_context a0, Z3_solver a1, void* a2, Z3_push_eh* a3, Z3_pop_eh* a4, Z3_fresh_eh* a5);
#define LOG_Z3_solver_propagate_init(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_init(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5); }
void log_Z3_solver_propagate_fixed(Z3_context a0, Z3_solver a1, Z3_fixed_eh* a2);
#define LOG_Z3_solver_propagate_fixed(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_fixed(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_final(Z3_context a0, Z3_solver a1, Z3_final_eh* a2);
#define LOG_Z3_solver_propagate_final(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_final(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_eq(Z3_context a0, Z3_solver a1, Z3_eq_eh* a2);
#define LOG_Z3_solver_propagate_eq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_eq(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_diseq(Z3_context a0, Z3_solver a1, Z3_eq_eh* a2);
#define LOG_Z3_solver_propagate_diseq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_diseq(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_created(Z3_context a0, Z3_solver a1, Z3_created_eh* a2);
#define LOG_Z3_solver_propagate_created(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_created(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_decide(Z3_context a0, Z3_solver a1, Z3_decide_eh* a2);
#define LOG_Z3_solver_propagate_decide(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_decide(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_next_split(Z3_context a0, Z3_solver_callback a1, Z3_ast a2, unsigned a3, Z3_lbool a4);
#define LOG_Z3_solver_next_split(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_next_split(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_solver_propagate_declare(Z3_context a0, Z3_symbol a1, unsigned a2, Z3_sort const * a3, Z3_sort a4);
#define LOG_Z3_solver_propagate_declare(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_declare(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_solver_propagate_register(Z3_context a0, Z3_solver a1, Z3_ast a2);
#define LOG_Z3_solver_propagate_register(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_register(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_register_cb(Z3_context a0, Z3_solver_callback a1, Z3_ast a2);
#define LOG_Z3_solver_propagate_register_cb(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_register_cb(_ARG0, _ARG1, _ARG2); }
void log_Z3_solver_propagate_consequence(Z3_context a0, Z3_solver_callback a1, unsigned a2, Z3_ast const * a3, unsigned a4, Z3_ast const * a5, Z3_ast const * a6, Z3_ast a7);
#define LOG_Z3_solver_propagate_consequence(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_propagate_consequence(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); }
void log_Z3_solver_set_initial_value(Z3_context a0, Z3_solver a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_solver_set_initial_value(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_set_initial_value(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_solver_check(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_check(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_check(_ARG0, _ARG1); }
void log_Z3_solver_check_assumptions(Z3_context a0, Z3_solver a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_solver_check_assumptions(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_check_assumptions(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_get_implied_equalities(Z3_context a0, Z3_solver a1, unsigned a2, Z3_ast const * a3, unsigned* a4);
#define LOG_Z3_get_implied_equalities(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_implied_equalities(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_solver_get_consequences(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, Z3_ast_vector a3, Z3_ast_vector a4);
#define LOG_Z3_solver_get_consequences(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_consequences(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_solver_cube(Z3_context a0, Z3_solver a1, Z3_ast_vector a2, unsigned a3);
#define LOG_Z3_solver_cube(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_cube(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_solver_get_model(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_model(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_model(_ARG0, _ARG1); }
void log_Z3_solver_get_proof(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_proof(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_proof(_ARG0, _ARG1); }
void log_Z3_solver_get_unsat_core(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_unsat_core(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_unsat_core(_ARG0, _ARG1); }
void log_Z3_solver_get_reason_unknown(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_reason_unknown(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_reason_unknown(_ARG0, _ARG1); }
void log_Z3_solver_get_statistics(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_get_statistics(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_get_statistics(_ARG0, _ARG1); }
void log_Z3_solver_to_string(Z3_context a0, Z3_solver a1);
#define LOG_Z3_solver_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_to_string(_ARG0, _ARG1); }
void log_Z3_solver_to_dimacs_string(Z3_context a0, Z3_solver a1, bool a2);
#define LOG_Z3_solver_to_dimacs_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_solver_to_dimacs_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_stats_to_string(Z3_context a0, Z3_stats a1);
#define LOG_Z3_stats_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_to_string(_ARG0, _ARG1); }
void log_Z3_stats_inc_ref(Z3_context a0, Z3_stats a1);
#define LOG_Z3_stats_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_inc_ref(_ARG0, _ARG1); }
void log_Z3_stats_dec_ref(Z3_context a0, Z3_stats a1);
#define LOG_Z3_stats_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_dec_ref(_ARG0, _ARG1); }
void log_Z3_stats_size(Z3_context a0, Z3_stats a1);
#define LOG_Z3_stats_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_size(_ARG0, _ARG1); }
void log_Z3_stats_get_key(Z3_context a0, Z3_stats a1, unsigned a2);
#define LOG_Z3_stats_get_key(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_get_key(_ARG0, _ARG1, _ARG2); }
void log_Z3_stats_is_uint(Z3_context a0, Z3_stats a1, unsigned a2);
#define LOG_Z3_stats_is_uint(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_is_uint(_ARG0, _ARG1, _ARG2); }
void log_Z3_stats_is_double(Z3_context a0, Z3_stats a1, unsigned a2);
#define LOG_Z3_stats_is_double(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_is_double(_ARG0, _ARG1, _ARG2); }
void log_Z3_stats_get_uint_value(Z3_context a0, Z3_stats a1, unsigned a2);
#define LOG_Z3_stats_get_uint_value(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_get_uint_value(_ARG0, _ARG1, _ARG2); }
void log_Z3_stats_get_double_value(Z3_context a0, Z3_stats a1, unsigned a2);
#define LOG_Z3_stats_get_double_value(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_stats_get_double_value(_ARG0, _ARG1, _ARG2); }
void log_Z3_get_estimated_alloc_size();
#define LOG_Z3_get_estimated_alloc_size() z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_get_estimated_alloc_size(); }
void log_Z3_mk_ast_vector(Z3_context a0);
#define LOG_Z3_mk_ast_vector(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ast_vector(_ARG0); }
void log_Z3_ast_vector_inc_ref(Z3_context a0, Z3_ast_vector a1);
#define LOG_Z3_ast_vector_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_inc_ref(_ARG0, _ARG1); }
void log_Z3_ast_vector_dec_ref(Z3_context a0, Z3_ast_vector a1);
#define LOG_Z3_ast_vector_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_dec_ref(_ARG0, _ARG1); }
void log_Z3_ast_vector_size(Z3_context a0, Z3_ast_vector a1);
#define LOG_Z3_ast_vector_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_size(_ARG0, _ARG1); }
void log_Z3_ast_vector_get(Z3_context a0, Z3_ast_vector a1, unsigned a2);
#define LOG_Z3_ast_vector_get(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_get(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_vector_set(Z3_context a0, Z3_ast_vector a1, unsigned a2, Z3_ast a3);
#define LOG_Z3_ast_vector_set(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_set(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_ast_vector_resize(Z3_context a0, Z3_ast_vector a1, unsigned a2);
#define LOG_Z3_ast_vector_resize(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_resize(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_vector_push(Z3_context a0, Z3_ast_vector a1, Z3_ast a2);
#define LOG_Z3_ast_vector_push(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_push(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_vector_translate(Z3_context a0, Z3_ast_vector a1, Z3_context a2);
#define LOG_Z3_ast_vector_translate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_translate(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_vector_to_string(Z3_context a0, Z3_ast_vector a1);
#define LOG_Z3_ast_vector_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_vector_to_string(_ARG0, _ARG1); }
void log_Z3_mk_ast_map(Z3_context a0);
#define LOG_Z3_mk_ast_map(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_ast_map(_ARG0); }
void log_Z3_ast_map_inc_ref(Z3_context a0, Z3_ast_map a1);
#define LOG_Z3_ast_map_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_inc_ref(_ARG0, _ARG1); }
void log_Z3_ast_map_dec_ref(Z3_context a0, Z3_ast_map a1);
#define LOG_Z3_ast_map_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_dec_ref(_ARG0, _ARG1); }
void log_Z3_ast_map_contains(Z3_context a0, Z3_ast_map a1, Z3_ast a2);
#define LOG_Z3_ast_map_contains(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_contains(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_map_find(Z3_context a0, Z3_ast_map a1, Z3_ast a2);
#define LOG_Z3_ast_map_find(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_find(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_map_insert(Z3_context a0, Z3_ast_map a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_ast_map_insert(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_insert(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_ast_map_erase(Z3_context a0, Z3_ast_map a1, Z3_ast a2);
#define LOG_Z3_ast_map_erase(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_erase(_ARG0, _ARG1, _ARG2); }
void log_Z3_ast_map_reset(Z3_context a0, Z3_ast_map a1);
#define LOG_Z3_ast_map_reset(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_reset(_ARG0, _ARG1); }
void log_Z3_ast_map_size(Z3_context a0, Z3_ast_map a1);
#define LOG_Z3_ast_map_size(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_size(_ARG0, _ARG1); }
void log_Z3_ast_map_keys(Z3_context a0, Z3_ast_map a1);
#define LOG_Z3_ast_map_keys(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_keys(_ARG0, _ARG1); }
void log_Z3_ast_map_to_string(Z3_context a0, Z3_ast_map a1);
#define LOG_Z3_ast_map_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_ast_map_to_string(_ARG0, _ARG1); }
void log_Z3_algebraic_is_value(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_is_value(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_is_value(_ARG0, _ARG1); }
void log_Z3_algebraic_is_pos(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_is_pos(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_is_pos(_ARG0, _ARG1); }
void log_Z3_algebraic_is_neg(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_is_neg(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_is_neg(_ARG0, _ARG1); }
void log_Z3_algebraic_is_zero(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_is_zero(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_is_zero(_ARG0, _ARG1); }
void log_Z3_algebraic_sign(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_sign(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_sign(_ARG0, _ARG1); }
void log_Z3_algebraic_add(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_add(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_add(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_sub(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_sub(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_sub(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_mul(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_mul(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_mul(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_div(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_div(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_div(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_root(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_algebraic_root(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_root(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_power(Z3_context a0, Z3_ast a1, unsigned a2);
#define LOG_Z3_algebraic_power(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_power(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_lt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_lt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_lt(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_gt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_gt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_gt(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_le(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_le(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_le(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_ge(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_ge(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_ge(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_eq(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_eq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_eq(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_neq(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_algebraic_neq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_neq(_ARG0, _ARG1, _ARG2); }
void log_Z3_algebraic_roots(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_algebraic_roots(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_roots(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_algebraic_eval(Z3_context a0, Z3_ast a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_algebraic_eval(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_eval(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_algebraic_get_poly(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_get_poly(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_get_poly(_ARG0, _ARG1); }
void log_Z3_algebraic_get_i(Z3_context a0, Z3_ast a1);
#define LOG_Z3_algebraic_get_i(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_algebraic_get_i(_ARG0, _ARG1); }
void log_Z3_polynomial_subresultants(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_polynomial_subresultants(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_polynomial_subresultants(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_rcf_del(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_del(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_del(_ARG0, _ARG1); }
void log_Z3_rcf_mk_rational(Z3_context a0, Z3_string a1);
#define LOG_Z3_rcf_mk_rational(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_mk_rational(_ARG0, _ARG1); }
void log_Z3_rcf_mk_small_int(Z3_context a0, int a1);
#define LOG_Z3_rcf_mk_small_int(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_mk_small_int(_ARG0, _ARG1); }
void log_Z3_rcf_mk_pi(Z3_context a0);
#define LOG_Z3_rcf_mk_pi(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_mk_pi(_ARG0); }
void log_Z3_rcf_mk_e(Z3_context a0);
#define LOG_Z3_rcf_mk_e(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_mk_e(_ARG0); }
void log_Z3_rcf_mk_infinitesimal(Z3_context a0);
#define LOG_Z3_rcf_mk_infinitesimal(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_mk_infinitesimal(_ARG0); }
void log_Z3_rcf_mk_roots(Z3_context a0, unsigned a1, Z3_rcf_num const * a2, Z3_rcf_num* a3);
#define LOG_Z3_rcf_mk_roots(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; unsigned _Z3_UNUSED Z3ARG1 = 0; Z3_rcf_num* _Z3_UNUSED Z3ARG3 = 0; if (_LOG_CTX.enabled()) { log_Z3_rcf_mk_roots(_ARG0, _ARG1, _ARG2, _ARG3); Z3ARG1 = _ARG1; Z3ARG3 = _ARG3; }
#define RETURN_Z3_rcf_mk_roots if (_LOG_CTX.enabled()) { for (unsigned i = 0; i < Z3ARG1; i++) { SetAO(Z3ARG3[i], 3, i); } } return
void log_Z3_rcf_add(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_add(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_add(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_sub(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_sub(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_sub(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_mul(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_mul(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_mul(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_div(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_div(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_div(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_neg(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_neg(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_neg(_ARG0, _ARG1); }
void log_Z3_rcf_inv(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_inv(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_inv(_ARG0, _ARG1); }
void log_Z3_rcf_power(Z3_context a0, Z3_rcf_num a1, unsigned a2);
#define LOG_Z3_rcf_power(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_power(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_lt(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_lt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_lt(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_gt(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_gt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_gt(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_le(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_le(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_le(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_ge(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_ge(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_ge(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_eq(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_eq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_eq(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_neq(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num a2);
#define LOG_Z3_rcf_neq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_neq(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_num_to_string(Z3_context a0, Z3_rcf_num a1, bool a2, bool a3);
#define LOG_Z3_rcf_num_to_string(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_num_to_string(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_rcf_num_to_decimal_string(Z3_context a0, Z3_rcf_num a1, unsigned a2);
#define LOG_Z3_rcf_num_to_decimal_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_num_to_decimal_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_get_numerator_denominator(Z3_context a0, Z3_rcf_num a1, Z3_rcf_num* a2, Z3_rcf_num* a3);
#define LOG_Z3_rcf_get_numerator_denominator(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; Z3_rcf_num* _Z3_UNUSED Z3ARG2 = 0; Z3_rcf_num* _Z3_UNUSED Z3ARG3 = 0; if (_LOG_CTX.enabled()) { log_Z3_rcf_get_numerator_denominator(_ARG0, _ARG1, _ARG2, _ARG3); Z3ARG2 = _ARG2; Z3ARG3 = _ARG3; }
#define RETURN_Z3_rcf_get_numerator_denominator if (_LOG_CTX.enabled()) { SetO((Z3ARG2 == 0 ? 0 : *Z3ARG2), 2); SetO((Z3ARG3 == 0 ? 0 : *Z3ARG3), 3); } return
void log_Z3_rcf_is_rational(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_is_rational(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_is_rational(_ARG0, _ARG1); }
void log_Z3_rcf_is_algebraic(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_is_algebraic(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_is_algebraic(_ARG0, _ARG1); }
void log_Z3_rcf_is_infinitesimal(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_is_infinitesimal(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_is_infinitesimal(_ARG0, _ARG1); }
void log_Z3_rcf_is_transcendental(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_is_transcendental(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_is_transcendental(_ARG0, _ARG1); }
void log_Z3_rcf_extension_index(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_extension_index(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_extension_index(_ARG0, _ARG1); }
void log_Z3_rcf_transcendental_name(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_transcendental_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_transcendental_name(_ARG0, _ARG1); }
void log_Z3_rcf_infinitesimal_name(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_infinitesimal_name(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_infinitesimal_name(_ARG0, _ARG1); }
void log_Z3_rcf_num_coefficients(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_num_coefficients(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_num_coefficients(_ARG0, _ARG1); }
void log_Z3_rcf_coefficient(Z3_context a0, Z3_rcf_num a1, unsigned a2);
#define LOG_Z3_rcf_coefficient(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_coefficient(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_interval(Z3_context a0, Z3_rcf_num a1, int* a2, int* a3, Z3_rcf_num* a4, int* a5, int* a6, Z3_rcf_num* a7);
#define LOG_Z3_rcf_interval(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7) z3_log_ctx _LOG_CTX; Z3_rcf_num* _Z3_UNUSED Z3ARG4 = 0; Z3_rcf_num* _Z3_UNUSED Z3ARG7 = 0; if (_LOG_CTX.enabled()) { log_Z3_rcf_interval(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5, _ARG6, _ARG7); Z3ARG4 = _ARG4; Z3ARG7 = _ARG7; }
#define RETURN_Z3_rcf_interval if (_LOG_CTX.enabled()) { SetO((Z3ARG4 == 0 ? 0 : *Z3ARG4), 4); SetO((Z3ARG7 == 0 ? 0 : *Z3ARG7), 7); } return
void log_Z3_rcf_num_sign_conditions(Z3_context a0, Z3_rcf_num a1);
#define LOG_Z3_rcf_num_sign_conditions(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_num_sign_conditions(_ARG0, _ARG1); }
void log_Z3_rcf_sign_condition_sign(Z3_context a0, Z3_rcf_num a1, unsigned a2);
#define LOG_Z3_rcf_sign_condition_sign(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_sign_condition_sign(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_num_sign_condition_coefficients(Z3_context a0, Z3_rcf_num a1, unsigned a2);
#define LOG_Z3_rcf_num_sign_condition_coefficients(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_num_sign_condition_coefficients(_ARG0, _ARG1, _ARG2); }
void log_Z3_rcf_sign_condition_coefficient(Z3_context a0, Z3_rcf_num a1, unsigned a2, unsigned a3);
#define LOG_Z3_rcf_sign_condition_coefficient(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_rcf_sign_condition_coefficient(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fixedpoint(Z3_context a0);
#define LOG_Z3_mk_fixedpoint(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fixedpoint(_ARG0); }
void log_Z3_fixedpoint_inc_ref(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_inc_ref(_ARG0, _ARG1); }
void log_Z3_fixedpoint_dec_ref(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_dec_ref(_ARG0, _ARG1); }
void log_Z3_fixedpoint_add_rule(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2, Z3_symbol a3);
#define LOG_Z3_fixedpoint_add_rule(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_add_rule(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_add_fact(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2, unsigned a3, unsigned const * a4);
#define LOG_Z3_fixedpoint_add_fact(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_add_fact(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_fixedpoint_assert(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2);
#define LOG_Z3_fixedpoint_assert(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_assert(_ARG0, _ARG1, _ARG2); }
void log_Z3_fixedpoint_query(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2);
#define LOG_Z3_fixedpoint_query(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_query(_ARG0, _ARG1, _ARG2); }
void log_Z3_fixedpoint_query_relations(Z3_context a0, Z3_fixedpoint a1, unsigned a2, Z3_func_decl const * a3);
#define LOG_Z3_fixedpoint_query_relations(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_query_relations(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_get_answer(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_answer(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_answer(_ARG0, _ARG1); }
void log_Z3_fixedpoint_get_reason_unknown(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_reason_unknown(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_reason_unknown(_ARG0, _ARG1); }
void log_Z3_fixedpoint_update_rule(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2, Z3_symbol a3);
#define LOG_Z3_fixedpoint_update_rule(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_update_rule(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_get_num_levels(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2);
#define LOG_Z3_fixedpoint_get_num_levels(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_num_levels(_ARG0, _ARG1, _ARG2); }
void log_Z3_fixedpoint_get_cover_delta(Z3_context a0, Z3_fixedpoint a1, int a2, Z3_func_decl a3);
#define LOG_Z3_fixedpoint_get_cover_delta(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_cover_delta(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_add_cover(Z3_context a0, Z3_fixedpoint a1, int a2, Z3_func_decl a3, Z3_ast a4);
#define LOG_Z3_fixedpoint_add_cover(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_add_cover(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_fixedpoint_get_statistics(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_statistics(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_statistics(_ARG0, _ARG1); }
void log_Z3_fixedpoint_register_relation(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2);
#define LOG_Z3_fixedpoint_register_relation(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_register_relation(_ARG0, _ARG1, _ARG2); }
void log_Z3_fixedpoint_set_predicate_representation(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2, unsigned a3, Z3_symbol const * a4);
#define LOG_Z3_fixedpoint_set_predicate_representation(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_set_predicate_representation(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_fixedpoint_get_rules(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_rules(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_rules(_ARG0, _ARG1); }
void log_Z3_fixedpoint_get_assertions(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_assertions(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_assertions(_ARG0, _ARG1); }
void log_Z3_fixedpoint_set_params(Z3_context a0, Z3_fixedpoint a1, Z3_params a2);
#define LOG_Z3_fixedpoint_set_params(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_set_params(_ARG0, _ARG1, _ARG2); }
void log_Z3_fixedpoint_get_help(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_help(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_help(_ARG0, _ARG1); }
void log_Z3_fixedpoint_get_param_descrs(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_param_descrs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_param_descrs(_ARG0, _ARG1); }
void log_Z3_fixedpoint_to_string(Z3_context a0, Z3_fixedpoint a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_fixedpoint_to_string(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_to_string(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_from_string(Z3_context a0, Z3_fixedpoint a1, Z3_string a2);
#define LOG_Z3_fixedpoint_from_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_from_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_fixedpoint_from_file(Z3_context a0, Z3_fixedpoint a1, Z3_string a2);
#define LOG_Z3_fixedpoint_from_file(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_from_file(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_optimize(Z3_context a0);
#define LOG_Z3_mk_optimize(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_optimize(_ARG0); }
void log_Z3_optimize_inc_ref(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_inc_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_inc_ref(_ARG0, _ARG1); }
void log_Z3_optimize_dec_ref(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_dec_ref(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_dec_ref(_ARG0, _ARG1); }
void log_Z3_optimize_assert(Z3_context a0, Z3_optimize a1, Z3_ast a2);
#define LOG_Z3_optimize_assert(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_assert(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_assert_and_track(Z3_context a0, Z3_optimize a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_optimize_assert_and_track(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_assert_and_track(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_optimize_assert_soft(Z3_context a0, Z3_optimize a1, Z3_ast a2, Z3_string a3, Z3_symbol a4);
#define LOG_Z3_optimize_assert_soft(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_assert_soft(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_optimize_maximize(Z3_context a0, Z3_optimize a1, Z3_ast a2);
#define LOG_Z3_optimize_maximize(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_maximize(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_minimize(Z3_context a0, Z3_optimize a1, Z3_ast a2);
#define LOG_Z3_optimize_minimize(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_minimize(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_push(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_push(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_push(_ARG0, _ARG1); }
void log_Z3_optimize_pop(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_pop(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_pop(_ARG0, _ARG1); }
void log_Z3_optimize_set_initial_value(Z3_context a0, Z3_optimize a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_optimize_set_initial_value(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_set_initial_value(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_optimize_check(Z3_context a0, Z3_optimize a1, unsigned a2, Z3_ast const * a3);
#define LOG_Z3_optimize_check(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_check(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_optimize_get_reason_unknown(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_reason_unknown(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_reason_unknown(_ARG0, _ARG1); }
void log_Z3_optimize_get_model(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_model(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_model(_ARG0, _ARG1); }
void log_Z3_optimize_get_unsat_core(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_unsat_core(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_unsat_core(_ARG0, _ARG1); }
void log_Z3_optimize_set_params(Z3_context a0, Z3_optimize a1, Z3_params a2);
#define LOG_Z3_optimize_set_params(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_set_params(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_get_param_descrs(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_param_descrs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_param_descrs(_ARG0, _ARG1); }
void log_Z3_optimize_get_lower(Z3_context a0, Z3_optimize a1, unsigned a2);
#define LOG_Z3_optimize_get_lower(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_lower(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_get_upper(Z3_context a0, Z3_optimize a1, unsigned a2);
#define LOG_Z3_optimize_get_upper(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_upper(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_get_lower_as_vector(Z3_context a0, Z3_optimize a1, unsigned a2);
#define LOG_Z3_optimize_get_lower_as_vector(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_lower_as_vector(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_get_upper_as_vector(Z3_context a0, Z3_optimize a1, unsigned a2);
#define LOG_Z3_optimize_get_upper_as_vector(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_upper_as_vector(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_to_string(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_to_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_to_string(_ARG0, _ARG1); }
void log_Z3_optimize_from_string(Z3_context a0, Z3_optimize a1, Z3_string a2);
#define LOG_Z3_optimize_from_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_from_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_from_file(Z3_context a0, Z3_optimize a1, Z3_string a2);
#define LOG_Z3_optimize_from_file(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_from_file(_ARG0, _ARG1, _ARG2); }
void log_Z3_optimize_get_help(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_help(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_help(_ARG0, _ARG1); }
void log_Z3_optimize_get_statistics(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_statistics(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_statistics(_ARG0, _ARG1); }
void log_Z3_optimize_get_assertions(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_assertions(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_assertions(_ARG0, _ARG1); }
void log_Z3_optimize_get_objectives(Z3_context a0, Z3_optimize a1);
#define LOG_Z3_optimize_get_objectives(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_optimize_get_objectives(_ARG0, _ARG1); }
void log_Z3_mk_fpa_rounding_mode_sort(Z3_context a0);
#define LOG_Z3_mk_fpa_rounding_mode_sort(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rounding_mode_sort(_ARG0); }
void log_Z3_mk_fpa_round_nearest_ties_to_even(Z3_context a0);
#define LOG_Z3_mk_fpa_round_nearest_ties_to_even(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_round_nearest_ties_to_even(_ARG0); }
void log_Z3_mk_fpa_rne(Z3_context a0);
#define LOG_Z3_mk_fpa_rne(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rne(_ARG0); }
void log_Z3_mk_fpa_round_nearest_ties_to_away(Z3_context a0);
#define LOG_Z3_mk_fpa_round_nearest_ties_to_away(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_round_nearest_ties_to_away(_ARG0); }
void log_Z3_mk_fpa_rna(Z3_context a0);
#define LOG_Z3_mk_fpa_rna(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rna(_ARG0); }
void log_Z3_mk_fpa_round_toward_positive(Z3_context a0);
#define LOG_Z3_mk_fpa_round_toward_positive(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_round_toward_positive(_ARG0); }
void log_Z3_mk_fpa_rtp(Z3_context a0);
#define LOG_Z3_mk_fpa_rtp(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rtp(_ARG0); }
void log_Z3_mk_fpa_round_toward_negative(Z3_context a0);
#define LOG_Z3_mk_fpa_round_toward_negative(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_round_toward_negative(_ARG0); }
void log_Z3_mk_fpa_rtn(Z3_context a0);
#define LOG_Z3_mk_fpa_rtn(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rtn(_ARG0); }
void log_Z3_mk_fpa_round_toward_zero(Z3_context a0);
#define LOG_Z3_mk_fpa_round_toward_zero(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_round_toward_zero(_ARG0); }
void log_Z3_mk_fpa_rtz(Z3_context a0);
#define LOG_Z3_mk_fpa_rtz(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rtz(_ARG0); }
void log_Z3_mk_fpa_sort(Z3_context a0, unsigned a1, unsigned a2);
#define LOG_Z3_mk_fpa_sort(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_sort_half(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_half(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_half(_ARG0); }
void log_Z3_mk_fpa_sort_16(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_16(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_16(_ARG0); }
void log_Z3_mk_fpa_sort_single(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_single(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_single(_ARG0); }
void log_Z3_mk_fpa_sort_32(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_32(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_32(_ARG0); }
void log_Z3_mk_fpa_sort_double(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_double(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_double(_ARG0); }
void log_Z3_mk_fpa_sort_64(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_64(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_64(_ARG0); }
void log_Z3_mk_fpa_sort_quadruple(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_quadruple(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_quadruple(_ARG0); }
void log_Z3_mk_fpa_sort_128(Z3_context a0);
#define LOG_Z3_mk_fpa_sort_128(_ARG0) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sort_128(_ARG0); }
void log_Z3_mk_fpa_nan(Z3_context a0, Z3_sort a1);
#define LOG_Z3_mk_fpa_nan(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_nan(_ARG0, _ARG1); }
void log_Z3_mk_fpa_inf(Z3_context a0, Z3_sort a1, bool a2);
#define LOG_Z3_mk_fpa_inf(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_inf(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_zero(Z3_context a0, Z3_sort a1, bool a2);
#define LOG_Z3_mk_fpa_zero(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_zero(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_fp(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_fpa_fp(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_fp(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_numeral_float(Z3_context a0, float a1, Z3_sort a2);
#define LOG_Z3_mk_fpa_numeral_float(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_numeral_float(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_numeral_double(Z3_context a0, double a1, Z3_sort a2);
#define LOG_Z3_mk_fpa_numeral_double(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_numeral_double(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_numeral_int(Z3_context a0, int a1, Z3_sort a2);
#define LOG_Z3_mk_fpa_numeral_int(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_numeral_int(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_numeral_int_uint(Z3_context a0, bool a1, int a2, unsigned a3, Z3_sort a4);
#define LOG_Z3_mk_fpa_numeral_int_uint(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_numeral_int_uint(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_fpa_numeral_int64_uint64(Z3_context a0, bool a1, int64_t a2, uint64_t a3, Z3_sort a4);
#define LOG_Z3_mk_fpa_numeral_int64_uint64(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_numeral_int64_uint64(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_fpa_abs(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_abs(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_abs(_ARG0, _ARG1); }
void log_Z3_mk_fpa_neg(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_neg(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_neg(_ARG0, _ARG1); }
void log_Z3_mk_fpa_add(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_fpa_add(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_add(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_sub(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_fpa_sub(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sub(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_mul(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_fpa_mul(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_mul(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_div(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3);
#define LOG_Z3_mk_fpa_div(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_div(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_fma(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3, Z3_ast a4);
#define LOG_Z3_mk_fpa_fma(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_fma(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_mk_fpa_sqrt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_sqrt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_sqrt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_rem(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_rem(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_rem(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_round_to_integral(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_round_to_integral(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_round_to_integral(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_min(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_min(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_min(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_max(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_max(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_max(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_leq(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_leq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_leq(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_lt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_lt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_lt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_geq(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_geq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_geq(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_gt(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_gt(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_gt(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_eq(Z3_context a0, Z3_ast a1, Z3_ast a2);
#define LOG_Z3_mk_fpa_eq(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_eq(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_is_normal(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_normal(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_normal(_ARG0, _ARG1); }
void log_Z3_mk_fpa_is_subnormal(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_subnormal(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_subnormal(_ARG0, _ARG1); }
void log_Z3_mk_fpa_is_zero(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_zero(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_zero(_ARG0, _ARG1); }
void log_Z3_mk_fpa_is_infinite(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_infinite(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_infinite(_ARG0, _ARG1); }
void log_Z3_mk_fpa_is_nan(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_nan(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_nan(_ARG0, _ARG1); }
void log_Z3_mk_fpa_is_negative(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_negative(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_negative(_ARG0, _ARG1); }
void log_Z3_mk_fpa_is_positive(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_is_positive(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_is_positive(_ARG0, _ARG1); }
void log_Z3_mk_fpa_to_fp_bv(Z3_context a0, Z3_ast a1, Z3_sort a2);
#define LOG_Z3_mk_fpa_to_fp_bv(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_fp_bv(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_to_fp_float(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3);
#define LOG_Z3_mk_fpa_to_fp_float(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_fp_float(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_to_fp_real(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3);
#define LOG_Z3_mk_fpa_to_fp_real(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_fp_real(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_to_fp_signed(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3);
#define LOG_Z3_mk_fpa_to_fp_signed(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_fp_signed(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_to_fp_unsigned(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_sort a3);
#define LOG_Z3_mk_fpa_to_fp_unsigned(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_fp_unsigned(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_to_ubv(Z3_context a0, Z3_ast a1, Z3_ast a2, unsigned a3);
#define LOG_Z3_mk_fpa_to_ubv(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_ubv(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_to_sbv(Z3_context a0, Z3_ast a1, Z3_ast a2, unsigned a3);
#define LOG_Z3_mk_fpa_to_sbv(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_sbv(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_mk_fpa_to_real(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_to_real(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_real(_ARG0, _ARG1); }
void log_Z3_fpa_get_ebits(Z3_context a0, Z3_sort a1);
#define LOG_Z3_fpa_get_ebits(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_ebits(_ARG0, _ARG1); }
void log_Z3_fpa_get_sbits(Z3_context a0, Z3_sort a1);
#define LOG_Z3_fpa_get_sbits(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_sbits(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_nan(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_nan(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_nan(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_inf(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_inf(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_inf(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_zero(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_zero(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_zero(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_normal(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_normal(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_normal(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_subnormal(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_subnormal(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_subnormal(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_positive(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_positive(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_positive(_ARG0, _ARG1); }
void log_Z3_fpa_is_numeral_negative(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_is_numeral_negative(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_is_numeral_negative(_ARG0, _ARG1); }
void log_Z3_fpa_get_numeral_sign_bv(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_get_numeral_sign_bv(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_sign_bv(_ARG0, _ARG1); }
void log_Z3_fpa_get_numeral_significand_bv(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_get_numeral_significand_bv(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_significand_bv(_ARG0, _ARG1); }
void log_Z3_fpa_get_numeral_sign(Z3_context a0, Z3_ast a1, int* a2);
#define LOG_Z3_fpa_get_numeral_sign(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_sign(_ARG0, _ARG1, _ARG2); }
void log_Z3_fpa_get_numeral_significand_string(Z3_context a0, Z3_ast a1);
#define LOG_Z3_fpa_get_numeral_significand_string(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_significand_string(_ARG0, _ARG1); }
void log_Z3_fpa_get_numeral_significand_uint64(Z3_context a0, Z3_ast a1, uint64_t* a2);
#define LOG_Z3_fpa_get_numeral_significand_uint64(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_significand_uint64(_ARG0, _ARG1, _ARG2); }
void log_Z3_fpa_get_numeral_exponent_string(Z3_context a0, Z3_ast a1, bool a2);
#define LOG_Z3_fpa_get_numeral_exponent_string(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_exponent_string(_ARG0, _ARG1, _ARG2); }
void log_Z3_fpa_get_numeral_exponent_int64(Z3_context a0, Z3_ast a1, int64_t* a2, bool a3);
#define LOG_Z3_fpa_get_numeral_exponent_int64(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_exponent_int64(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fpa_get_numeral_exponent_bv(Z3_context a0, Z3_ast a1, bool a2);
#define LOG_Z3_fpa_get_numeral_exponent_bv(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fpa_get_numeral_exponent_bv(_ARG0, _ARG1, _ARG2); }
void log_Z3_mk_fpa_to_ieee_bv(Z3_context a0, Z3_ast a1);
#define LOG_Z3_mk_fpa_to_ieee_bv(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_ieee_bv(_ARG0, _ARG1); }
void log_Z3_mk_fpa_to_fp_int_real(Z3_context a0, Z3_ast a1, Z3_ast a2, Z3_ast a3, Z3_sort a4);
#define LOG_Z3_mk_fpa_to_fp_int_real(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_mk_fpa_to_fp_int_real(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_fixedpoint_query_from_lvl(Z3_context a0, Z3_fixedpoint a1, Z3_ast a2, unsigned a3);
#define LOG_Z3_fixedpoint_query_from_lvl(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_query_from_lvl(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_get_ground_sat_answer(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_ground_sat_answer(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_ground_sat_answer(_ARG0, _ARG1); }
void log_Z3_fixedpoint_get_rules_along_trace(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_rules_along_trace(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_rules_along_trace(_ARG0, _ARG1); }
void log_Z3_fixedpoint_get_rule_names_along_trace(Z3_context a0, Z3_fixedpoint a1);
#define LOG_Z3_fixedpoint_get_rule_names_along_trace(_ARG0, _ARG1) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_rule_names_along_trace(_ARG0, _ARG1); }
void log_Z3_fixedpoint_add_invariant(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2, Z3_ast a3);
#define LOG_Z3_fixedpoint_add_invariant(_ARG0, _ARG1, _ARG2, _ARG3) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_add_invariant(_ARG0, _ARG1, _ARG2, _ARG3); }
void log_Z3_fixedpoint_get_reachable(Z3_context a0, Z3_fixedpoint a1, Z3_func_decl a2);
#define LOG_Z3_fixedpoint_get_reachable(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_fixedpoint_get_reachable(_ARG0, _ARG1, _ARG2); }
void log_Z3_qe_model_project(Z3_context a0, Z3_model a1, unsigned a2, Z3_app const * a3, Z3_ast a4);
#define LOG_Z3_qe_model_project(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_qe_model_project(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4); }
void log_Z3_qe_model_project_skolem(Z3_context a0, Z3_model a1, unsigned a2, Z3_app const * a3, Z3_ast a4, Z3_ast_map a5);
#define LOG_Z3_qe_model_project_skolem(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_qe_model_project_skolem(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5); }
void log_Z3_qe_model_project_with_witness(Z3_context a0, Z3_model a1, unsigned a2, Z3_app const * a3, Z3_ast a4, Z3_ast_map a5);
#define LOG_Z3_qe_model_project_with_witness(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_qe_model_project_with_witness(_ARG0, _ARG1, _ARG2, _ARG3, _ARG4, _ARG5); }
void log_Z3_model_extrapolate(Z3_context a0, Z3_model a1, Z3_ast a2);
#define LOG_Z3_model_extrapolate(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_model_extrapolate(_ARG0, _ARG1, _ARG2); }
void log_Z3_qe_lite(Z3_context a0, Z3_ast_vector a1, Z3_ast a2);
#define LOG_Z3_qe_lite(_ARG0, _ARG1, _ARG2) z3_log_ctx _LOG_CTX; if (_LOG_CTX.enabled()) { log_Z3_qe_lite(_ARG0, _ARG1, _ARG2); }

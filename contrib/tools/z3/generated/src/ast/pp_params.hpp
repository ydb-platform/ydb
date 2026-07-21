// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct pp_params {
  params_ref const & p;
  params_ref g;
  pp_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("pp")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("max_indent", CPK_UINT, "max. indentation in pretty printer", "4294967295","pp");
    d.insert("max_num_lines", CPK_UINT, "max. number of lines to be displayed in pretty printer", "4294967295","pp");
    d.insert("max_width", CPK_UINT, "max. width in pretty printer", "80","pp");
    d.insert("max_ribbon", CPK_UINT, "max. ribbon (width - indentation) in pretty printer", "80","pp");
    d.insert("max_depth", CPK_UINT, "max. term depth (when pretty printing SMT2 terms/formulas)", "5","pp");
    d.insert("no_lets", CPK_BOOL, "dont print lets in low level SMT printer", "false","pp");
    d.insert("min_alias_size", CPK_UINT, "min. size for creating an alias for a shared term (when pretty printing SMT2 terms/formulas)", "10","pp");
    d.insert("decimal", CPK_BOOL, "pretty print real numbers using decimal notation (the output may be truncated). Z3 adds a ? if the value is not precise", "false","pp");
    d.insert("decimal_precision", CPK_UINT, "maximum number of decimal places to be used when pp.decimal=true", "10","pp");
    d.insert("bv_literals", CPK_BOOL, "use Bit-Vector literals (e.g, #x0F and #b0101) during pretty printing", "true","pp");
    d.insert("fp_real_literals", CPK_BOOL, "use real-numbered floating point literals (e.g, +1.0p-1) during pretty printing", "false","pp");
    d.insert("bv_neg", CPK_BOOL, "use bvneg when displaying Bit-Vector literals where the most significant bit is 1", "false","pp");
    d.insert("flat_assoc", CPK_BOOL, "flat associative operators (when pretty printing SMT2 terms/formulas)", "true","pp");
    d.insert("fixed_indent", CPK_BOOL, "use a fixed indentation for applications", "false","pp");
    d.insert("single_line", CPK_BOOL, "ignore line breaks when true", "false","pp");
    d.insert("bounded", CPK_BOOL, "ignore characters exceeding max width", "false","pp");
    d.insert("pretty_proof", CPK_BOOL, "use slower, but prettier, printer for proofs", "false","pp");
    d.insert("simplify_implies", CPK_BOOL, "simplify nested implications for pretty printing", "true","pp");
  }
  /*
     REG_MODULE_PARAMS('pp', 'pp_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('pp', 'pretty printer')
  */
  unsigned max_indent() const { return p.get_uint("max_indent", g, 4294967295u); }
  unsigned max_num_lines() const { return p.get_uint("max_num_lines", g, 4294967295u); }
  unsigned max_width() const { return p.get_uint("max_width", g, 80u); }
  unsigned max_ribbon() const { return p.get_uint("max_ribbon", g, 80u); }
  unsigned max_depth() const { return p.get_uint("max_depth", g, 5u); }
  bool no_lets() const { return p.get_bool("no_lets", g, false); }
  unsigned min_alias_size() const { return p.get_uint("min_alias_size", g, 10u); }
  bool decimal() const { return p.get_bool("decimal", g, false); }
  unsigned decimal_precision() const { return p.get_uint("decimal_precision", g, 10u); }
  bool bv_literals() const { return p.get_bool("bv_literals", g, true); }
  bool fp_real_literals() const { return p.get_bool("fp_real_literals", g, false); }
  bool bv_neg() const { return p.get_bool("bv_neg", g, false); }
  bool flat_assoc() const { return p.get_bool("flat_assoc", g, true); }
  bool fixed_indent() const { return p.get_bool("fixed_indent", g, false); }
  bool single_line() const { return p.get_bool("single_line", g, false); }
  bool bounded() const { return p.get_bool("bounded", g, false); }
  bool pretty_proof() const { return p.get_bool("pretty_proof", g, false); }
  bool simplify_implies() const { return p.get_bool("simplify_implies", g, true); }
};

// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct algebraic_params {
  params_ref const & p;
  params_ref g;
  algebraic_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("algebraic")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("zero_accuracy", CPK_UINT, "one of the most time-consuming operations in the real algebraic number module is determining the sign of a polynomial evaluated at a sample point with non-rational algebraic number values. Let k be the value of this option. If k is 0, Z3 uses precise computation. Otherwise, the result of a polynomial evaluation is considered to be 0 if Z3 can show it is inside the interval (-1/2^k, 1/2^k)", "0","algebraic");
    d.insert("min_mag", CPK_UINT, "Z3 represents algebraic numbers using a (square-free) polynomial p and an isolating interval (which contains one and only one root of p). This interval may be refined during the computations. This parameter specifies whether to cache the value of a refined interval or not. It says the minimal size of an interval for caching purposes is 1/2^16", "16","algebraic");
    d.insert("factor", CPK_BOOL, "use polynomial factorization to simplify polynomials representing algebraic numbers", "true","algebraic");
    d.insert("factor_max_prime", CPK_UINT, "parameter for the polynomial factorization procedure in the algebraic number module. Z3 polynomial factorization is composed of three steps: factorization in GF(p), lifting and search. This parameter limits the maximum prime number p to be used in the first step", "31","algebraic");
    d.insert("factor_num_primes", CPK_UINT, "parameter for the polynomial factorization procedure in the algebraic number module. Z3 polynomial factorization is composed of three steps: factorization in GF(p), lifting and search. The search space may be reduced by factoring the polynomial in different GF(p)'s. This parameter specify the maximum number of finite factorizations to be considered, before lifting and searching", "1","algebraic");
    d.insert("factor_search_size", CPK_UINT, "parameter for the polynomial factorization procedure in the algebraic number module. Z3 polynomial factorization is composed of three steps: factorization in GF(p), lifting and search. This parameter can be used to limit the search space", "5000","algebraic");
  }
  /*
     REG_MODULE_PARAMS('algebraic', 'algebraic_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('algebraic', 'real algebraic number package. Non-default parameter settings are not supported')
  */
  unsigned zero_accuracy() const { return p.get_uint("zero_accuracy", g, 0u); }
  unsigned min_mag() const { return p.get_uint("min_mag", g, 16u); }
  bool factor() const { return p.get_bool("factor", g, true); }
  unsigned factor_max_prime() const { return p.get_uint("factor_max_prime", g, 31u); }
  unsigned factor_num_primes() const { return p.get_uint("factor_num_primes", g, 1u); }
  unsigned factor_search_size() const { return p.get_uint("factor_search_size", g, 5000u); }
};

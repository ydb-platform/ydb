#ifndef STAN_LANG_AST_FUN_HAS_NON_PARAM_VAR_HPP
#define STAN_LANG_AST_FUN_HAS_NON_PARAM_VAR_HPP

namespace stan {
  namespace lang {

    struct expression;
    struct variable_map;

    /**
     * Returns true if the specified expression contains a variable
     * that requires a Jacobian warning.  This is either a transformed
     * variable or a local variable or a non-linear function of a
     * parameter.
     *
     * <p>Compare to <code>has_var</code>, which is similar, but
     * just tests for inclusion of variables declared in the
     * parameters, transformed parameters, or model block.
     *
     * @param e Expression to test.
     * @param var_map Variable mapping for origin and types of
     * variables.
     * @return true if expression contains a variable defined as a
     * transformed parameter, or is a local variable that is not
     * an integer.
     */
    bool has_non_param_var(const expression& e, const variable_map& var_map);

  }
}
#endif

PY2_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(0.10.2)

PEERDIR(
    contrib/python/numpy
    contrib/python/patsy
    contrib/python/scipy
)

ADDINCL(
    contrib/python/statsmodels/py2/statsmodels/src
    FOR cython contrib/python/statsmodels/py2
    FOR cython contrib/python/scipy/py2
)

NO_CHECK_IMPORTS(
    statsmodels.*
)

NO_COMPILER_WARNINGS()

NO_LINT()

PY_SRCS(
    TOP_LEVEL

    statsmodels/nonparametric/_smoothers_lowess.pyx
    statsmodels/nonparametric/linbin.pyx
    statsmodels/tsa/_exponential_smoothers.pyx
    statsmodels/tsa/innovations/_arma_innovations.pyx
    statsmodels/tsa/kalmanf/kalman_loglike.pyx
    statsmodels/tsa/regime_switching/_hamilton_filter.pyx
    statsmodels/tsa/regime_switching/_kim_smoother.pyx
    statsmodels/tsa/statespace/_filters/_conventional.pyx
    statsmodels/tsa/statespace/_filters/_inversions.pyx
    statsmodels/tsa/statespace/_filters/_univariate.pyx
    statsmodels/tsa/statespace/_filters/_univariate_diffuse.pyx
    statsmodels/tsa/statespace/_initialization.pyx
    statsmodels/tsa/statespace/_kalman_filter.pyx
    statsmodels/tsa/statespace/_kalman_smoother.pyx
    statsmodels/tsa/statespace/_representation.pyx
    statsmodels/tsa/statespace/_simulation_smoother.pyx
    statsmodels/tsa/statespace/_smoothers/_alternative.pyx
    statsmodels/tsa/statespace/_smoothers/_classical.pyx
    statsmodels/tsa/statespace/_smoothers/_conventional.pyx
    statsmodels/tsa/statespace/_smoothers/_univariate.pyx
    statsmodels/tsa/statespace/_smoothers/_univariate_diffuse.pyx
    statsmodels/tsa/statespace/_tools.pyx

    statsmodels/__init__.py
    statsmodels/_version.py
    statsmodels/api.py
    statsmodels/base/__init__.py
    statsmodels/base/_constraints.py
    statsmodels/base/_parameter_inference.py
    statsmodels/base/_penalized.py
    statsmodels/base/_penalties.py
    statsmodels/base/_screening.py
    statsmodels/base/covtype.py
    statsmodels/base/data.py
    statsmodels/base/distributed_estimation.py
    statsmodels/base/elastic_net.py
    statsmodels/base/l1_cvxopt.py
    statsmodels/base/l1_slsqp.py
    statsmodels/base/l1_solvers_common.py
    statsmodels/base/model.py
    statsmodels/base/optimizer.py
    statsmodels/base/transform.py
    statsmodels/base/wrapper.py
    statsmodels/compat/__init__.py
    statsmodels/compat/numpy.py
    statsmodels/compat/pandas.py
    statsmodels/compat/platform.py
    statsmodels/compat/python.py
    statsmodels/compat/scipy.py
    statsmodels/discrete/__init__.py
    statsmodels/discrete/_diagnostics_count.py
    statsmodels/discrete/conditional_models.py
    statsmodels/discrete/count_model.py
    statsmodels/discrete/discrete_margins.py
    statsmodels/discrete/discrete_model.py
    statsmodels/distributions/__init__.py
    statsmodels/distributions/discrete.py
    statsmodels/distributions/edgeworth.py
    statsmodels/distributions/empirical_distribution.py
    statsmodels/distributions/mixture_rvs.py
    statsmodels/duration/__init__.py
    statsmodels/duration/_kernel_estimates.py
    statsmodels/duration/api.py
    statsmodels/duration/hazard_regression.py
    statsmodels/duration/survfunc.py
    statsmodels/emplike/__init__.py
    statsmodels/emplike/aft_el.py
    statsmodels/emplike/api.py
    statsmodels/emplike/descriptive.py
    statsmodels/emplike/elanova.py
    statsmodels/emplike/elregress.py
    statsmodels/emplike/originregress.py
    # statsmodels/examples/es_misc_poisson2.py
    # statsmodels/examples/ex_arch_canada.py
    # statsmodels/examples/ex_emplike_1.py
    # statsmodels/examples/ex_emplike_2.py
    # statsmodels/examples/ex_emplike_3.py
    # statsmodels/examples/ex_feasible_gls_het.py
    # statsmodels/examples/ex_feasible_gls_het_0.py
    # statsmodels/examples/ex_generic_mle.py
    # statsmodels/examples/ex_generic_mle_t.py
    # statsmodels/examples/ex_generic_mle_tdist.py
    # statsmodels/examples/ex_grangercausality.py
    # statsmodels/examples/ex_inter_rater.py
    # statsmodels/examples/ex_kde_confint.py
    # statsmodels/examples/ex_kde_normalreference.py
    # statsmodels/examples/ex_kernel_regression.py
    # statsmodels/examples/ex_kernel_regression2.py
    # statsmodels/examples/ex_kernel_regression3.py
    # statsmodels/examples/ex_kernel_regression_censored2.py
    # statsmodels/examples/ex_kernel_regression_dgp.py
    # statsmodels/examples/ex_kernel_regression_sigtest.py
    # statsmodels/examples/ex_kernel_semilinear_dgp.py
    # statsmodels/examples/ex_kernel_singleindex_dgp.py
    # statsmodels/examples/ex_kernel_test_functional.py
    # statsmodels/examples/ex_kernel_test_functional_li_wang.py
    # statsmodels/examples/ex_lowess.py
    # statsmodels/examples/ex_misc_tarma.py
    # statsmodels/examples/ex_misc_tmodel.py
    # statsmodels/examples/ex_multivar_kde.py
    # statsmodels/examples/ex_nearest_corr.py
    # statsmodels/examples/ex_ols_robustcov.py
    # statsmodels/examples/ex_outliers_influence.py
    # statsmodels/examples/ex_pairwise.py
    # statsmodels/examples/ex_pandas.py
    # statsmodels/examples/ex_pareto_plot.py
    # statsmodels/examples/ex_predict_results.py
    # statsmodels/examples/ex_proportion.py
    # statsmodels/examples/ex_regressionplots.py
    # statsmodels/examples/ex_rootfinding.py
    # statsmodels/examples/ex_sandwich.py
    # statsmodels/examples/ex_sandwich2.py
    # statsmodels/examples/ex_sandwich3.py
    # statsmodels/examples/ex_scatter_ellipse.py
    # statsmodels/examples/ex_univar_kde.py
    # statsmodels/examples/ex_wald_anova.py
    # statsmodels/examples/example_discrete_mnl.py
    # statsmodels/examples/example_enhanced_boxplots.py
    # statsmodels/examples/example_functional_plots.py
    # statsmodels/examples/example_kde.py
    # statsmodels/examples/example_ols_minimal_comp.py
    # statsmodels/examples/example_rpy.py
    # statsmodels/examples/koul_and_mc.py
    # statsmodels/examples/l1_demo/demo.py
    # statsmodels/examples/l1_demo/short_demo.py
    # statsmodels/examples/l1_demo/sklearn_compare.py
    # statsmodels/examples/run_all.py
    # statsmodels/examples/try_2regress.py
    # statsmodels/examples/try_fit_constrained.py
    # statsmodels/examples/try_gee.py
    # statsmodels/examples/try_gof_chisquare.py
    # statsmodels/examples/try_polytrend.py
    # statsmodels/examples/try_power.py
    # statsmodels/examples/try_power2.py
    # statsmodels/examples/try_tukey_hsd.py
    # statsmodels/examples/tsa/ar1cholesky.py
    # statsmodels/examples/tsa/arma_plots.py
    # statsmodels/examples/tsa/compare_arma.py
    # statsmodels/examples/tsa/ex_arma.py
    # statsmodels/examples/tsa/ex_arma_all.py
    # statsmodels/examples/tsa/ex_coint.py
    # statsmodels/examples/tsa/ex_var.py
    # statsmodels/examples/tsa/ex_var_reorder.py
    # statsmodels/examples/tsa/lagpolynomial.py
    # statsmodels/examples/tsa/try_ar.py
    # statsmodels/examples/tut_ols_ancova.py
    # statsmodels/examples/tut_ols_rlm_short.py
    statsmodels/formula/__init__.py
    statsmodels/formula/api.py
    statsmodels/formula/formulatools.py
    statsmodels/gam/__init__.py
    statsmodels/gam/api.py
    statsmodels/gam/gam_cross_validation/__init__.py
    statsmodels/gam/gam_cross_validation/cross_validators.py
    statsmodels/gam/gam_cross_validation/gam_cross_validation.py
    statsmodels/gam/gam_penalties.py
    statsmodels/gam/generalized_additive_model.py
    statsmodels/gam/smooth_basis.py
    statsmodels/genmod/__init__.py
    statsmodels/genmod/_prediction.py
    statsmodels/genmod/_tweedie_compound_poisson.py
    statsmodels/genmod/api.py
    statsmodels/genmod/bayes_mixed_glm.py
    statsmodels/genmod/cov_struct.py
    statsmodels/genmod/families/__init__.py
    statsmodels/genmod/families/family.py
    statsmodels/genmod/families/links.py
    statsmodels/genmod/families/varfuncs.py
    statsmodels/genmod/generalized_estimating_equations.py
    statsmodels/genmod/generalized_linear_model.py
    statsmodels/genmod/qif.py
    statsmodels/graphics/__init__.py
    statsmodels/graphics/_regressionplots_doc.py
    statsmodels/graphics/agreement.py
    statsmodels/graphics/api.py
    statsmodels/graphics/boxplots.py
    statsmodels/graphics/correlation.py
    statsmodels/graphics/dotplots.py
    statsmodels/graphics/factorplots.py
    statsmodels/graphics/functional.py
    statsmodels/graphics/gofplots.py
    statsmodels/graphics/mosaicplot.py
    statsmodels/graphics/plot_grids.py
    statsmodels/graphics/plottools.py
    statsmodels/graphics/regressionplots.py
    statsmodels/graphics/tsaplots.py
    statsmodels/graphics/tukeyplot.py
    statsmodels/graphics/utils.py
    statsmodels/imputation/__init__.py
    statsmodels/imputation/bayes_mi.py
    statsmodels/imputation/mice.py
    statsmodels/imputation/ros.py
    statsmodels/interface/__init__.py
    statsmodels/iolib/__init__.py
    statsmodels/iolib/api.py
    statsmodels/iolib/foreign.py
    statsmodels/iolib/openfile.py
    statsmodels/iolib/smpickle.py
    statsmodels/iolib/stata_summary_examples.py
    statsmodels/iolib/summary.py
    statsmodels/iolib/summary2.py
    statsmodels/iolib/table.py
    statsmodels/iolib/tableformatting.py
    statsmodels/miscmodels/__init__.py
    statsmodels/miscmodels/api.py
    statsmodels/miscmodels/count.py
    statsmodels/miscmodels/nonlinls.py
    statsmodels/miscmodels/tmodel.py
    statsmodels/miscmodels/try_mlecov.py
    statsmodels/multivariate/__init__.py
    statsmodels/multivariate/api.py
    statsmodels/multivariate/cancorr.py
    statsmodels/multivariate/factor.py
    statsmodels/multivariate/factor_rotation/__init__.py
    statsmodels/multivariate/factor_rotation/_analytic_rotation.py
    statsmodels/multivariate/factor_rotation/_gpa_rotation.py
    statsmodels/multivariate/factor_rotation/_wrappers.py
    statsmodels/multivariate/manova.py
    statsmodels/multivariate/multivariate_ols.py
    statsmodels/multivariate/pca.py
    statsmodels/multivariate/plots.py
    statsmodels/nonparametric/__init__.py
    statsmodels/nonparametric/_kernel_base.py
    statsmodels/nonparametric/api.py
    statsmodels/nonparametric/bandwidths.py
    statsmodels/nonparametric/kde.py
    statsmodels/nonparametric/kdetools.py
    statsmodels/nonparametric/kernel_density.py
    statsmodels/nonparametric/kernel_regression.py
    statsmodels/nonparametric/kernels.py
    statsmodels/nonparametric/smoothers_lowess.py
    statsmodels/nonparametric/smoothers_lowess_old.py
    statsmodels/regression/__init__.py
    statsmodels/regression/_prediction.py
    statsmodels/regression/_tools.py
    statsmodels/regression/dimred.py
    statsmodels/regression/feasible_gls.py
    statsmodels/regression/linear_model.py
    statsmodels/regression/mixed_linear_model.py
    statsmodels/regression/process_regression.py
    statsmodels/regression/quantile_regression.py
    statsmodels/regression/recursive_ls.py
    statsmodels/resampling/__init__.py
    statsmodels/robust/__init__.py
    statsmodels/robust/norms.py
    statsmodels/robust/robust_linear_model.py
    statsmodels/robust/scale.py
    statsmodels/sandbox/__init__.py
    statsmodels/sandbox/archive/__init__.py
    statsmodels/sandbox/archive/linalg_covmat.py
    statsmodels/sandbox/archive/linalg_decomp_1.py
    statsmodels/sandbox/archive/tsa.py
    statsmodels/sandbox/bspline.py
    statsmodels/sandbox/contrast_old.py
    statsmodels/sandbox/datarich/__init__.py
    statsmodels/sandbox/datarich/factormodels.py
    statsmodels/sandbox/descstats.py
    statsmodels/sandbox/distributions/__init__.py
    statsmodels/sandbox/distributions/copula.py
    statsmodels/sandbox/distributions/estimators.py
    # statsmodels/sandbox/distributions/examples/__init__.py
    # statsmodels/sandbox/distributions/examples/ex_extras.py
    # statsmodels/sandbox/distributions/examples/ex_fitfr.py
    # statsmodels/sandbox/distributions/examples/ex_gof.py
    # statsmodels/sandbox/distributions/examples/ex_mvelliptical.py
    # statsmodels/sandbox/distributions/examples/ex_transf2.py
    # statsmodels/sandbox/distributions/examples/matchdist.py
    statsmodels/sandbox/distributions/extras.py
    statsmodels/sandbox/distributions/genpareto.py
    statsmodels/sandbox/distributions/gof_new.py
    statsmodels/sandbox/distributions/multivariate.py
    statsmodels/sandbox/distributions/mv_measures.py
    statsmodels/sandbox/distributions/mv_normal.py
    statsmodels/sandbox/distributions/otherdist.py
    statsmodels/sandbox/distributions/quantize.py
    statsmodels/sandbox/distributions/sppatch.py
    statsmodels/sandbox/distributions/transform_functions.py
    statsmodels/sandbox/distributions/transformed.py
    statsmodels/sandbox/distributions/try_max.py
    statsmodels/sandbox/distributions/try_pot.py
    # statsmodels/sandbox/examples/bayesprior.py
    # statsmodels/sandbox/examples/ex_cusum.py
    # statsmodels/sandbox/examples/ex_formula.py
    # statsmodels/sandbox/examples/ex_formula_factor.py
    # statsmodels/sandbox/examples/ex_gam_results.py
    # statsmodels/sandbox/examples/ex_mixed_lls_0.py
    # statsmodels/sandbox/examples/ex_mixed_lls_re.py
    # statsmodels/sandbox/examples/ex_mixed_lls_timecorr.py
    # statsmodels/sandbox/examples/ex_onewaygls.py
    # statsmodels/sandbox/examples/ex_random_panel.py
    # statsmodels/sandbox/examples/example_crossval.py
    # statsmodels/sandbox/examples/example_gam.py
    # statsmodels/sandbox/examples/example_gam_0.py
    # statsmodels/sandbox/examples/example_garch.py
    # statsmodels/sandbox/examples/example_maxent.py
    # statsmodels/sandbox/examples/example_mle.py
    # statsmodels/sandbox/examples/example_nbin.py
    # statsmodels/sandbox/examples/example_pca.py
    # statsmodels/sandbox/examples/example_pca_regression.py
    # statsmodels/sandbox/examples/example_sysreg.py
    # statsmodels/sandbox/examples/run_all.py
    # statsmodels/sandbox/examples/thirdparty/ex_ratereturn.py
    # statsmodels/sandbox/examples/thirdparty/findow_1.py
    # statsmodels/sandbox/examples/thirdparty/try_interchange.py
    # statsmodels/sandbox/examples/try_gmm_other.py
    # statsmodels/sandbox/examples/try_multiols.py
    # statsmodels/sandbox/examples/try_quantile_regression.py
    # statsmodels/sandbox/examples/try_quantile_regression1.py
    # statsmodels/sandbox/examples/try_smoothers.py
    statsmodels/sandbox/formula.py
    statsmodels/sandbox/gam.py
    statsmodels/sandbox/infotheo.py
    statsmodels/sandbox/mcevaluate/__init__.py
    statsmodels/sandbox/mcevaluate/arma.py
    statsmodels/sandbox/mle.py
    statsmodels/sandbox/multilinear.py
    statsmodels/sandbox/nonparametric/__init__.py
    statsmodels/sandbox/nonparametric/densityorthopoly.py
    statsmodels/sandbox/nonparametric/dgp_examples.py
    statsmodels/sandbox/nonparametric/kde2.py
    statsmodels/sandbox/nonparametric/kdecovclass.py
    statsmodels/sandbox/nonparametric/kernel_extras.py
    statsmodels/sandbox/nonparametric/kernels.py
    statsmodels/sandbox/nonparametric/smoothers.py
    statsmodels/sandbox/nonparametric/testdata.py
    statsmodels/sandbox/panel/__init__.py
    statsmodels/sandbox/panel/correlation_structures.py
    statsmodels/sandbox/panel/mixed.py
    statsmodels/sandbox/panel/panel_short.py
    statsmodels/sandbox/panel/panelmod.py
    statsmodels/sandbox/panel/random_panel.py
    statsmodels/sandbox/panel/sandwich_covariance_generic.py
    statsmodels/sandbox/pca.py
    statsmodels/sandbox/predict_functional.py
    statsmodels/sandbox/regression/__init__.py
    statsmodels/sandbox/regression/anova_nistcertified.py
    statsmodels/sandbox/regression/ar_panel.py
    statsmodels/sandbox/regression/example_kernridge.py
    statsmodels/sandbox/regression/gmm.py
    statsmodels/sandbox/regression/kernridgeregress_class.py
    statsmodels/sandbox/regression/ols_anova_original.py
    statsmodels/sandbox/regression/onewaygls.py
    statsmodels/sandbox/regression/penalized.py
    statsmodels/sandbox/regression/predstd.py
    statsmodels/sandbox/regression/runmnl.py
    statsmodels/sandbox/regression/sympy_diff.py
    statsmodels/sandbox/regression/tools.py
    statsmodels/sandbox/regression/treewalkerclass.py
    statsmodels/sandbox/regression/try_catdata.py
    statsmodels/sandbox/regression/try_ols_anova.py
    statsmodels/sandbox/regression/try_treewalker.py
    statsmodels/sandbox/rls.py
    statsmodels/sandbox/stats/__init__.py
    statsmodels/sandbox/stats/contrast_tools.py
    statsmodels/sandbox/stats/diagnostic.py
    statsmodels/sandbox/stats/ex_newtests.py
    statsmodels/sandbox/stats/multicomp.py
    statsmodels/sandbox/stats/runs.py
    statsmodels/sandbox/stats/stats_dhuard.py
    statsmodels/sandbox/stats/stats_mstats_short.py
    statsmodels/sandbox/sysreg.py
    statsmodels/sandbox/tools/__init__.py
    statsmodels/sandbox/tools/cross_val.py
    statsmodels/sandbox/tools/mctools.py
    statsmodels/sandbox/tools/tools_pca.py
    statsmodels/sandbox/tools/try_mctools.py
    statsmodels/sandbox/tsa/__init__.py
    statsmodels/sandbox/tsa/diffusion.py
    statsmodels/sandbox/tsa/diffusion2.py
    statsmodels/sandbox/tsa/example_arma.py
    # statsmodels/sandbox/tsa/examples/ex_mle_arma.py
    # statsmodels/sandbox/tsa/examples/ex_mle_garch.py
    # statsmodels/sandbox/tsa/examples/example_var.py
    # statsmodels/sandbox/tsa/examples/try_ld_nitime.py
    statsmodels/sandbox/tsa/fftarma.py
    statsmodels/sandbox/tsa/garch.py
    statsmodels/sandbox/tsa/movstat.py
    statsmodels/sandbox/tsa/try_arma_more.py
    statsmodels/sandbox/tsa/try_fi.py
    statsmodels/sandbox/tsa/try_var_convolve.py
    statsmodels/sandbox/tsa/varma.py
    statsmodels/sandbox/utils_old.py
    statsmodels/src/__init__.py
    statsmodels/stats/__init__.py
    statsmodels/stats/_adnorm.py
    statsmodels/stats/_diagnostic_other.py
    statsmodels/stats/_knockoff.py
    statsmodels/stats/_lilliefors.py
    statsmodels/stats/anova.py
    statsmodels/stats/api.py
    statsmodels/stats/base.py
    statsmodels/stats/contingency_tables.py
    statsmodels/stats/contrast.py
    statsmodels/stats/correlation_tools.py
    statsmodels/stats/descriptivestats.py
    statsmodels/stats/diagnostic.py
    statsmodels/stats/gof.py
    statsmodels/stats/inter_rater.py
    statsmodels/stats/knockoff_regeffects.py
    statsmodels/stats/libqsturng/__init__.py
    statsmodels/stats/libqsturng/make_tbls.py
    statsmodels/stats/libqsturng/qsturng_.py
    statsmodels/stats/mediation.py
    statsmodels/stats/moment_helpers.py
    statsmodels/stats/multicomp.py
    statsmodels/stats/multitest.py
    statsmodels/stats/multivariate_tools.py
    statsmodels/stats/outliers_influence.py
    statsmodels/stats/power.py
    statsmodels/stats/proportion.py
    statsmodels/stats/regularized_covariance.py
    statsmodels/stats/sandwich_covariance.py
    statsmodels/stats/stattools.py
    statsmodels/stats/tabledist.py
    statsmodels/stats/weightstats.py
    statsmodels/tools/__init__.py
    statsmodels/tools/_testing.py
    statsmodels/tools/catadd.py
    statsmodels/tools/data.py
    statsmodels/tools/decorators.py
    statsmodels/tools/docstring.py
    statsmodels/tools/eval_measures.py
    statsmodels/tools/grouputils.py
    statsmodels/tools/linalg.py
    statsmodels/tools/numdiff.py
    statsmodels/tools/parallel.py
    statsmodels/tools/print_version.py
    statsmodels/tools/rootfinding.py
    statsmodels/tools/sequences.py
    statsmodels/tools/sm_exceptions.py
    statsmodels/tools/testing.py
    statsmodels/tools/tools.py
    statsmodels/tools/transform_model.py
    statsmodels/tools/web.py
    statsmodels/tsa/__init__.py
    statsmodels/tsa/_bds.py
    statsmodels/tsa/adfvalues.py
    statsmodels/tsa/api.py
    statsmodels/tsa/ar_model.py
    statsmodels/tsa/arima_model.py
    statsmodels/tsa/arima_process.py
    statsmodels/tsa/arma_mle.py
    statsmodels/tsa/base/__init__.py
    statsmodels/tsa/base/datetools.py
    statsmodels/tsa/base/tsa_model.py
    statsmodels/tsa/coint_tables.py
    statsmodels/tsa/descriptivestats.py
    statsmodels/tsa/filters/__init__.py
    statsmodels/tsa/filters/_utils.py
    statsmodels/tsa/filters/api.py
    statsmodels/tsa/filters/bk_filter.py
    statsmodels/tsa/filters/cf_filter.py
    statsmodels/tsa/filters/filtertools.py
    statsmodels/tsa/filters/hp_filter.py
    statsmodels/tsa/holtwinters.py
    statsmodels/tsa/innovations/__init__.py
    statsmodels/tsa/innovations/api.py
    statsmodels/tsa/innovations/arma_innovations.py
    statsmodels/tsa/interp/__init__.py
    statsmodels/tsa/interp/denton.py
    statsmodels/tsa/kalmanf/__init__.py
    statsmodels/tsa/kalmanf/kalmanfilter.py
    statsmodels/tsa/mlemodel.py
    statsmodels/tsa/regime_switching/__init__.py
    statsmodels/tsa/regime_switching/markov_autoregression.py
    statsmodels/tsa/regime_switching/markov_regression.py
    statsmodels/tsa/regime_switching/markov_switching.py
    statsmodels/tsa/seasonal.py
    statsmodels/tsa/statespace/__init__.py
    statsmodels/tsa/statespace/_filters/__init__.py
    statsmodels/tsa/statespace/_pykalman_smoother.py
    statsmodels/tsa/statespace/_smoothers/__init__.py
    statsmodels/tsa/statespace/api.py
    statsmodels/tsa/statespace/dynamic_factor.py
    statsmodels/tsa/statespace/initialization.py
    statsmodels/tsa/statespace/kalman_filter.py
    statsmodels/tsa/statespace/kalman_smoother.py
    statsmodels/tsa/statespace/mlemodel.py
    statsmodels/tsa/statespace/representation.py
    statsmodels/tsa/statespace/sarimax.py
    statsmodels/tsa/statespace/simulation_smoother.py
    statsmodels/tsa/statespace/structural.py
    statsmodels/tsa/statespace/tools.py
    statsmodels/tsa/statespace/varmax.py
    statsmodels/tsa/stattools.py
    statsmodels/tsa/tsatools.py
    statsmodels/tsa/varma_process.py
    statsmodels/tsa/vector_ar/__init__.py
    statsmodels/tsa/vector_ar/api.py
    statsmodels/tsa/vector_ar/dynamic.py
    statsmodels/tsa/vector_ar/hypothesis_test_results.py
    statsmodels/tsa/vector_ar/irf.py
    statsmodels/tsa/vector_ar/output.py
    statsmodels/tsa/vector_ar/plotting.py
    statsmodels/tsa/vector_ar/svar_model.py
    statsmodels/tsa/vector_ar/util.py
    statsmodels/tsa/vector_ar/var_model.py
    statsmodels/tsa/vector_ar/vecm.py
    statsmodels/tsa/x13.py
)

RESOURCE_FILES(
    PREFIX contrib/python/statsmodels/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)

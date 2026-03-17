import numpy as np
import pandas as pd
from sklearn.metrics import auc
from sklearn.utils.extmath import stable_cumsum
from sklearn.utils.validation import check_consistent_length
from sklearn.metrics import make_scorer

from ..utils import check_is_binary


def make_uplift_scorer(metric_name, treatment, **kwargs):
    """Make uplift scorer which can be used with the same API as ``sklearn.metrics.make_scorer``.

    Args:
        metric_name (string): Name of desirable uplift metric. Raise ValueError if invalid.
        treatment (pandas.Series): A Series from original DataFrame which
            contains original index and treatment group column.
        kwargs (additional arguments): Additional parameters to be passed to metric func.
            For example: `negative_effect`, `strategy`, `k` or somtething else.

    Returns:
        scorer (callable): An uplift scorer with passed treatment variable (and kwargs, optionally) that returns a scalar score.

    Raises:
        ValueError: if `metric_name` does not present in metrics list.
        ValueError: if `treatment` is not a pandas Series.

    Example::

        from sklearn.model_selection import cross_validate
        from sklift.metrics import make_uplift_scorer

        # define X_cv, y_cv, trmnt_cv and estimator

        # Use make_uplift_scorer to initialize new `sklearn.metrics.make_scorer` object
        qini_scorer = make_uplift_scorer("qini_auc_score", trmnt_cv)
        # or pass additional parameters if necessary
        uplift50_scorer = make_uplift_scorer("uplift_at_k", trmnt_cv, strategy='overall', k=0.5)

        # Use this object in model selection functions
        cross_validate(estimator,
           X=X_cv,
           y=y_cv,
           fit_params={'treatment': trmnt_cv}
           scoring=qini_scorer,
        )
    """
    metrics_dict = {
        'uplift_auc_score': uplift_auc_score,
        'qini_auc_score': qini_auc_score,
        'uplift_at_k': uplift_at_k,
        'weighted_average_uplift': weighted_average_uplift,
    }

    if metric_name not in metrics_dict.keys():
        raise ValueError(
            f"'{metric_name}' is not a valid scoring value. "
            f"List of valid metrics: {list(metrics_dict.keys())}"
        )

    if not isinstance(treatment, pd.Series):
        raise TypeError("Expected pandas.Series in treatment vector, got %s" % type(treatment))

    def scorer(y_true, uplift, treatment_value, **kwargs):
        t = treatment_value.loc[y_true.index]
        return metrics_dict[metric_name](y_true, uplift, t, **kwargs)

    return make_scorer(scorer, treatment_value=treatment, **kwargs)


def uplift_curve(y_true, uplift, treatment):
    """Compute Uplift curve.

    For computing the area under the Uplift Curve, see :func:`.uplift_auc_score`.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.

    Returns:
        array (shape = [>2]), array (shape = [>2]): Points on a curve.

    See also:
        :func:`.uplift_auc_score`: Compute normalized Area Under the Uplift curve from prediction scores.

        :func:`.perfect_uplift_curve`: Compute the perfect Uplift curve.

        :func:`.plot_uplift_curve`: Plot Uplift curves from predictions.

        :func:`.qini_curve`: Compute Qini curve.

    References:
        Devriendt, F., Guns, T., & Verbeke, W. (2020). Learning to rank for uplift modeling. ArXiv, abs/2002.05897.
    """

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)

    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)

    desc_score_indices = np.argsort(uplift, kind="mergesort")[::-1]
    y_true, uplift, treatment = y_true[desc_score_indices], uplift[desc_score_indices], treatment[desc_score_indices]

    y_true_ctrl, y_true_trmnt = y_true.copy(), y_true.copy()

    y_true_ctrl[treatment == 1] = 0
    y_true_trmnt[treatment == 0] = 0

    distinct_value_indices = np.where(np.diff(uplift))[0]
    threshold_indices = np.r_[distinct_value_indices, uplift.size - 1]

    num_trmnt = stable_cumsum(treatment)[threshold_indices]
    y_trmnt = stable_cumsum(y_true_trmnt)[threshold_indices]

    num_all = threshold_indices + 1

    num_ctrl = num_all - num_trmnt
    y_ctrl = stable_cumsum(y_true_ctrl)[threshold_indices]

    curve_values = (np.divide(y_trmnt, num_trmnt, out=np.zeros_like(y_trmnt), where=num_trmnt != 0) -
                    np.divide(y_ctrl, num_ctrl, out=np.zeros_like(y_ctrl), where=num_ctrl != 0)) * num_all

    if num_all.size == 0 or curve_values[0] != 0 or num_all[0] != 0:
        # Add an extra threshold position if necessary
        # to make sure that the curve starts at (0, 0)
        num_all = np.r_[0, num_all]
        curve_values = np.r_[0, curve_values]

    return num_all, curve_values


def perfect_uplift_curve(y_true, treatment):
    """Compute the perfect (optimum) Uplift curve.

    This is a function, given points on a curve.  For computing the
    area under the Uplift Curve, see :func:`.uplift_auc_score`.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        treatment (1d array-like): Treatment labels.

    Returns:
        array (shape = [>2]), array (shape = [>2]): Points on a curve.

    See also:
        :func:`.uplift_curve`: Compute the area under the Qini curve.

        :func:`.uplift_auc_score`: Compute normalized Area Under the Uplift curve from prediction scores.

        :func:`.plot_uplift_curve`: Plot Uplift curves from predictions.
    """

    check_consistent_length(y_true, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    y_true, treatment = np.array(y_true), np.array(treatment)

    cr_num = np.sum((y_true == 1) & (treatment == 0))  # Control Responders
    tn_num = np.sum((y_true == 0) & (treatment == 1))  # Treated Non-Responders

    # express an ideal uplift curve through y_true and treatment
    summand = y_true if cr_num > tn_num else treatment
    perfect_uplift = 2 * (y_true == treatment) + summand

    return uplift_curve(y_true, perfect_uplift, treatment)


def uplift_auc_score(y_true, uplift, treatment):
    """Compute normalized Area Under the Uplift Curve from prediction scores.

    By computing the area under the Uplift curve, the curve information is summarized in one number.
    For binary outcomes the ratio of the actual uplift gains curve above the diagonal to that of
    the optimum Uplift Curve.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.

    Returns:
        float: Area Under the Uplift Curve.

    See also:
        :func:`.uplift_curve`: Compute Uplift curve.

        :func:`.perfect_uplift_curve`: Compute the perfect (optimum) Uplift curve.

        :func:`.plot_uplift_curve`: Plot Uplift curves from predictions.

        :func:`.qini_auc_score`: Compute normalized Area Under the Qini Curve from prediction scores.
    """

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)

    x_actual, y_actual = uplift_curve(y_true, uplift, treatment)
    x_perfect, y_perfect = perfect_uplift_curve(y_true, treatment)
    x_baseline, y_baseline = np.array([0, x_perfect[-1]]), np.array([0, y_perfect[-1]])

    auc_score_baseline = auc(x_baseline, y_baseline)
    auc_score_perfect = auc(x_perfect, y_perfect) - auc_score_baseline
    auc_score_actual = auc(x_actual, y_actual) - auc_score_baseline

    return auc_score_actual / auc_score_perfect


def qini_curve(y_true, uplift, treatment):
    """Compute Qini curve.

    For computing the area under the Qini Curve, see :func:`.qini_auc_score`.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.

    Returns:
        array (shape = [>2]), array (shape = [>2]): Points on a curve.

    See also:
        :func:`.uplift_curve`: Compute the area under the Qini curve.

        :func:`.perfect_qini_curve`: Compute the perfect Qini curve.

        :func:`.plot_qini_curves`: Plot Qini curves from predictions..

        :func:`.uplift_curve`: Compute Uplift curve.

    References:
        Nicholas J Radcliffe. (2007). Using control groups to target on predicted lift:
        Building and assessing uplift model. Direct Marketing Analytics Journal, (3):14–21, 2007.

        Devriendt, F., Guns, T., & Verbeke, W. (2020). Learning to rank for uplift modeling. ArXiv, abs/2002.05897.
    """

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)

    desc_score_indices = np.argsort(uplift, kind="mergesort")[::-1]

    y_true = y_true[desc_score_indices]
    treatment = treatment[desc_score_indices]
    uplift = uplift[desc_score_indices]

    y_true_ctrl, y_true_trmnt = y_true.copy(), y_true.copy()

    y_true_ctrl[treatment == 1] = 0
    y_true_trmnt[treatment == 0] = 0

    distinct_value_indices = np.where(np.diff(uplift))[0]
    threshold_indices = np.r_[distinct_value_indices, uplift.size - 1]

    num_trmnt = stable_cumsum(treatment)[threshold_indices]
    y_trmnt = stable_cumsum(y_true_trmnt)[threshold_indices]

    num_all = threshold_indices + 1

    num_ctrl = num_all - num_trmnt
    y_ctrl = stable_cumsum(y_true_ctrl)[threshold_indices]

    curve_values = y_trmnt - y_ctrl * np.divide(num_trmnt, num_ctrl, out=np.zeros_like(num_trmnt), where=num_ctrl != 0)
    if num_all.size == 0 or curve_values[0] != 0 or num_all[0] != 0:
        # Add an extra threshold position if necessary
        # to make sure that the curve starts at (0, 0)
        num_all = np.r_[0, num_all]
        curve_values = np.r_[0, curve_values]

    return num_all, curve_values


def perfect_qini_curve(y_true, treatment, negative_effect=True):
    """Compute the perfect (optimum) Qini curve.

    For computing the area under the Qini Curve, see :func:`.qini_auc_score`.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        treatment (1d array-like): Treatment labels.
        negative_effect (bool): If True, optimum Qini Curve contains the negative effects
            (negative uplift because of campaign). Otherwise, optimum Qini Curve will not
            contain the negative effects.
    Returns:
        array (shape = [>2]), array (shape = [>2]): Points on a curve.

    See also:
        :func:`.qini_curve`: Compute Qini curve.

        :func:`.qini_auc_score`: Compute the area under the Qini curve.

        :func:`.plot_qini_curves`: Plot Qini curves from predictions..
    """

    check_consistent_length(y_true, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    n_samples = len(y_true)

    y_true, treatment = np.array(y_true), np.array(treatment)

    if not isinstance(negative_effect, bool):
        raise TypeError(f'Negative_effects flag should be bool, got: {type(negative_effect)}')

    # express an ideal uplift curve through y_true and treatment
    if negative_effect:
        x_perfect, y_perfect = qini_curve(
            y_true, y_true * treatment - y_true * (1 - treatment), treatment
        )
    else:
        ratio_random = (y_true[treatment == 1].sum() - len(y_true[treatment == 1]) *
                        y_true[treatment == 0].sum() / len(y_true[treatment == 0]))

        x_perfect, y_perfect = np.array([0, ratio_random, n_samples]), np.array([0, ratio_random, ratio_random])

    return x_perfect, y_perfect


def qini_auc_score(y_true, uplift, treatment, negative_effect=True):
    """Compute normalized Area Under the Qini curve (aka Qini coefficient) from prediction scores.

    By computing the area under the Qini curve, the curve information is summarized in one number.
    For binary outcomes the ratio of the actual uplift gains curve above the diagonal to that of
    the optimum Qini curve.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        negative_effect (bool): If True, optimum Qini Curve contains the negative effects
            (negative uplift because of campaign). Otherwise, optimum Qini Curve will not contain the negative effects.

            .. versionadded:: 0.2.0

    Returns:
        float: Qini coefficient.

    See also:
        :func:`.qini_curve`: Compute Qini curve.

        :func:`.perfect_qini_curve`: Compute the perfect (optimum) Qini curve.

        :func:`.plot_qini_curves`: Plot Qini curves from predictions..

        :func:`.uplift_auc_score`: Compute normalized Area Under the Uplift curve from prediction scores.

    References:
        Nicholas J Radcliffe. (2007). Using control groups to target on predicted lift:
        Building and assessing uplift model. Direct Marketing Analytics Journal, (3):14–21, 2007.
    """

    # TODO: Add Continuous Outcomes
    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)

    if not isinstance(negative_effect, bool):
        raise TypeError(f'Negative_effects flag should be bool, got: {type(negative_effect)}')

    x_actual, y_actual = qini_curve(y_true, uplift, treatment)
    x_perfect, y_perfect = perfect_qini_curve(y_true, treatment, negative_effect)
    x_baseline, y_baseline = np.array([0, x_perfect[-1]]), np.array([0, y_perfect[-1]])

    auc_score_baseline = auc(x_baseline, y_baseline)
    auc_score_perfect = auc(x_perfect, y_perfect) - auc_score_baseline
    auc_score_actual = auc(x_actual, y_actual) - auc_score_baseline

    return auc_score_actual / auc_score_perfect


def uplift_at_k(y_true, uplift, treatment, strategy, k=0.3):
    """Compute uplift at first k observations by uplift of the total sample.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        k (float or int): If float, should be between 0.0 and 1.0 and represent the proportion of the dataset
            to include in the computation of uplift. If int, represents the absolute number of samples.
        strategy (string, ['overall', 'by_group']): Determines the calculating strategy.

            * ``'overall'``:
                The first step is taking the first k observations of all test data ordered by uplift prediction
                (overall both groups - control and treatment) and conversions in treatment and control groups
                calculated only on them. Then the difference between these conversions is calculated.

            * ``'by_group'``:
                Separately calculates conversions in top k observations in each group (control and treatment)
                sorted by uplift predictions. Then the difference between these conversions is calculated



    .. versionchanged:: 0.1.0

        * Add supporting absolute values for ``k`` parameter
        * Add parameter ``strategy``

    Returns:
        float: Uplift score at first k observations of the total sample.

    See also:
        :func:`.uplift_auc_score`: Compute normalized Area Under the Uplift curve from prediction scores.

        :func:`.qini_auc_score`: Compute normalized Area Under the Qini Curve from prediction scores.
    """

    # TODO: checker all groups is not empty
    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)

    strategy_methods = ['overall', 'by_group']
    if strategy not in strategy_methods:
        raise ValueError(f'Uplift score supports only calculating methods in {strategy_methods},'
                         f' got {strategy}.'
                         )

    n_samples = len(y_true)
    order = np.argsort(uplift, kind='mergesort')[::-1]
    _, treatment_counts = np.unique(treatment, return_counts=True)
    n_samples_ctrl = treatment_counts[0]
    n_samples_trmnt = treatment_counts[1]

    k_type = np.asarray(k).dtype.kind

    if (k_type == 'i' and (k >= n_samples or k <= 0)
            or k_type == 'f' and (k <= 0 or k >= 1)):
        raise ValueError(f'k={k} should be either positive and smaller'
                         f' than the number of samples {n_samples} or a float in the '
                         f'(0, 1) range')

    if k_type not in ('i', 'f'):
        raise ValueError(f'Invalid value for k: {k_type}')

    if strategy == 'overall':
        if k_type == 'f':
            n_size = int(n_samples * k)
        else:
            n_size = k

        # ToDo: _checker_ there are observations among two groups among first k
        score_ctrl = y_true[order][:n_size][treatment[order][:n_size] == 0].mean()
        score_trmnt = y_true[order][:n_size][treatment[order][:n_size] == 1].mean()

    else:  # strategy == 'by_group':
        if k_type == 'f':
            n_ctrl = int((treatment == 0).sum() * k)
            n_trmnt = int((treatment == 1).sum() * k)

        else:
            n_ctrl = k
            n_trmnt = k

        if n_ctrl > n_samples_ctrl:
            raise ValueError(f'With k={k}, the number of the first k observations'
                             ' bigger than the number of samples'
                             f'in the control group: {n_samples_ctrl}'
                             )
        if n_trmnt > n_samples_trmnt:
            raise ValueError(f'With k={k}, the number of the first k observations'
                             ' bigger than the number of samples'
                             f'in the treatment group: {n_samples_ctrl}'
                             )

        score_ctrl = y_true[order][treatment[order] == 0][:n_ctrl].mean()
        score_trmnt = y_true[order][treatment[order] == 1][:n_trmnt].mean()

    return score_trmnt - score_ctrl


def response_rate_by_percentile(y_true, uplift, treatment, group, strategy='overall', bins=10):
    """Compute response rate (target mean in the control or treatment group) at each percentile.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        group (string, ['treatment', 'control']): Group type for computing response rate: treatment or control.

            * ``'treatment'``:
                Values equal 1 in the treatment column.
            * ``'control'``:
                Values equal 0 in the treatment column.

        strategy (string, ['overall', 'by_group']): Determines the calculating strategy. Default is 'overall'.

            * ``'overall'``:
                The first step is taking the first k observations of all test data ordered by uplift prediction
                (overall both groups - control and treatment) and conversions in treatment and control groups
                calculated only on them. Then the difference between these conversions is calculated.
            * ``'by_group'``:
                Separately calculates conversions in top k observations in each group (control and treatment)
                sorted by uplift predictions. Then the difference between these conversions is calculated.

        bins (int): Determines the number of bins (and relative percentile) in the data. Default is 10.
        
    Returns:
        array (shape = [>2]), array (shape = [>2]), array (shape = [>2]):
        response rate at each percentile for control or treatment group,
        variance of the response rate at each percentile,
        group size at each percentile.
    """

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    group_types = ['treatment', 'control']
    strategy_methods = ['overall', 'by_group']
    
    n_samples = len(y_true)
    
    if group not in group_types:
        raise ValueError(f'Response rate supports only group types in {group_types},'
                         f' got {group}.') 

    if strategy not in strategy_methods:
        raise ValueError(f'Response rate supports only calculating methods in {strategy_methods},'
                         f' got {strategy}.')
    
    if not isinstance(bins, int) or bins <= 0:
        raise ValueError(f'Bins should be positive integer. Invalid value bins: {bins}')

    if bins >= n_samples:
        raise ValueError(f'Number of bins = {bins} should be smaller than the length of y_true {n_samples}')
    
    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)
    order = np.argsort(uplift, kind='mergesort')[::-1]

    trmnt_flag = 1 if group == 'treatment' else 0
    
    if strategy == 'overall':
        y_true_bin = np.array_split(y_true[order], bins)
        trmnt_bin = np.array_split(treatment[order], bins)
        
        group_size = np.array([len(y[trmnt == trmnt_flag]) for y, trmnt in zip(y_true_bin, trmnt_bin)])
        response_rate = np.array([np.mean(y[trmnt == trmnt_flag]) for y, trmnt in zip(y_true_bin, trmnt_bin)])

    else:  # strategy == 'by_group'
        y_bin = np.array_split(y_true[order][treatment[order] == trmnt_flag], bins)
        
        group_size = np.array([len(y) for y in y_bin])
        response_rate = np.array([np.mean(y) for y in y_bin])

    variance = np.multiply(response_rate, np.divide((1 - response_rate), group_size))

    return response_rate, variance, group_size


def weighted_average_uplift(y_true, uplift, treatment, strategy='overall', bins=10):
    """Weighted average uplift.

    It is an average of uplift by percentile.
    Weights are sizes of the treatment group by percentile.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        strategy (string, ['overall', 'by_group']): Determines the calculating strategy. Default is 'overall'.

            * ``'overall'``:
                The first step is taking the first k observations of all test data ordered by uplift prediction
                (overall both groups - control and treatment) and conversions in treatment and control groups
                calculated only on them. Then the difference between these conversions is calculated.
            * ``'by_group'``:
                Separately calculates conversions in top k observations in each group (control and treatment)
                sorted by uplift predictions. Then the difference between these conversions is calculated

        bins (int): Determines the number of bins (and the relative percentile) in the data. Default is 10.

    Returns:
        float: Weighted average uplift.
    """

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)

    strategy_methods = ['overall', 'by_group']

    n_samples = len(y_true)

    if strategy not in strategy_methods:
        raise ValueError(f'Response rate supports only calculating methods in {strategy_methods},'
                         f' got {strategy}.')

    if not isinstance(bins, int) or bins <= 0:
        raise ValueError(f'Bins should be positive integer.'
                         f' Invalid value bins: {bins}')

    if bins >= n_samples:
        raise ValueError(f'Number of bins = {bins} should be smaller than the length of y_true {n_samples}')

    response_rate_trmnt, variance_trmnt, n_trmnt = response_rate_by_percentile(
        y_true, uplift, treatment, group='treatment', strategy=strategy, bins=bins)

    response_rate_ctrl, variance_ctrl, n_ctrl = response_rate_by_percentile(
        y_true, uplift, treatment, group='control', strategy=strategy, bins=bins)

    uplift_scores = response_rate_trmnt - response_rate_ctrl

    weighted_avg_uplift = np.dot(n_trmnt, uplift_scores) / np.sum(n_trmnt)

    return weighted_avg_uplift


def uplift_by_percentile(y_true, uplift, treatment, strategy='overall',
                         bins=10, std=False, total=False, string_percentiles=True):
    """Compute metrics: uplift, group size, group response rate, standard deviation at each percentile.

    Metrics in columns and percentiles in rows of pandas DataFrame:

        - ``n_treatment``, ``n_control`` - group sizes.
        - ``response_rate_treatment``, ``response_rate_control`` - group response rates.
        - ``uplift`` - treatment response rate substract control response rate.
        - ``std_treatment``, ``std_control`` - (optional) response rates standard deviation.
        - ``std_uplift`` - (optional) uplift standard deviation.

    Args:
        y_true (1d array-like): Correct (true) binary target values.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        strategy (string, ['overall', 'by_group']): Determines the calculating strategy. Default is 'overall'.

            * ``'overall'``:
                The first step is taking the first k observations of all test data ordered by uplift prediction
                (overall both groups - control and treatment) and conversions in treatment and control groups
                calculated only on them. Then the difference between these conversions is calculated.
            * ``'by_group'``:
                Separately calculates conversions in top k observations in each group (control and treatment)
                sorted by uplift predictions. Then the difference between these conversions is calculated

        std (bool): If True, add columns with the uplift standard deviation and the response rate standard deviation.
            Default is False.
        total (bool): If True, add the last row with the total values. Default is False.
            The total uplift computes as a total response rate treatment - a total response rate control.
            The total response rate is a response rate on the full data amount.
        bins (int): Determines the number of bins (and the relative percentile) in the data. Default is 10.
        string_percentiles (bool): type of percentiles in the index: float or string. Default is True (string).

    Returns:
        pandas.DataFrame: DataFrame where metrics are by columns and percentiles are by rows.
    """

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)

    strategy_methods = ['overall', 'by_group']

    n_samples = len(y_true)

    if strategy not in strategy_methods:
        raise ValueError(f'Response rate supports only calculating methods in {strategy_methods},'
                         f' got {strategy}.')

    if not isinstance(total, bool):
        raise ValueError(f'Flag total should be bool: True or False.'
                         f' Invalid value total: {total}')

    if not isinstance(std, bool):
        raise ValueError(f'Flag std should be bool: True or False.'
                         f' Invalid value std: {std}')

    if not isinstance(bins, int) or bins <= 0:
        raise ValueError(f'Bins should be positive integer.'
                         f' Invalid value bins: {bins}')

    if bins >= n_samples:
        raise ValueError(f'Number of bins = {bins} should be smaller than the length of y_true {n_samples}')

    if not isinstance(string_percentiles, bool):
        raise ValueError(f'string_percentiles flag should be bool: True or False.'
                         f' Invalid value string_percentiles: {string_percentiles}')

    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)

    response_rate_trmnt, variance_trmnt, n_trmnt = response_rate_by_percentile(
        y_true, uplift, treatment, group='treatment', strategy=strategy, bins=bins)

    response_rate_ctrl, variance_ctrl, n_ctrl = response_rate_by_percentile(
        y_true, uplift, treatment, group='control', strategy=strategy, bins=bins)

    uplift_scores = response_rate_trmnt - response_rate_ctrl
    uplift_variance = variance_trmnt + variance_ctrl

    percentiles = [round(p * 100 / bins) for p in range(1, bins + 1)]

    if string_percentiles:
        percentiles = [f"0-{percentiles[0]}"] + \
            [f"{percentiles[i]}-{percentiles[i + 1]}" for i in range(len(percentiles) - 1)]


    df = pd.DataFrame({
        'percentile': percentiles,
        'n_treatment': n_trmnt,
        'n_control': n_ctrl,
        'response_rate_treatment': response_rate_trmnt,
        'response_rate_control': response_rate_ctrl,
        'uplift': uplift_scores
    })

    if total:
        response_rate_trmnt_total, variance_trmnt_total, n_trmnt_total = response_rate_by_percentile(
            y_true, uplift, treatment, strategy=strategy, group='treatment', bins=1)

        response_rate_ctrl_total, variance_ctrl_total, n_ctrl_total = response_rate_by_percentile(
            y_true, uplift, treatment, strategy=strategy, group='control', bins=1)

        df.loc[-1, :] = ['total', n_trmnt_total, n_ctrl_total, response_rate_trmnt_total,
                         response_rate_ctrl_total, response_rate_trmnt_total - response_rate_ctrl_total]

    if std:
        std_treatment = np.sqrt(variance_trmnt)
        std_control = np.sqrt(variance_ctrl)
        std_uplift = np.sqrt(uplift_variance)

        if total:
            std_treatment = np.append(std_treatment, np.sum(std_treatment))
            std_control = np.append(std_control, np.sum(std_control))
            std_uplift = np.append(std_uplift, np.sum(std_uplift))

        df.loc[:, 'std_treatment'] = std_treatment
        df.loc[:, 'std_control'] = std_control
        df.loc[:, 'std_uplift'] = std_uplift

    df = df \
        .set_index('percentile', drop=True, inplace=False) \
        .astype({'n_treatment': 'int32', 'n_control': 'int32'})

    return df


def treatment_balance_curve(uplift, treatment, winsize):
    """Compute the treatment balance curve: proportion of treatment group in the ordered predictions.

    Args:
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        winsize(int): Size of the sliding window for calculating the balance between treatment and control.

    Returns:
        array (shape = [>2]), array (shape = [>2]): Points on a curve.
    """

    check_consistent_length(uplift, treatment)
    check_is_binary(treatment)
    uplift, treatment = np.array(uplift), np.array(treatment)

    desc_score_indices = np.argsort(uplift, kind="mergesort")[::-1]

    treatment = treatment[desc_score_indices]

    balance = np.convolve(treatment, np.ones(winsize), 'valid') / winsize
    idx = np.linspace(1, 100, len(balance))
    return idx, balance


def average_squared_deviation(y_true_train, uplift_train, treatment_train, y_true_val,
                              uplift_val, treatment_val, strategy='overall', bins=10):
    """Compute the average squared deviation.

    The average squared deviation (ASD) is a model stability metric that shows how much the model overfits
    the training data. Larger values of ASD mean greater overfit.

    Args:
        y_true_train (1d array-like): Correct (true) target values for training set.
        uplift_train (1d array-like): Predicted uplift for training set, as returned by a model.
        treatment_train (1d array-like): Treatment labels for training set.
        y_true_val (1d array-like): Correct (true) target values for validation set.
        uplift_val (1d array-like): Predicted uplift for validation set, as returned by a model.
        treatment_val (1d array-like): Treatment labels for validation set.
        strategy (string, ['overall', 'by_group']): Determines the calculating strategy. Default is 'overall'.

            * ``'overall'``:
                The first step is taking the first k observations of all test data ordered by uplift prediction
                (overall both groups - control and treatment) and conversions in treatment and control groups
                calculated only on them. Then the difference between these conversions is calculated.
            * ``'by_group'``:
                Separately calculates conversions in top k observations in each group (control and treatment)
                sorted by uplift predictions. Then the difference between these conversions is calculated

        bins (int): Determines the number of bins (and the relative percentile) in the data. Default is 10.

    Returns:
        float: average squared deviation

    References:
        René Michel, Igor Schnakenburg, Tobias von Martens. Targeting Uplift. An Introduction to Net Scores.
    """
    check_consistent_length(y_true_train, uplift_train, treatment_train)
    check_is_binary(treatment_train)

    check_consistent_length(y_true_val, uplift_val, treatment_val)
    check_is_binary(treatment_val)

    strategy_methods = ['overall', 'by_group']

    n_samples_train = len(y_true_train)
    n_samples_val = len(y_true_val)
    min_n_samples = min(n_samples_train, n_samples_val)

    if strategy not in strategy_methods:
        raise ValueError(
            f'Response rate supports only calculating methods in {strategy_methods},'
            f' got {strategy}.')

    if not isinstance(bins, int) or bins <= 0:
        raise ValueError(
            f'Bins should be positive integer. Invalid value bins: {bins}')

    if bins >= min_n_samples:
        raise ValueError(
            f'Number of bins = {bins} should be smaller than the length of y_true_train {n_samples_train}'
            f'and length of y_true_val {n_samples_val}')

    uplift_by_percentile_train = uplift_by_percentile(y_true_train, uplift_train, treatment_train,
                                                      strategy=strategy, bins=bins)
    uplift_by_percentile_val = uplift_by_percentile(y_true_val, uplift_val, treatment_val,
                                                    strategy=strategy, bins=bins)
    
    return np.mean(np.square(uplift_by_percentile_train['uplift'] - uplift_by_percentile_val['uplift']))


def max_prof_uplift(df_sorted, treatment_name, churn_name, pos_outcome, benefit, c_incentive, c_contact, a_cost=0):
  """Compute the maximum profit generated from an uplift model decided campaign

    This can be visualised by plotting plt.plot(perc, cumulative_profit)

    Args:
      df_sorted (pandas dataframe): dataframe with descending uplift predictions for each customer (i.e. highest 1st)
      treatment_name (string): column name of treatment columm (assuming 1 = treated)   
      churn_name (string): column name of churn column
      pos_outcome (int or float): 1 or 0 value in churn column indicating a positive outcome (i.e. purchase = 1, whereas churn = 0)
      benefit (int or float): the benefit of retaining a customer (e.g., the average customer lifetime value)
      c_incentive (int or float): the cost of the incentive if a customer accepts the offer
      c_contact (int or float): the cost of contacting a customer regardless of conversion
      a_cost (int or float): the fixed administration cost for the campaign

    Returns:
      1d array-like: the incremental increase in x, for plotting
      1d array-like: the cumulative profit per customer 

    References:
        Floris Devriendt, Jeroen Berrevoets, Wouter Verbeke. Why you should stop predicting customer churn and start using uplift models.
  """
#   VARIABLES

#       n_ct0             no. people not treated
#       n_ct1             no. people treated

#       n_y1_ct0          no. people not treated with +ve outcome
#       n_y1_ct1          no. people treated with +ve outcome

#       r_y1_ct0          mean of not treated people with +ve outcome
#       r_y1_ct1          mean of treated people with +ve outcome

#       cs                cumsum() of each variable

  n_ct0 = np.where(df_sorted[treatment_name] == 0, 1, 0)
  cs_n_ct0 = pd.Series(n_ct0.cumsum())

  n_ct1 = np.where(df_sorted[treatment_name] == 1, 1, 0)
  cs_n_ct1 = pd.Series(n_ct1.cumsum())

  if pos_outcome == 0:
    n_y1_ct0 = np.where((df_sorted[treatment_name] == 0) & (df_sorted[churn_name] == 0), 1, 0)
    n_y1_ct1 = np.where((df_sorted[treatment_name] == 1) & (df_sorted[churn_name] == 0), 1, 0)  

  elif pos_outcome == 1:
    n_y1_ct0 = np.where((df_sorted[treatment_name] == 0) & (df_sorted[churn_name] == 1), 1, 0)
    n_y1_ct1 = np.where((df_sorted[treatment_name] == 1) & (df_sorted[churn_name] == 1), 1, 0)
    
  cs_n_y1_ct0 = pd.Series(n_y1_ct0.cumsum())
  cs_n_y1_ct1 = pd.Series(n_y1_ct1.cumsum())

  cs_r_y1_ct0 = (cs_n_y1_ct0 / cs_n_ct0).fillna(0)
  cs_r_y1_ct1 = (cs_n_y1_ct1 / cs_n_ct1).fillna(0)

  cs_uplift = cs_r_y1_ct1 - cs_r_y1_ct0

  # Dataframe of all calculated variables
  df = pd.concat([cs_n_ct0,cs_n_ct1,cs_n_y1_ct0,cs_n_y1_ct1, cs_r_y1_ct0, cs_r_y1_ct1, cs_uplift], axis=1)
  df.columns = ['cs_n_ct0', 'cs_n_ct1', 'cs_n_y1_ct0', 'cs_n_y1_ct1', 'cs_r_y1_c0', 'cs_r_y1_ct1', 'cs_uplift']

  x = cs_n_ct0 + cs_n_ct1
  max = cs_n_ct0.max() + cs_n_ct1.max()

  t_profit = (x * cs_uplift * benefit) - (c_incentive * x * cs_r_y1_ct1) - (c_contact * x) - a_cost
  perc = x / max
  cumulative_profit = t_profit / max

  return perc, cumulative_profit

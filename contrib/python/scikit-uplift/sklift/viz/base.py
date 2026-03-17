import matplotlib.pyplot as plt
import numpy as np
from sklearn.utils.validation import check_consistent_length
from sklearn.utils._optional_dependencies import check_matplotlib_support

from ..utils import check_is_binary
from ..metrics import (
    uplift_curve, perfect_uplift_curve, uplift_auc_score,
    qini_curve, perfect_qini_curve, qini_auc_score,
    treatment_balance_curve, uplift_by_percentile
)


def plot_uplift_preds(trmnt_preds, ctrl_preds, log=False, bins=100):
    """Plot histograms of treatment, control and uplift predictions.

    Args:
        trmnt_preds (1d array-like): Predictions for all observations if they are treatment.
        ctrl_preds (1d array-like): Predictions for all observations if they are control.
        log (bool): Logarithm of source samples. Default is False.
        bins (integer or sequence): Number of histogram bins to be used. Default is 100.
            If an integer is given, bins + 1 bin edges are calculated and returned.
            If bins is a sequence, gives bin edges, including left edge of first bin and right edge of last bin.
            In this case, bins is returned unmodified. Default is 100.

    Returns:
        Object that stores computed values.
    """

    # TODO: Add k as parameter: vertical line on plots
    check_consistent_length(trmnt_preds, ctrl_preds)

    if not isinstance(bins, int) or bins <= 0:
        raise ValueError(
            f'Bins should be positive integer. Invalid value for bins: {bins}')

    if log:
        trmnt_preds = np.log(trmnt_preds + 1)
        ctrl_preds = np.log(ctrl_preds + 1)

    fig, axes = plt.subplots(ncols=3, nrows=1, figsize=(20, 7))
    axes[0].hist(
        trmnt_preds, bins=bins, alpha=0.3, color='b', label='Treated', histtype='stepfilled')
    axes[0].set_ylabel('Probability hist')
    axes[0].legend()
    axes[0].set_title('Treatment predictions')

    axes[1].hist(
        ctrl_preds, bins=bins, alpha=0.5, color='y', label='Not treated', histtype='stepfilled')
    axes[1].legend()
    axes[1].set_title('Control predictions')

    axes[2].hist(
        trmnt_preds - ctrl_preds, bins=bins, alpha=0.5, color='green', label='Uplift', histtype='stepfilled')
    axes[2].legend()
    axes[2].set_title('Uplift predictions')

    return axes


class UpliftCurveDisplay:
    """Qini and Uplift curve visualization.

    Args:
        x_actual, y_actual (array (shape = [>2]), array (shape = [>2])): Points on a curve
        x_baseline, y_baseline (array (shape = [>2]), array (shape = [>2])): Points on a random curve
        x_perfect, y_perfect (array (shape = [>2]), array (shape = [>2])): Points on a perfect curve
        random (bool): Plotting a random curve
        perfect (bool): Plotting a perfect curve
        estimator_name (str): Name of estimator. If None, the estimator name is not shown.
    """

    def __init__(self, x_actual, y_actual, x_baseline=None,
                 y_baseline=None, x_perfect=None, y_perfect=None,
                 random=None, perfect=None, estimator_name=None):
        self.x_actual = x_actual
        self.y_actual = y_actual
        self.x_baseline = x_baseline
        self.y_baseline = y_baseline
        self.x_perfect = x_perfect
        self.y_perfect = y_perfect
        self.random = random
        self.perfect = perfect
        self.estimator_name = estimator_name

    def plot(self, auc_score, ax=None, name=None, title=None, **kwargs):
        """Plot visualization

        Args:
            auc_score (float): Area under curve.§
            ax (matplotlib axes): Axes object to plot on. If `None`, a new figure and axes is created. Default is None.
            name (str): Name of ROC Curve for labeling. If `None`, use the name of the estimator. Default is None.
            title (str): Title plot. Default is None.

        Returns:
            Object that stores computed values
        """
        check_matplotlib_support('UpliftCurveDisplay.plot')

        name = self.estimator_name if name is None else name

        line_kwargs = {}
        if auc_score is not None and name is not None:
            line_kwargs["label"] = f"{name} ({title} = {auc_score:0.2f})"
        elif auc_score is not None:
            line_kwargs["label"] = f"{title} = {auc_score:0.2f}"
        elif name is not None:
            line_kwargs["label"] = name

        line_kwargs.update(**kwargs)

        if ax is None:
            fig, ax = plt.subplots()

        self.line_, = ax.plot(self.x_actual, self.y_actual, **line_kwargs)

        if self.random:
            ax.plot(self.x_baseline, self.y_baseline, label="Random")
            ax.fill_between(self.x_actual, self.y_actual, self.y_baseline, alpha=0.2)

        if self.perfect:
            ax.plot(self.x_perfect, self.y_perfect, label="Perfect")

        ax.set_xlabel('Number targeted')
        ax.set_ylabel('Number of incremental outcome')

        if self.random == self.perfect:
            variance = False
        else:
            variance = True

        if len(ax.lines) > 4:
            ax.lines.pop(len(ax.lines) - 1)
            if variance == False:
                ax.lines.pop(len(ax.lines) - 1)

        if "label" in line_kwargs:
            ax.legend(loc=u'upper left', bbox_to_anchor=(1, 1))

        self.ax_ = ax
        self.figure_ = ax.figure

        return self


def plot_qini_curve(y_true, uplift, treatment,
                    random=True, perfect=True, negative_effect=True, ax=None, name=None, **kwargs):
    """Plot Qini curves from predictions.

    Args:
        y_true (1d array-like): Ground truth (correct) binary labels.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        random (bool): Draw a random curve. Default is True.
        perfect (bool): Draw a perfect curve. Default is True.
        negative_effect (bool): If True, optimum Qini Curve contains the negative effects
            (negative uplift because of campaign). Otherwise, optimum Qini Curve will not
            contain the negative effects. Default is True.
        ax (object): The graph on which the function will be built. Default is None.
        name (string): The name of the function. Default is None.

    Returns:
        Object that stores computed values.

    Example::

        from sklift.viz import plot_qini_curve


        qini_disp = plot_qini_curve(
            y_test, uplift_predicted, trmnt_test,
            perfect=True, name='Model name'
        );

        qini_disp.figure_.suptitle("Qini curve");
    """
    check_matplotlib_support('plot_qini_curve')
    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)

    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)
    x_actual, y_actual = qini_curve(y_true, uplift, treatment)

    if random:
        x_baseline, y_baseline = x_actual, x_actual * y_actual[-1] / len(y_true)
    else:
        x_baseline, y_baseline = None, None

    if perfect:
        x_perfect, y_perfect = perfect_qini_curve(
            y_true, treatment, negative_effect)
    else:
        x_perfect, y_perfect = None, None

    viz = UpliftCurveDisplay(
        x_actual=x_actual,
        y_actual=y_actual,
        x_baseline=x_baseline,
        y_baseline=y_baseline,
        x_perfect=x_perfect,
        y_perfect=y_perfect,
        random=random,
        perfect=perfect,
        estimator_name=name,
    )

    auc = qini_auc_score(y_true, uplift, treatment, negative_effect)

    return viz.plot(auc, ax=ax, title="AUC", **kwargs)


def plot_uplift_curve(y_true, uplift, treatment,
                      random=True, perfect=True, ax=None, name=None, **kwargs):
    """Plot Uplift curves from predictions.

    Args:
        y_true (1d array-like): Ground truth (correct) binary labels.
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        random (bool): Draw a random curve. Default is True.
        perfect (bool): Draw a perfect curve. Default is True.
        ax (object): The graph on which the function will be built. Default is None.
        name (string): The name of the function. Default is None.

    Returns:
        Object that stores computed values.

    Example::

        from sklift.viz import plot_uplift_curve


        uplift_disp = plot_uplift_curve(
            y_test, uplift_predicted, trmnt_test,
            perfect=True, name='Model name'
        );

        uplift_disp.figure_.suptitle("Uplift curve");
    """
    check_matplotlib_support('plot_uplift_curve')
    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)

    y_true, uplift, treatment = np.array(y_true), np.array(uplift), np.array(treatment)
    x_actual, y_actual = uplift_curve(y_true, uplift, treatment)

    if random:
        x_baseline, y_baseline = x_actual, x_actual * y_actual[-1] / len(y_true)
    else:
        x_baseline, y_baseline = None, None

    if perfect:
        x_perfect, y_perfect = perfect_uplift_curve(y_true, treatment)
    else:
        x_perfect, y_perfect = None, None

    viz = UpliftCurveDisplay(
        x_actual=x_actual,
        y_actual=y_actual,
        x_baseline=x_baseline,
        y_baseline=y_baseline,
        x_perfect=x_perfect,
        y_perfect=y_perfect,
        random=random,
        perfect=perfect,
        estimator_name=name,
    )

    auc = uplift_auc_score(y_true, uplift, treatment)

    return viz.plot(auc, ax=ax, title="AUC", **kwargs)


def plot_uplift_by_percentile(y_true, uplift, treatment, strategy='overall',
                              kind='line', bins=10, string_percentiles=True):
    """Plot uplift score, treatment response rate and control response rate at each percentile.

    Treatment response rate ia a target mean in the treatment group.
    Control response rate is a target mean in the control group.
    Uplift score is a difference between treatment response rate and control response rate.

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
                sorted by uplift predictions. Then the difference between these conversions is calculated.

        kind (string, ['line', 'bar']): The type of plot to draw. Default is 'line'.

            * ``'line'``:
                Generates a line plot.
            * ``'bar'``:
                Generates a traditional bar-style plot.

        bins (int): Determines а number of bins (and the relative percentile) in the test data. Default is 10.
        string_percentiles (bool): type of xticks: float or string to plot. Default is True (string).

    Returns:
        Object that stores computed values.
    """

    strategy_methods = ['overall', 'by_group']
    kind_methods = ['line', 'bar']

    check_consistent_length(y_true, uplift, treatment)
    check_is_binary(treatment)
    check_is_binary(y_true)
    n_samples = len(y_true)

    if strategy not in strategy_methods:
        raise ValueError(f'Response rate supports only calculating methods in {strategy_methods},'
                         f' got {strategy}.')

    if kind not in kind_methods:
        raise ValueError(f'Function supports only types of plots in {kind_methods},'
                         f' got {kind}.')

    if not isinstance(bins, int) or bins <= 0:
        raise ValueError(
            f'Bins should be positive integer. Invalid value bins: {bins}')

    if bins >= n_samples:
        raise ValueError(
            f'Number of bins = {bins} should be smaller than the length of y_true {n_samples}')

    if not isinstance(string_percentiles, bool):
        raise ValueError(f'string_percentiles flag should be bool: True or False.'
                         f' Invalid value string_percentiles: {string_percentiles}')

    df = uplift_by_percentile(y_true, uplift, treatment, strategy=strategy,
                              std=True, total=True, bins=bins, string_percentiles=False)

    percentiles = df.index[:bins].values.astype(float)

    response_rate_trmnt = df.loc[percentiles, 'response_rate_treatment'].values
    std_trmnt = df.loc[percentiles, 'std_treatment'].values

    response_rate_ctrl = df.loc[percentiles, 'response_rate_control'].values
    std_ctrl = df.loc[percentiles, 'std_control'].values

    uplift_score = df.loc[percentiles, 'uplift'].values
    std_uplift = df.loc[percentiles, 'std_uplift'].values

    uplift_weighted_avg = df.loc['total', 'uplift']

    check_consistent_length(percentiles, response_rate_trmnt,
                            response_rate_ctrl, uplift_score,
                            std_trmnt, std_ctrl, std_uplift)

    if kind == 'line':
        _, axes = plt.subplots(ncols=1, nrows=1, figsize=(8, 6))
        axes.errorbar(percentiles, response_rate_trmnt, yerr=std_trmnt,
                      linewidth=2, color='forestgreen', label='treatment\nresponse rate')
        axes.errorbar(percentiles, response_rate_ctrl, yerr=std_ctrl,
                      linewidth=2, color='orange', label='control\nresponse rate')
        axes.errorbar(percentiles, uplift_score, yerr=std_uplift,
                      linewidth=2, color='red', label='uplift')
        axes.fill_between(percentiles, response_rate_trmnt,
                          response_rate_ctrl, alpha=0.1, color='red')

        if np.amin(uplift_score) < 0:
            axes.axhline(y=0, color='black', linewidth=1)

        if string_percentiles:  # string percentiles for plotting
            percentiles_str = [f"0-{percentiles[0]:.0f}"] + \
                              [f"{percentiles[i]:.0f}-{percentiles[i + 1]:.0f}" for i in range(len(percentiles) - 1)]
            axes.set_xticks(percentiles)
            axes.set_xticklabels(percentiles_str, rotation=45)
        else:
            axes.set_xticks(percentiles)

        axes.legend(loc='upper right')
        axes.set_title(
            f'Uplift by percentile\nweighted average uplift = {uplift_weighted_avg:.4f}')
        axes.set_xlabel('Percentile')
        axes.set_ylabel(
            'Uplift = treatment response rate - control response rate')

    else:  # kind == 'bar'
        delta = percentiles[0]
        fig, axes = plt.subplots(ncols=1, nrows=2, figsize=(8, 6), sharex=True, sharey=True)
        fig.text(0.04, 0.5, 'Uplift = treatment response rate - control response rate',
                 va='center', ha='center', rotation='vertical')

        axes[1].bar(np.array(percentiles) - delta / 6, response_rate_trmnt, delta / 3,
                    yerr=std_trmnt, color='forestgreen', label='treatment\nresponse rate')
        axes[1].bar(np.array(percentiles) + delta / 6, response_rate_ctrl, delta / 3,
                    yerr=std_ctrl, color='orange', label='control\nresponse rate')
        axes[0].bar(np.array(percentiles), uplift_score, delta / 1.5,
                    yerr=std_uplift, color='red', label='uplift')

        axes[0].legend(loc='upper right')
        axes[0].tick_params(axis='x', bottom=False)
        axes[0].axhline(y=0, color='black', linewidth=1)
        axes[0].set_title(
            f'Uplift by percentile\nweighted average uplift = {uplift_weighted_avg:.4f}')

        if string_percentiles:  # string percentiles for plotting
            percentiles_str = [f"0-{percentiles[0]:.0f}"] + \
                              [f"{percentiles[i]:.0f}-{percentiles[i + 1]:.0f}" for i in range(len(percentiles) - 1)]
            axes[1].set_xticks(percentiles)
            axes[1].set_xticklabels(percentiles_str, rotation=45)

        else:
            axes[1].set_xticks(percentiles)

        axes[1].legend(loc='upper right')
        axes[1].axhline(y=0, color='black', linewidth=1)
        axes[1].set_xlabel('Percentile')
        axes[1].set_title('Response rate by percentile')

    return axes


def plot_treatment_balance_curve(uplift, treatment, random=True, winsize=0.1):
    """Plot Treatment Balance curve.

    Args:
        uplift (1d array-like): Predicted uplift, as returned by a model.
        treatment (1d array-like): Treatment labels.
        random (bool): Draw a random curve. Default is True.
        winsize (float): Size of the sliding window to apply. Should be between 0 and 1, extremes excluded. Default is 0.1.

    Returns:
        Object that stores computed values.
    """

    check_consistent_length(uplift, treatment)
    check_is_binary(treatment)

    if (winsize <= 0) or (winsize >= 1):
        raise ValueError(
            'winsize should be between 0 and 1, extremes excluded')

    x_tb, y_tb = treatment_balance_curve(
        uplift, treatment, winsize=int(len(uplift) * winsize))

    _, ax = plt.subplots(ncols=1, nrows=1, figsize=(14, 7))

    ax.plot(x_tb, y_tb, label='Model', color='b')

    if random:
        y_tb_random = np.average(treatment) * np.ones_like(x_tb)

        ax.plot(x_tb, y_tb_random, label='Random', color='black')
        ax.fill_between(x_tb, y_tb, y_tb_random, alpha=0.2, color='b')

    ax.legend()
    ax.set_title('Treatment balance curve')
    ax.set_xlabel('Percentage targeted')
    ax.set_ylabel('Balance: treatment / (treatment + control)')

    return ax

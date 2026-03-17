from __future__ import annotations

import copy
from typing import Any
from typing import Container
from typing import Iterable

import optuna
from optuna import logging
from optuna._imports import try_import
from optuna.distributions import BaseDistribution
from optuna.samplers import BaseSampler
from optuna.trial import FrozenTrial
from optuna.trial import TrialState
from optuna_dashboard.preferential._system_attrs import get_n_generate
from optuna_dashboard.preferential._system_attrs import get_preferences
from optuna_dashboard.preferential._system_attrs import get_skipped_trial_ids
from optuna_dashboard.preferential._system_attrs import is_skipped_trial
from optuna_dashboard.preferential._system_attrs import report_preferences
from optuna_dashboard.preferential._system_attrs import set_n_generate


with try_import() as _imports:
    from optuna_dashboard.preferential.samplers.gp import PreferentialGPSampler


_logger = logging.get_logger(__name__)
_SYSTEM_ATTR_PREFERENTIAL_STUDY = "preference:is_preferential"


class PreferentialStudy:
    """A Study-like class for preferential optimization.

    This object provides interfaces to create a new `Trial`_, set/get results
    of pairwise comparison called preferences.

    .. _Trial: https://optuna.readthedocs.io/en/stable/reference/generated/\
    optuna.trial.Trial.html#optuna.trial.Trial

    Note that the direct use of this constructor is not recommended.
    To create and load a study, please refer to the documentation of
    :func:`~optuna_dashboard.preferential.create_study` and
    :func:`~optuna_dashboard.preferential.load_study` respectively.

    .. note::
        Preferential optimization is an experimental feature (introduced in v0.13.0).
        The interface may change in newer versions without prior notice.
    """

    def __init__(self, study: optuna.Study) -> None:
        self._study = study

    @property
    def trials(self) -> list[FrozenTrial]:
        """Return the all trials.

        .. seealso::

            See `Study.trials`_ for details.

            .. _Study.trials: https://optuna.readthedocs.io/en/stable/reference/generated/\
            optuna.study.Study.html#optuna.study.Study.trials

        Returns:
            A list of FrozenTrial object
        """
        return self._study.trials

    @property
    def best_trials(self) -> list[FrozenTrial]:
        """Return the trials that is not dominated by other trials.

        Returns:
            A list of FrozenTrial object
        """
        return get_best_trials(self._study._study_id, self._study._storage)

    @property
    def study_name(self) -> str:
        """Return the name of the study.

        Returns:
            A string object
        """
        return self._study.study_name

    @property
    def user_attrs(self) -> dict[str, Any]:
        """Return user attributes of the study.

        .. seealso::

            See `Study.user_attrs`_ for details.

            .. _Study.user_attrs: https://optuna.readthedocs.io/en/stable/reference/\
            generated/optuna.study.Study.html#optuna.study.Study.user_attrs

        Returns:
            A dictionary containing all user attributes
        """
        return self._study.user_attrs

    @property
    def preferences(self) -> list[tuple[FrozenTrial, FrozenTrial]]:
        """Return results of pairwise comparison.

        Returns:
            A list of the pair of FrozenTrial objects. The left trial is better than the right one.
        """
        return self.get_preferences(deepcopy=True)

    def get_trials(
        self,
        deepcopy: bool = True,
        states: Container[optuna.trial.TrialState] | None = None,
    ) -> list[FrozenTrial]:
        """Return the trials that is not dominated by other trials.

        .. seealso::

            See `Study.get_trials`_ for details.

            .. _Study.get_trials: https://optuna.readthedocs.io/en/stable/reference/\
            generated/optuna.study.Study.html#optuna.study.Study.get_trials

        Args:
            deepcopy:
                Flag to control whether to apply ``copy.deepcopy()`` to the trials.
                Note that if you set the flag to :obj:`False`, you shouldn't mutate
                any fields of the returned trial. Otherwise the internal state of
                the study may corrupt and unexpected behavior may happen.
            states:
                Trial states to filter on. If :obj:`None`, include all states.

        Returns:
            A list of FrozenTrial object
        """
        return self._study.get_trials(deepcopy, states)

    def ask(self, fixed_distributions: dict[str, BaseDistribution] | None = None) -> optuna.Trial:
        """Create a new trial from which hyperparameters can be suggested.

        .. seealso::

            See `Study.ask`_ for details.

            .. _Study.ask: https://optuna.readthedocs.io/en/stable/reference/\
            generated/optuna.study.Study.html#optuna.study.Study.ask

        Args:
            fixed_distributions:
                A dictionary containing the parameter names and parameter's distributions. Each
                parameter in this dictionary is automatically suggested for the returned trial,
                even when the suggest method is not explicitly invoked by the user. If this
                argument is set to :obj:`None`, no parameter is automatically suggested.

        Returns:
            A Trial object.
        """
        return self._study.ask(fixed_distributions)

    def add_trial(self, trial: FrozenTrial) -> None:
        """Add a trial to the study.

        .. seealso::

            See `Study.add_trials()`_ for details.

            .. _Study.add_trials(): https://optuna.readthedocs.io/en/stable/reference/\
            generated/optuna.study.Study.html#optuna.study.Study.add_trials
        """
        self._study.add_trial(trial)

    def add_trials(self, trials: Iterable[FrozenTrial]) -> None:
        """Add trials to the study.

        .. seealso::

            See `Study.add_trials()`_ for details.

            .. _Study.add_trials(): https://optuna.readthedocs.io/en/stable/reference/\
            generated/optuna.study.Study.html#optuna.study.Study.add_trials
        """
        self._study.add_trials(trials)

    def enqueue_trial(
        self,
        params: dict[str, Any],
        user_attrs: dict[str, Any] | None = None,
        skip_if_exists: bool = False,
    ) -> None:
        """Enqueue a trial with given parameter values.

        You can fix the next sampling parameters which will be evaluated in your
        objective function.

        .. seealso::

            See `Study.enqueue_trials`_ for details.

            .. _Study.get_trials: https://optuna.readthedocs.io/en/stable/reference/\
            generated/optuna.study.Study.html#optuna.study.Study.enqueue_trials

        Args:
            params:
                Parameter values to pass your objective function.
            user_attrs:
                A dictionary of user-specific attributes other than ``params``.
            skip_if_exists:
                When :obj:`True`, prevents duplicate trials from being enqueued again.

                .. note::
                    This method might produce duplicated trials if called simultaneously
                    by multiple processes at the same time with same ``params`` dict.
        """
        self._study.enqueue_trial(params, user_attrs, skip_if_exists)

    def report_preference(
        self,
        better_trials: FrozenTrial | list[FrozenTrial],
        worse_trials: FrozenTrial | list[FrozenTrial],
    ) -> None:
        """Report results of pairwise comparison.

        Args:
            better_trials:
                Trials that are better than worse_trials.
            worse_trials:
                Trials that are worse than better_trials.
        """
        if not isinstance(better_trials, list):
            better_trials = [better_trials]
        if not isinstance(worse_trials, list):
            worse_trials = [worse_trials]

        report_preferences(
            self._study._study_id,
            self._study._storage,
            [(b.number, w.number) for b in better_trials for w in worse_trials],
        )

    def get_preferences(self, *, deepcopy: bool = True) -> list[tuple[FrozenTrial, FrozenTrial]]:
        """Return results of pairwise comparison.

        Args:
            deepcopy:
                Flag to control whether to apply ``copy.deepcopy()`` to the trials.
                Note that if you set the flag to :obj:`False`, you shouldn't mutate
                any fields of the returned trial. Otherwise the internal state of
                the study may corrupt and unexpected behavior may happen.

        Returns:
            A list of the pair of FrozenTrial objects. The left trial is better than the right one.
        """

        preferences = get_preferences(
            self._study._storage.get_study_system_attrs(self._study._study_id)
        )  # Must come before study.get_trials()
        trials = self._study.get_trials(deepcopy=deepcopy)
        return [(trials[better], trials[worse]) for (better, worse) in preferences]

    def set_user_attr(self, key: str, value: Any) -> None:
        """Set a user attribute to the study.

        Args:
            key: A key string of the attribute.
            value: A value of the attribute. The value should be JSON serializable.

        .. seealso::

            See the `tutorial for user attributes <https://optuna.readthedocs.io/en/stable/\
            tutorial/20_recipes/003_attributes.html>`_ on Optuna's documentation.
        """
        self._study.set_user_attr(key, value)

    def should_generate(self) -> bool:
        """Return whether the generator should generate a new trial now.

        Returns :obj:`True` if the number of trials not reported bad and not skipped are less than
        :attr:`~optuna_dashboard.preferential.PreferentialStudy.n_generate`. Users are recommended
        to generate a new trial if this method returns :obj:`True`, and to wait for human
        evaluation if this method returns :obj:`False`.
        """
        study_system_attrs = self._study._storage.get_study_system_attrs(
            self._study._study_id
        )  # Must come before _study.get_trials()
        trials = self._study.get_trials(
            deepcopy=False, states=(TrialState.COMPLETE, TrialState.RUNNING)
        )
        worse_trial_numbers = {worse for _, worse in get_preferences(study_system_attrs)}
        skipped_trial_ids = set(get_skipped_trial_ids(study_system_attrs))
        active_trials = [
            t
            for t in trials
            if t.number not in worse_trial_numbers and t._trial_id not in skipped_trial_ids
        ]
        return len(active_trials) < get_n_generate(
            self._study._storage.get_study_system_attrs(self._study._study_id)
        )


def get_best_trials(study_id: int, storage: optuna.storages.BaseStorage) -> list[FrozenTrial]:
    preferences = get_preferences(storage.get_study_system_attrs(study_id))
    worse_numbers = {worse for _, worse in preferences}
    nondominated_numbers = {better for better, _ in preferences if better not in worse_numbers}
    trials = storage.get_all_trials(study_id, deepcopy=False)

    study_system_attrs = storage.get_study_system_attrs(study_id)

    best_trials = []
    for n in nondominated_numbers:
        t = trials[n]
        if is_skipped_trial(t._trial_id, study_system_attrs):
            continue
        best_trials.append(copy.deepcopy(t))
    return best_trials


def create_study(
    *,
    n_generate: int,
    storage: str | optuna.storages.BaseStorage | None = None,
    sampler: BaseSampler | None = None,
    study_name: str | None = None,
    load_if_exists: bool = False,
) -> PreferentialStudy:
    """Like ``optuna.create_study()``, but for preferential optimization.

    Example:

        .. testcode::

            import optuna
            from optuna_dashboard.preferential import create_study


            study = create_study()
            trial = study.ask()

    Args:
        n_generate:
            The number of active trials to keep.
            :func:`~optuna_dashboard.preferential.PreferentialStudy.should_generate` returns
            :obj:`True` if the number of trials not reported bad and not skipped are less than
            ``n_generate``.

        storage:
            Database URL. If this argument is set to None, in-memory storage is used, and the
            :class:`~optuna_dashboard.preferential.PreferentialStudy` will not be persistent.

        sampler:
            A sampler object that implements background algorithm for value suggestion.
            If :obj:`None` is specified,
            :class:`~optuna_dashboard.preferential.samplers.gp.PreferentialGPSampler` is used.
            Please note that most Optuna samplers does not work efficiently for preferential
            optimization.

        study_name:
            Study's name. If this argument is set to None, a unique name is generated
            automatically.

        load_if_exists:
            Flag to control the behavior to handle a conflict of study names.
            In the case where a study named ``study_name`` already exists in the ``storage``,
            a :class:`~optuna.exceptions.DuplicatedStudyError` is raised if ``load_if_exists`` is
            set to :obj:`False`.
            Otherwise, the creation of the study is skipped, and the existing one is returned.

    Returns:
        A :class:`~optuna_dashboard.preferential.PreferentialStudy` object.

    .. note::
        Preferential optimization is an experimental feature (introduced in v0.13.0).
        The interface may change in newer versions without prior notice.
    """
    try:
        if sampler is None:
            _imports.check()  # If BoTorch is not installed, raise ImportError.
            sampler = PreferentialGPSampler()

        study = optuna.create_study(
            storage=storage,
            sampler=sampler,
            study_name=study_name,
        )
        study._storage.set_study_system_attr(
            study._study_id, _SYSTEM_ATTR_PREFERENTIAL_STUDY, True
        )
        set_n_generate(study._study_id, study._storage, n_generate)
        return PreferentialStudy(study)

    except optuna.exceptions.DuplicatedStudyError:
        if load_if_exists:
            assert study_name is not None
            assert storage is not None

            _logger.info(
                "Using an existing study with name '{}' instead of creating a new one.".format(
                    study_name
                )
            )
            return load_study(
                study_name=study_name,
                storage=storage,
                sampler=sampler,
            )
        else:
            raise


def load_study(
    *,
    study_name: str | None,
    storage: str | optuna.storages.BaseStorage,
    sampler: BaseSampler | None = None,
) -> PreferentialStudy:
    """Like ``optuna.load_study()``, but for preferential optimization.

    Example:

        .. testsetup::

            import os

            if os.path.exists("example.db"):
                raise RuntimeError("'example.db' already exists. Please remove it.")

        .. testcode::

            import optuna
            from optuna_dashboard.preferential import create_study
            from optuna_dashboard.preferential import load_study

            study = create_study(storage="sqlite:///example.db", study_name="my_study")
            study.ask()

            loaded_study = load_study(study_name="my_study", storage="sqlite:///example.db")
            assert len(loaded_study.trials) == len(study.trials)

        .. testcleanup::

            os.remove("example.db")

    Args:
        study_name:
            Study's name. Each study has a unique name as an identifier. If :obj:`None`, checks
            whether the storage contains a single study, and if so loads that study.
            ``study_name`` is required if there are multiple studies in the storage.
        storage:
            Database URL such as ``sqlite:///example.db``. Please see also the documentation of
            :func:`~optuna.study.create_study` for further details.
        sampler:
            A sampler object that implements background algorithm for value suggestion.
            If :obj:`None` is specified,
            :class:`~optuna_dashboard.preferential.samplers.gp.PreferentialGPSampler` is used.
            Please note that most Optuna samplers does not work efficiently for preferential
            optimization.

    Returns:
        A :class:`~optuna_dashboard.preferential.PreferentialStudy` object.

    .. note::
        Preferential optimization is an experimental feature (introduced in v0.13.0).
        The interface may change in newer versions without prior notice.
    """
    if sampler is None:
        _imports.check()  # If BoTorch is not installed, raise ImportError.
        sampler = PreferentialGPSampler()

    study = optuna.load_study(study_name=study_name, storage=storage, sampler=sampler)
    system_attrs = study._storage.get_study_system_attrs(study._study_id)
    if not system_attrs.get(_SYSTEM_ATTR_PREFERENTIAL_STUDY):
        raise ValueError("The study is not a PreferentialStudy.")
    return PreferentialStudy(study)

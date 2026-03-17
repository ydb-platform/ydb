import warnings

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.utils.multiclass import type_of_target
from sklearn.utils.validation import check_consistent_length

from ..utils import check_is_binary


class SoloModel(BaseEstimator):
    """aka Treatment Dummy approach, or Single model approach, or S-Learner.

    Fit solo model on whole dataset with 'treatment' as an additional feature.

    Each object from the test sample is scored twice: with the communication flag equal to 1 and equal to 0.
    Subtracting the probabilities for each observation, we get the uplift.

    Return delta of predictions for each example.

    Read more in the :ref:`User Guide <SoloModel>`.

    Args:
        estimator (estimator object implementing 'fit'): The object to use to fit the data.
        method (string, ’dummy’ or ’treatment_interaction’, default='dummy'): Specifies the approach:

            * ``'dummy'``:
                Single model;
            * ``'treatment_interaction'``:
                Single model including treatment interactions.

    Attributes:
        trmnt_preds_ (array-like, shape (n_samples, )): Estimator predictions on samples when treatment.
        ctrl_preds_ (array-like, shape (n_samples, )): Estimator predictions on samples when control.

    Example::

        # import approach
        from sklift.models import SoloModel
        # import any estimator adheres to scikit-learn conventions
        from catboost import CatBoostClassifier


        sm = SoloModel(CatBoostClassifier(verbose=100, random_state=777))  # define approach
        sm = sm.fit(X_train, y_train, treat_train, estimator_fit_params={{'plot': True})  # fit the model
        uplift_sm = sm.predict(X_val)  # predict uplift

    References:
        Lo, Victor. (2002). The True Lift Model - A Novel Data Mining Approach to Response Modeling
        in Database Marketing. SIGKDD Explorations. 4. 78-86.

    See Also:

        **Other approaches:**

        * :class:`.ClassTransformation`: Class Variable Transformation approach.
        * :class:`.ClassTransformationReg`: Transformed Outcome approach.
        * :class:`.TwoModels`: Double classifier approach.

        **Other:**

        * :func:`.plot_uplift_preds`: Plot histograms of treatment, control and uplift predictions.
    """

    def __init__(self, estimator, method='dummy'):
        self.estimator = estimator
        self.method = method
        self.trmnt_preds_ = None
        self.ctrl_preds_ = None
        self._type_of_target = None

        all_methods = ['dummy', 'treatment_interaction']
        if method not in all_methods:
            raise ValueError("SoloModel approach supports only methods in %s, got"
                             " %s." % (all_methods, method))

    def fit(self, X, y, treatment, estimator_fit_params=None):
        """Fit the model according to the given training data.

        For each test example calculate predictions on new set twice: by the first and second models.
        After that calculate uplift as a delta between these predictions.

        Return delta of predictions for each example.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of
                samples and n_features is the number of features.
            y (array-like, shape (n_samples,)): Binary target vector relative to X.
            treatment (array-like, shape (n_samples,)): Binary treatment vector relative to X.
            estimator_fit_params (dict, optional): Parameters to pass to the fit method of the estimator.

        Returns:
            object: self
        """

        check_consistent_length(X, y, treatment)
        check_is_binary(treatment)
        treatment_values = np.unique(treatment)

        if len(treatment_values) != 2:
            raise ValueError("Expected only two unique values in treatment vector, got %s" % len(treatment_values))

        if self.method == 'dummy':
            if isinstance(X, np.ndarray):
                X_mod = np.column_stack((X, treatment))
            elif isinstance(X, pd.DataFrame):
                X_mod = X.assign(treatment=treatment)
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame in training vector X, got %s" % type(X))

        if self.method == 'treatment_interaction':
            if isinstance(X, np.ndarray):
                X_mod = np.column_stack((X, np.multiply(X, np.array(treatment).reshape(-1, 1)), treatment))
            elif isinstance(X, pd.DataFrame):
                X_mod = pd.concat([
                    X,
                    X.apply(lambda x: x * treatment)
                        .rename(columns=lambda x: str(x) + '_treatment_interaction')
                ], axis=1) \
                    .assign(treatment=treatment)
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame in training vector X, got %s" % type(X))

        self._type_of_target = type_of_target(y)

        if estimator_fit_params is None:
            estimator_fit_params = {}
        self.estimator.fit(X_mod, y, **estimator_fit_params)
        return self

    def predict(self, X):
        """Perform uplift on samples in X.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples
                and n_features is the number of features.

        Returns:
            array (shape (n_samples,)): uplift
        """

        if self.method == 'dummy':
            if isinstance(X, np.ndarray):
                X_mod_trmnt = np.column_stack((X, np.ones(X.shape[0])))
                X_mod_ctrl = np.column_stack((X, np.zeros(X.shape[0])))
            elif isinstance(X, pd.DataFrame):
                X_mod_trmnt = X.assign(treatment=np.ones(X.shape[0]))
                X_mod_ctrl = X.assign(treatment=np.zeros(X.shape[0]))
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame in training vector X, got %s" % type(X))

        if self.method == 'treatment_interaction':
            if isinstance(X, np.ndarray):
                X_mod_trmnt = np.column_stack((X, np.multiply(X, np.ones((X.shape[0], 1))), np.ones(X.shape[0])))
                X_mod_ctrl = np.column_stack((X, np.multiply(X, np.zeros((X.shape[0], 1))), np.zeros(X.shape[0])))
            elif isinstance(X, pd.DataFrame):
                X_mod_trmnt = pd.concat([
                    X,
                    X.apply(lambda x: x * np.ones(X.shape[0]))
                        .rename(columns=lambda x: str(x) + '_treatment_interaction')
                ], axis=1) \
                    .assign(treatment=np.ones(X.shape[0]))
                X_mod_ctrl = pd.concat([
                    X,
                    X.apply(lambda x: x * np.zeros(X.shape[0]))
                        .rename(columns=lambda x: str(x) + '_treatment_interaction')
                ], axis=1) \
                    .assign(treatment=np.zeros(X.shape[0]))
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame in training vector X, got %s" % type(X))

        if self._type_of_target == 'binary':
            self.trmnt_preds_ = self.estimator.predict_proba(X_mod_trmnt)[:, 1]
            self.ctrl_preds_ = self.estimator.predict_proba(X_mod_ctrl)[:, 1]
        else:
            self.trmnt_preds_ = self.estimator.predict(X_mod_trmnt)
            self.ctrl_preds_ = self.estimator.predict(X_mod_ctrl)

        uplift = self.trmnt_preds_ - self.ctrl_preds_
        return uplift


class ClassTransformation(BaseEstimator):
    """aka Class Variable Transformation or Revert Label approach.

    Redefine target variable, which indicates that treatment make some impact on target or
    did target is negative without treatment: ``Z = Y * W + (1 - Y)(1 - W)``,

    where ``Y`` - target vector, ``W`` - vector of binary communication flags.

    Then, ``Uplift ~ 2 * (Z == 1) - 1``

    Returns only uplift predictions.

    Read more in the :ref:`User Guide <ClassTransformation>`.

    Args:
        estimator (estimator object implementing 'fit'): The object to use to fit the data.

    Example::

        # import approach
        from sklift.models import ClassTransformation
        # import any estimator adheres to scikit-learn conventions
        from catboost import CatBoostClassifier


        # define approach
        ct = ClassTransformation(CatBoostClassifier(verbose=100, random_state=777))
        # fit the model
        ct = ct.fit(X_train, y_train, treat_train, estimator_fit_params={{'plot': True})
        # predict uplift
        uplift_ct = ct.predict(X_val)

    References:
        Maciej Jaskowski and Szymon Jaroszewicz. Uplift modeling for clinical trial data.
        ICML Workshop on Clinical Data Analysis, 2012.

    See Also:

        **Other approaches:**

        * :class:`.ClassTransformationReg`: Transformed Outcome approach.
        * :class:`.SoloModel`: Single model approach.
        * :class:`.TwoModels`: Double classifier approach.
    """

    def __init__(self, estimator):
        self.estimator = estimator
        self._type_of_target = None

    def fit(self, X, y, treatment, estimator_fit_params=None):
        """Fit the model according to the given training data.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples and
                n_features is the number of features.
            y (array-like, shape (n_samples,)): Target vector relative to X.
            treatment (array-like, shape (n_samples,)): Binary treatment vector relative to X.
            estimator_fit_params (dict, optional): Parameters to pass to the fit method of the estimator.

        Returns:
            object: self
        """

        check_consistent_length(X, y, treatment)
        check_is_binary(treatment)
        self._type_of_target = type_of_target(y)

        if self._type_of_target != 'binary':
            raise ValueError("This approach is only suitable for binary classification problem")

        y_mod = (np.array(y) == np.array(treatment)).astype(int)

        if estimator_fit_params is None:
            estimator_fit_params = {}
        self.estimator.fit(X, y_mod, **estimator_fit_params)
        return self

    def predict(self, X):
        """Perform uplift on samples in X.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples
                and n_features is the number of features.

        Returns:
            array (shape (n_samples,)): uplift
        """
        uplift = 2 * self.estimator.predict_proba(X)[:, 1] - 1
        return uplift


class ClassTransformationReg(BaseEstimator):
    """aka CATE-generating (Conditional Average Treatment Effect) Transformation of the Outcome.

    Redefine target variable, which indicates that treatment make some impact on target or
    did target is negative without treatment: ``Z = Y * (W - p)/(p * (1 - p))``,
    
    where ``Y`` - target vector, ``W`` - vector of binary communication flags, and ``p`` is a propensity score
    (the probabilty that each y_i is assigned to the treatment group.).

    Then, train a regressor on ``Z`` to predict uplift.

    Returns uplift predictions and optionally propensity predictions.

    The propensity score can be a scalar value (e.g. p = 0.5), which would mean that every subject has identical
    probability of being assigned to the treatment group.

    Alternatively, the propensity can be learned using a Classifier model.
    In this case, the model predicts the probability that a given subject would be assigned to the treatment group.

    Read more in the :ref:`User Guide <ClassTransformationReg>`.

    Args:
        estimator (estimator object implementing 'fit'): The object to use to fit the data.
        propensity_val (float): A constant propensity value, which assumes every subject has equal probability of assignment to the treatment group.
        propensity_estimator (estimator object with `predict_proba`): The object used to predict the propensity score if `propensity_val` is not given.


    Example::

        # import approach
        from sklift.models import ClassTransformationReg
        # import any estimator adheres to scikit-learn conventions
        from sklearn.linear_model import LinearRegression, LogisticRegression


        # define approach
        ct = ClassTransformationReg(estimator=LinearRegression(), propensity_estimator=LogisticRegression())
        # fit the model
        ct = ct.fit(X_train, y_train, treat_train)
        # predict uplift
        uplift_ct = ct.predict(X_val)

    References:
        Athey, Susan & Imbens, Guido & Ramachandra, Vikas. (2015).
        Machine Learning Methods for Estimating Heterogeneous Causal Effects.

    See Also:

        **Other approaches:**

        * :class:`.SoloModel`: Single model approach.
        * :class:`.TwoModels`: Double classifier approach.
        * :class:`.ClassTransformation`: Binary classifier transformation approach.
    """

    def __init__(self, estimator, propensity_val=None, propensity_estimator=None):

        if (propensity_val is None) and (propensity_estimator is None):
            raise ValueError('`propensity_val` and `propensity_estimator` cannot both be equal to `None`. Both arguments are currently null.')
        elif (propensity_val is not None) and (propensity_estimator is not None):
            raise ValueError('Exactly one of (`propensity_val`, `propensity_estimator`) must be None, and the other must be defined. Both arguments are currently non-null.')

        self.estimator = estimator
        self.propensity_val = propensity_val
        self.propensity_estimator = propensity_estimator

        self._type_of_target = None

    def fit(self, X, y, treatment, estimator_fit_params=None):
        """Fit the model according to the given training data.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples and
                n_features is the number of features.
            y (array-like, shape (n_samples,)): Target vector relative to X.
            treatment (array-like, shape (n_samples,)): Binary treatment vector relative to X.
            estimator_fit_params (dict, optional): Parameters to pass to the fit method of the estimator.

        Returns:
            object: self
        """

        check_consistent_length(X, y, treatment)
        check_is_binary(treatment)
        self._type_of_target = type_of_target(y)

        if self.propensity_val is not None:
            p = self.propensity_val

        elif self.propensity_estimator is not None:
            self.propensity_estimator.fit(X, treatment)
            p = self.propensity_estimator.predict_proba(X)[:, 1]

        y_mod = y * ((treatment - p) / (p * (1 - p)))

        if estimator_fit_params is None:
                estimator_fit_params = {}

        self.estimator.fit(X, y_mod, **estimator_fit_params)

        return self


    def predict_propensity(self, X):
        """Predict propensity values.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples
                and n_features is the number of features.

        Returns:
            array (shape (n_samples,)): propensity
        """

        if self.propensity_estimator is not None:
            return self.propensity_estimator.predict_proba(X)[:, 1]
        else:
            return self.propensity_val

    def predict(self, X):
        """Perform uplift on samples in X.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples
                and n_features is the number of features.

        Returns:
            array (shape (n_samples,)): uplift
        """

        uplift = self.estimator.predict(X)
        return uplift


class TwoModels(BaseEstimator):
    """aka naïve approach, or difference score method, or double classifier approach.

    Fit two separate models: on the treatment data and on the control data.

    Read more in the :ref:`User Guide <TwoModels>`.

    Args:
        estimator_trmnt (estimator object implementing 'fit'): The object to use to fit the treatment data.
        estimator_ctrl (estimator object implementing 'fit'): The object to use to fit the control data.
        method (string, 'vanilla', 'ddr_control' or 'ddr_treatment', default='vanilla'): Specifies the approach:

            * ``'vanilla'``:
                Two independent models;
            * ``'ddr_control'``:
                Dependent data representation (First train control estimator).
            * ``'ddr_treatment'``:
                Dependent data representation (First train treatment estimator).

    Attributes:
        trmnt_preds_ (array-like, shape (n_samples, )): Estimator predictions on samples when treatment.
        ctrl_preds_ (array-like, shape (n_samples, )): Estimator predictions on samples when control.

    Example::

        # import approach
        from sklift.models import TwoModels
        # import any estimator adheres to scikit-learn conventions
        from catboost import CatBoostClassifier


        estimator_trmnt = CatBoostClassifier(silent=True, thread_count=2, random_state=42)
        estimator_ctrl = CatBoostClassifier(silent=True, thread_count=2, random_state=42)

        # define approach
        tm_ctrl = TwoModels(
            estimator_trmnt=estimator_trmnt,
            estimator_ctrl=estimator_ctrl,
            method='ddr_control'
        )

        # fit the models
        tm_ctrl = tm_ctrl.fit(
            X_train, y_train, treat_train,
            estimator_trmnt_fit_params={'cat_features': cat_features},
            estimator_ctrl_fit_params={'cat_features': cat_features}
        )
        uplift_tm_ctrl = tm_ctrl.predict(X_val)  # predict uplift

    References
        Betlei, Artem & Diemert, Eustache & Amini, Massih-Reza. (2018).
        Uplift Prediction with Dependent Feature Representation in Imbalanced Treatment and Control Conditions:
        25th International Conference, ICONIP 2018, Siem Reap, Cambodia, December 13–16, 2018,
        Proceedings, Part V. 10.1007/978-3-030-04221-9_5.

        Zhao, Yan & Fang, Xiao & Simchi-Levi, David. (2017).
        Uplift Modeling with Multiple Treatments and General Response Types.
        10.1137/1.9781611974973.66.

    See Also:

        **Other approaches:**

        * :class:`.SoloModel`: Single model approach.
        * :class:`.ClassTransformation`: Class Variable Transformation approach.
        * :class:`.ClassTransformationReg`: Transformed Outcome approach.

        **Other:**

        * :func:`.plot_uplift_preds`: Plot histograms of treatment, control and uplift predictions.
    """

    def __init__(self, estimator_trmnt, estimator_ctrl, method='vanilla'):
        self.estimator_trmnt = estimator_trmnt
        self.estimator_ctrl = estimator_ctrl
        self.method = method
        self.trmnt_preds_ = None
        self.ctrl_preds_ = None
        self._type_of_target = None

        all_methods = ['vanilla', 'ddr_control', 'ddr_treatment']
        if method not in all_methods:
            raise ValueError("Two models approach supports only methods in %s, got"
                             " %s." % (all_methods, method))

        if estimator_trmnt is estimator_ctrl:
            raise ValueError('Control and Treatment estimators should be different objects.')

    def fit(self, X, y, treatment, estimator_trmnt_fit_params=None, estimator_ctrl_fit_params=None):
        """Fit the model according to the given training data.

        For each test example calculate predictions on new set twice: by the first and second models.
        After that calculate uplift as a delta between these predictions.

        Return delta of predictions for each example.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number
                of samples and n_features is the number of features.
            y (array-like, shape (n_samples,)): Target vector relative to X.
            treatment (array-like, shape (n_samples,)): Binary treatment vector relative to X.
            estimator_trmnt_fit_params (dict, optional): Parameters to pass to the fit method
                of the treatment estimator.
            estimator_ctrl_fit_params (dict, optional): Parameters to pass to the fit method
                of the control estimator.

        Returns:
            object: self
        """

        check_consistent_length(X, y, treatment)
        check_is_binary(treatment)
        self._type_of_target = type_of_target(y)

        y_copy = y.copy()
        treatment_copy = treatment.copy()

        if (isinstance(X, pd.Series) or isinstance(X, pd.DataFrame)) and isinstance(y_copy, pd.Series) and not X.index.equals(y_copy.index):
            y_copy.index = X.index
            warnings.warn("Target indexes do not match data indexes, re-indexing has been performed")
        if (isinstance(X, pd.Series) or isinstance(X, pd.DataFrame)) and isinstance(treatment_copy, pd.Series) and not X.index.equals(treatment_copy.index):
            treatment_copy.index = X.index
            warnings.warn("Treatment indexes do not match data indexes, re-indexing has been performed")

        X_ctrl, y_ctrl = X[treatment_copy == 0], y_copy[treatment_copy == 0]
        X_trmnt, y_trmnt = X[treatment_copy == 1], y_copy[treatment_copy == 1]

        if estimator_trmnt_fit_params is None:
            estimator_trmnt_fit_params = {}
        if estimator_ctrl_fit_params is None:
            estimator_ctrl_fit_params = {}

        if self.method == 'vanilla':
            self.estimator_ctrl.fit(
                X_ctrl, y_ctrl, **estimator_ctrl_fit_params
            )
            self.estimator_trmnt.fit(
                X_trmnt, y_trmnt, **estimator_trmnt_fit_params
            )

        if self.method == 'ddr_control':
            self.estimator_ctrl.fit(
                X_ctrl, y_ctrl, **estimator_ctrl_fit_params
            )
            if self._type_of_target == 'binary':
                ddr_control = self.estimator_ctrl.predict_proba(X_trmnt)[:, 1]
            else:
                ddr_control = self.estimator_ctrl.predict(X_trmnt)

            if isinstance(X_trmnt, np.ndarray):
                X_trmnt_mod = np.column_stack((X_trmnt, ddr_control))
            elif isinstance(X_trmnt, pd.DataFrame):
                X_trmnt_mod = X_trmnt.assign(ddr_control=ddr_control)
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame, got %s" % type(X_trmnt))

            self.estimator_trmnt.fit(
                X_trmnt_mod, y_trmnt, **estimator_trmnt_fit_params
            )

        if self.method == 'ddr_treatment':
            self.estimator_trmnt.fit(
                X_trmnt, y_trmnt, **estimator_trmnt_fit_params
            )
            if self._type_of_target == 'binary':
                ddr_treatment = self.estimator_trmnt.predict_proba(X_ctrl)[:, 1]
            else:
                ddr_treatment = self.estimator_trmnt.predict(X_ctrl)

            if isinstance(X_ctrl, np.ndarray):
                X_ctrl_mod = np.column_stack((X_ctrl, ddr_treatment))
            elif isinstance(X_trmnt, pd.DataFrame):
                X_ctrl_mod = X_ctrl.assign(ddr_treatment=ddr_treatment)
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame, got %s" % type(X_ctrl))

            self.estimator_ctrl.fit(
                X_ctrl_mod, y_ctrl, **estimator_ctrl_fit_params
            )

        return self

    def predict(self, X):
        """Perform uplift on samples in X.

        Args:
            X (array-like, shape (n_samples, n_features)): Training vector, where n_samples is the number of samples
                and n_features is the number of features.

        Returns:
            array (shape (n_samples,)): uplift
        """

        if self.method == 'ddr_control':
            if self._type_of_target == 'binary':
                self.ctrl_preds_ = self.estimator_ctrl.predict_proba(X)[:, 1]
            else:
                self.ctrl_preds_ = self.estimator_ctrl.predict(X)

            if isinstance(X, np.ndarray):
                X_mod = np.column_stack((X, self.ctrl_preds_))
            elif isinstance(X, pd.DataFrame):
                X_mod = X.assign(ddr_control=self.ctrl_preds_)
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame, got %s" % type(X))

            if self._type_of_target == 'binary':
                self.trmnt_preds_ = self.estimator_trmnt.predict_proba(X_mod)[:, 1]
            else:
                self.trmnt_preds_ = self.estimator_trmnt.predict(X_mod)

        elif self.method == 'ddr_treatment':
            if self._type_of_target == 'binary':
                self.trmnt_preds_ = self.estimator_trmnt.predict_proba(X)[:, 1]
            else:
                self.trmnt_preds_ = self.estimator_trmnt.predict(X)

            if isinstance(X, np.ndarray):
                X_mod = np.column_stack((X, self.trmnt_preds_))
            elif isinstance(X, pd.DataFrame):
                X_mod = X.assign(ddr_treatment=self.trmnt_preds_)
            else:
                raise TypeError("Expected numpy.ndarray or pandas.DataFrame, got %s" % type(X))

            if self._type_of_target == 'binary':
                self.ctrl_preds_ = self.estimator_ctrl.predict_proba(X_mod)[:, 1]
            else:
                self.ctrl_preds_ = self.estimator_ctrl.predict(X_mod)

        else:
            if self._type_of_target == 'binary':
                self.ctrl_preds_ = self.estimator_ctrl.predict_proba(X)[:, 1]
                self.trmnt_preds_ = self.estimator_trmnt.predict_proba(X)[:, 1]
            else:
                self.ctrl_preds_ = self.estimator_ctrl.predict(X)
                self.trmnt_preds_ = self.estimator_trmnt.predict(X)

        uplift = self.trmnt_preds_ - self.ctrl_preds_

        return uplift

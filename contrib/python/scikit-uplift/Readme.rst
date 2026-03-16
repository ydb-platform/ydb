.. -*- mode: rst -*-

|Python3|_ |PyPi|_ |Docs|_ |License|_

.. |Python3| image:: https://img.shields.io/badge/python-3-blue.svg
.. _Python3: https://badge.fury.io/py/scikit-uplift

.. |PyPi| image:: https://badge.fury.io/py/scikit-uplift.svg
.. _PyPi: https://badge.fury.io/py/scikit-uplift

.. |Docs| image:: https://readthedocs.org/projects/scikit-uplift/badge/?version=latest
.. _Docs: https://www.uplift-modeling.com/en/latest/

.. |License| image:: https://img.shields.io/badge/license-MIT-green
.. _License: https://github.com/maks-sh/scikit-uplift/blob/master/LICENSE

.. |Open In Colab1| image:: https://colab.research.google.com/assets/colab-badge.svg
.. _Open In Colab1: https://colab.research.google.com/github/maks-sh/scikit-uplift/blob/master/notebooks/RetailHero_EN.ipynb

.. |Open In Colab2| image:: https://colab.research.google.com/assets/colab-badge.svg
.. _Open In Colab2: https://colab.research.google.com/github/maks-sh/scikit-uplift/blob/master/notebooks/RetailHero.ipynb

.. |Open In Colab3| image:: https://colab.research.google.com/assets/colab-badge.svg
.. _Open In Colab3: https://colab.research.google.com/github/maks-sh/scikit-uplift/blob/master/notebooks/pipeline_usage_EN.ipynb

.. |Open In Colab4| image:: https://colab.research.google.com/assets/colab-badge.svg
.. _Open In Colab4: https://colab.research.google.com/github/maks-sh/scikit-uplift/blob/master/notebooks/pipeline_usage_RU.ipynb

.. _uplift-modeling.com: https://www.uplift-modeling.com/en/latest/

.. image:: https://raw.githubusercontent.com/maks-sh/scikit-uplift/dev/docs/_static/sklift-github-logo.png
    :align: center
    :alt: scikit-uplift: uplift modeling in scikit-learn style in python

.. |Contribs| image:: https://contrib.rocks/image?repo=maks-sh/scikit-uplift
   :target: https://github.com/maks-sh/scikit-uplift/graphs/contributors
   :alt: Contributors

scikit-uplift
===============

**scikit-uplift (sklift)** is an uplift modeling python package that provides fast sklearn-style models implementation, evaluation metrics and visualization tools.

Uplift modeling estimates a causal effect of treatment and uses it to effectively target customers that are most likely to respond to a marketing campaign.

**Use cases for uplift modeling:**

* Target customers in the marketing campaign. Quite useful in promotion of some popular product where there is a big part of customers who make a target action by themself without any influence. By modeling uplift you can find customers who are likely to make the target action (for instance, install an app) only when treated (for instance, received a push).

* Combine a churn model and an uplift model to offer some bonus to a group of customers who are likely to churn.

* Select a tiny group of customers in the campaign where a price per customer is high.

Read more about uplift modeling problem in `User Guide <https://www.uplift-modeling.com/en/latest/user_guide/index.html>`__.

Articles in russian on habr.com: `Part 1 <https://habr.com/ru/company/ru_mts/blog/485980/>`__ ,
`Part 2 <https://habr.com/ru/company/ru_mts/blog/485976/>`__
and `Part 3 <https://habr.com/ru/company/ru_mts/blog/538934/>`__.

Why sklift
-------------

- Сomfortable and intuitive *scikit-learn*-like API;

- More uplift metrics than you have ever seen in one place! Include brilliants like  *Area Under Uplift Curve* (AUUC) or *Area Under Qini Curve* (Qini coefficient) with ideal cases;

- Supporting any estimator compatible with scikit-learn (e.g. Xgboost, LightGBM, Catboost, etc.);

- All approaches can be used in the ``sklearn.pipeline``. See the example of usage on `the Tutorials page <https://www.uplift-modeling.com/en/latest/tutorials.html>`__;

- Also metrics are compatible with the classes from ``sklearn.model_selection``. See the example of usage on `the Tutorials page <https://www.uplift-modeling.com/en/latest/tutorials.html>`__;

- Almost all implemented approaches solve classification and regression problems;

- Nice and useful viz for analysing a performance model.

Installation
-------------

**Install** the package by the following command from PyPI:

.. code-block:: bash

    pip install scikit-uplift

Or install from source:

.. code-block:: bash

    git clone https://github.com/maks-sh/scikit-uplift.git
    cd scikit-uplift
    python setup.py install

Documentation
--------------

The full documentation is available at `uplift-modeling.com`_.

Or you can build the documentation locally using `Sphinx <http://sphinx-doc.org/>`_ 1.4 or later:

.. code-block:: bash

    cd docs
    pip install -r requirements.txt
    make html

And if you now point your browser to ``_build/html/index.html``, you should see a documentation site.

Quick Start
-----------

See the **RetailHero tutorial notebook** (`EN <https://nbviewer.jupyter.org/github/maks-sh/scikit-uplift/blob/master/notebooks/RetailHero_EN.ipynb>`__ |Open In Colab1|_, `RU <https://nbviewer.jupyter.org/github/maks-sh/scikit-uplift/blob/master/notebooks/RetailHero.ipynb>`__ |Open In Colab2|_) for details.

**Train and predict uplift model**

Use the intuitive python API to train uplift models with `sklift.models  <https://www.uplift-modeling.com/en/latest/api/models/index.html>`__.

.. code-block:: python

    # import approaches
    from sklift.models import SoloModel, ClassTransformation
    # import any estimator adheres to scikit-learn conventions.
    from lightgbm import LGBMClassifier

    # define models
    estimator = LGBMClassifier(n_estimators=10)

    # define metamodel
    slearner = SoloModel(estimator=estimator)

    # fit model
    slearner.fit(
        X=X_tr,
        y=y_tr,
        treatment=trmnt_tr,
    )

    # predict uplift
    uplift_slearner = slearner.predict(X_val)

**Evaluate your uplift model**

Uplift model evaluation metrics are available in `sklift.metrics  <https://www.uplift-modeling.com/en/latest/api/metrics/index.html>`__.

.. code-block:: python

    # import metrics to evaluate your model
    from sklift.metrics import (
        uplift_at_k, uplift_auc_score, qini_auc_score, weighted_average_uplift
    )


    # Uplift@30%
    uplift_at_k = uplift_at_k(y_true=y_val, uplift=uplift_slearner,
                              treatment=trmnt_val,
                              strategy='overall', k=0.3)

    # Area Under Qini Curve
    qini_coef = qini_auc_score(y_true=y_val, uplift=uplift_slearner,
                               treatment=trmnt_val)

    # Area Under Uplift Curve
    uplift_auc = uplift_auc_score(y_true=y_val, uplift=uplift_slearner,
                                  treatment=trmnt_val)

    # Weighted average uplift
    wau = weighted_average_uplift(y_true=y_val, uplift=uplift_slearner,
                                  treatment=trmnt_val)

**Vizualize the results**

Visualize performance metrics with `sklift.viz  <https://www.uplift-modeling.com/en/latest/api/viz/index.html>`__.

.. code-block:: python

    from sklift.viz import plot_qini_curve
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(1, 1)
    ax.set_title('Qini curves')

    plot_qini_curve(
        y_test, uplift_slearner, trmnt_test,
        perfect=True, name='Slearner', ax=ax
    );

    plot_qini_curve(
        y_test, uplift_revert, trmnt_test,
        perfect=False, name='Revert label', ax=ax
    );

.. image:: docs/_static/images/quick_start_qini.png
    :width: 514px
    :height: 400px
    :alt: Example of some models qini curves, perfect qini curve and random qini curve

Development
-----------

We welcome new contributors of all experience levels.

- Please see our `Contributing Guide <https://www.uplift-modeling.com/en/latest/contributing.html>`_ for more details.
- By participating in this project, you agree to abide by its `Code of Conduct <https://github.com/maks-sh/scikit-uplift/blob/master/.github/CODE_OF_CONDUCT.md>`__.

Thanks to all our contributors!

|Contribs|

If you have any questions, please contact us at team@uplift-modeling.com

Important links
~~~~~~~~~~~~~~~

- Official source code repo: https://github.com/maks-sh/scikit-uplift/
- Issue tracker: https://github.com/maks-sh/scikit-uplift/issues
- Documentation: https://www.uplift-modeling.com/en/latest/
- User Guide: https://www.uplift-modeling.com/en/latest/user_guide/index.html
- Contributing guide: https://www.uplift-modeling.com/en/latest/contributing.html
- Release History: https://www.uplift-modeling.com/en/latest/changelog.html

===============

Papers and materials
---------------------
1. Gutierrez, P., & Gérardy, J. Y.
	Causal Inference and Uplift Modelling: A Review of the Literature.
	In International Conference on Predictive Applications and APIs (pp. 1-13).

2. Artem Betlei, Criteo Research; Eustache Diemert, Criteo Research; Massih-Reza Amini, Univ. Grenoble Alpes
	Dependent and Shared Data Representations improve Uplift Prediction in Imbalanced Treatment Conditions
	FAIM'18 Workshop on CausalML.

3. Eustache Diemert, Artem Betlei, Christophe Renaudin, and Massih-Reza Amini. 2018.
    A Large Scale Benchmark for Uplift Modeling.
    In Proceedings of AdKDD & TargetAd (ADKDD’18). ACM, New York, NY, USA, 6 pages.

4. Athey, Susan, and Imbens, Guido. 2015.
    Machine learning methods for estimating heterogeneous causal effects.
    Preprint, arXiv:1504.01132. Google Scholar.

5. Oscar Mesalles Naranjo. 2012.
    Testing a New Metric for Uplift Models.
    Dissertation Presented for the Degree of MSc in Statistics and Operational Research.

6. Kane, K., V. S. Y. Lo, and J. Zheng. 2014.
    Mining for the Truly Responsive Customers and Prospects Using True-Lift Modeling:
    Comparison of New and Existing Methods.
    Journal of Marketing Analytics 2 (4): 218–238.

7. Maciej Jaskowski and Szymon Jaroszewicz.
    Uplift modeling for clinical trial data.
    ICML Workshop on Clinical Data Analysis, 2012.

8. Lo, Victor. 2002.
    The True Lift Model - A Novel Data Mining Approach to Response Modeling in Database Marketing.
    SIGKDD Explorations. 4. 78-86.

9. Zhao, Yan & Fang, Xiao & Simchi-Levi, David. 2017.
    Uplift Modeling with Multiple Treatments and General Response Types. 10.1137/1.9781611974973.66.

10. Nicholas J Radcliffe. 2007.
	Using control groups to target on predicted lift: Building and assessing uplift model. Direct Marketing Analytics Journal, (3):14–21, 2007.

11. Devriendt, F., Guns, T., & Verbeke, W. 2020.
	Learning to rank for uplift modeling. ArXiv, abs/2002.05897.

===============

Tags
~~~~~~~~~~~~~~~
**EN**: uplift modeling, uplift modelling, causal inference, causal effect, causality, individual treatment effect, true lift, net lift, incremental modeling

**RU**: аплифт моделирование, Uplift модель

**ZH**: uplift增量建模, 因果推断, 因果效应, 因果关系, 个体干预因果效应, 真实增量, 净增量, 增量建模


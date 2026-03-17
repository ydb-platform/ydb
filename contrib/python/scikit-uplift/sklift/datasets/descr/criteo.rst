Criteo Uplift Modeling Dataset
================================
This is a copy of `Criteo AI Lab Uplift Prediction dataset <https://ailab.criteo.com/criteo-uplift-prediction-dataset/>`_.

Data description
################

This dataset is constructed by assembling data resulting from several incrementality tests, a particular randomized trial procedure where a random part of the population is prevented from being targeted by advertising.


Fields
################

Here is a detailed description of the fields (they are comma-separated in the file):

* **f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11**: feature values (dense, float)
* **treatment**: treatment group. Flag if a company participates in the RTB auction for a particular user (binary: 1 = treated, 0 = control)
* **exposure**: treatment effect, whether the user has been effectively exposed. Flag if a company wins in the RTB auction for the user (binary)
* **conversion**: whether a conversion occured for this user (binary, label)
* **visit**: whether a visit occured for this user (binary, label)


Key figures
################
* Format: CSV
* Size: 297M (compressed) 3,2GB (uncompressed)
* Rows: 13,979,592
* Response Ratio:

    * Average `Visit` Rate: .046992
    * Average `Conversion` Rate: .00292

* Treatment Ratio: .85



This dataset is released along with the paper:
“*A Large Scale Benchmark for Uplift Modeling*"
Eustache Diemert, Artem Betlei, Christophe Renaudin; (Criteo AI Lab), Massih-Reza Amini (LIG, Grenoble INP)
This work was published in: `AdKDD 2018  <https://adkdd-targetad.wixsite.com/2018/>`_ Workshop, in conjunction with KDD 2018.

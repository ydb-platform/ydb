# paramz

[![pypi](https://badge.fury.io/py/paramz.svg)](https://pypi.python.org/pypi/paramz)
[![build](https://travis-ci.org/sods/paramz.svg?branch=master)](https://travis-ci.org/sods/paramz)
[![codecov](https://codecov.io/gh/sods/paramz/branch/master/graph/badge.svg)](https://codecov.io/gh/sods/paramz)

#### Coverage
![codecov.io](https://codecov.io/gh/sods/paramz/branch/master/graphs/commits.svg)

Parameterization Framework for parameterized model creation and handling.
This is a lightweight framework for using parameterized models.

See examples model in 'paramz.examples.<tab>'

### Features:

 - Easy model creation with parameters
 - Fast optimized access of parameters for optimization routines
 - Memory efficient storage of parameters (only one copy in memory)
 - Renaming of parameters
 - Intuitive printing of models and parameters
 - Gradient saving directly inside parameters
 - Gradient checking of parameters
 - Optimization of parameters
 - Jupyter notebook integration
 - Efficient storage of models, for reloading
 - Efficient caching included

## Installation
You can install this package via pip

  pip install paramz

There is regular update for this package, so make sure to keep up to date
(Rerunning the install above will update the package and dependencies).

## Supported Platforms:

Python 2.7, 3.5 and higher

## Saving models in a consistent way across versions:

As pickle is inconsistent across python versions and heavily dependent on class structure, it behaves inconsistent across versions. 
Pickling as meant to serialize models within the same environment, and not to store models on disk to be used later on.

To save a model it is best to save the m.param_array of it to disk (using numpy’s np.save).
Additionally, you save the script, which creates the model. 
In this script you can create the model using initialize=False as a keyword argument and with the data loaded as normal. 
You then set the model parameters by setting m.param_array[:] = loaded_params as the previously saved parameters. 
Then you initialize the model by m.initialize_parameter(), which will make the model usable. 
Be aware that up to this point the model is in an inconsistent state and cannot be used to produce any results.

```python
# let X, Y be data loaded above
# Model creation:
m = paramz.examples.RidgeRegression(X, Y)
m.optimize()
# 1: Saving a model:
np.save('model_save.npy', m.param_array)
# 2: loading a model
# Model creation, without initialization:
m_load = paramz.examples.RidgeRegression(X, Y,initialize=False)
m_load.update_model(False) # do not call the underlying expensive algebra on load
m_load.initialize_parameter() # Initialize the parameters (connect the parameters up)
m_load[:] = np.load('model_save.npy') # Load the parameters
m_load.update_model(True) # Call the algebra only once
print(m_load)
```

## Running unit tests:

Ensure nose is installed via pip:

    pip install nose

Run nosetests from the root directory of the repository:

    nosetests -v paramz/tests

or using setuptools

    python setup.py test

## Developer Documentation:

- [HTML version](http://paramz.rtfd.io)
- [PDF version](https://media.readthedocs.org/pdf/paramz/latest/paramz.pdf)

### Compiling documentation:

The documentation is stored in doc/ and is compiled with the Sphinx Python documentation generator, and is written in the reStructuredText format.

The Sphinx documentation is available here: http://sphinx-doc.org/latest/contents.html

**Installing dependencies:**

To compile the documentation, first ensure that Sphinx is installed. On Debian-based systems, this can be achieved as follows:

    sudo apt-get install python-pip
    sudo pip install sphinx

**Compiling documentation:**

The documentation can be compiled as follows:

    cd doc
    sphinx-apidoc -o source/ ../GPy/
    make html

The HTML files are then stored in doc/build/html

## Funding Acknowledgements

Current support for the paramz software is coming through the following projects.

* [EU FP7-PEOPLE Project Ref 316861](http://staffwww.dcs.shef.ac.uk/people/N.Lawrence/projects/mlpm/) "MLPM2012: Machine Learning for Personalized Medicine"


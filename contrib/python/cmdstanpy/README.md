# CmdStanPy

[![codecov](https://codecov.io/gh/stan-dev/cmdstanpy/branch/master/graph/badge.svg)](https://codecov.io/gh/stan-dev/cmdstanpy)


CmdStanPy is a lightweight pure-Python interface to CmdStan which provides access to the Stan compiler and all inference algorithms.  It supports both development and production workflows. Because model development and testing may require many iterations, the defaults favor development mode and therefore output files are stored on a temporary filesystem. Non-default options allow all aspects of a run to be specified so that scripts can be used to distributed analysis jobs across nodes and machines.

CmdStanPy is distributed via PyPi: https://pypi.org/project/cmdstanpy/

or Conda Forge: https://anaconda.org/conda-forge/cmdstanpy

### Goals

- Clean interface to Stan services so that CmdStanPy can keep up with Stan releases.

- Provide access to all CmdStan inference methods.

- Easy to install,
  + minimal Python library dependencies: numpy, pandas
  + Python code doesn't interface directly with c++, only calls compiled executables

- Modular - CmdStanPy produces a MCMC sample (or point estimate) from the posterior; other packages do analysis and visualization.

- Low memory overhead - by default, minimal memory used above that required by CmdStanPy; objects run CmdStan programs and track CmdStan input and output files.


### Source Repository

CmdStanPy and CmdStan are available from GitHub: https://github.com/stan-dev/cmdstanpy and https://github.com/stan-dev/cmdstan


### Docs

The latest release documentation is hosted on  https://mc-stan.org/cmdstanpy, older release versions are available from readthedocs:  https://cmdstanpy.readthedocs.io

### Licensing

The CmdStanPy, CmdStan, and the core Stan C++ code are licensed under new BSD.

### Example

```python
import os
from cmdstanpy import cmdstan_path, CmdStanModel

# specify locations of Stan program file and data
stan_file = os.path.join(cmdstan_path(), 'examples', 'bernoulli', 'bernoulli.stan')
data_file = os.path.join(cmdstan_path(), 'examples', 'bernoulli', 'bernoulli.data.json')

# instantiate a model; compiles the Stan program by default
model = CmdStanModel(stan_file=stan_file)

# obtain a posterior sample from the model conditioned on the data
fit = model.sample(chains=4, data=data_file)

# summarize the results (wraps CmdStan `bin/stansummary`):
fit.summary()
```

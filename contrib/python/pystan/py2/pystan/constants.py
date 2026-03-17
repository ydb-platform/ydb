MAX_UINT = 2**31 - 1  # conservative choice, maximum unsigned int on 32-bit Python 2.7

try:
    from enum import Enum  # Python 3.4
except ImportError:
    from pystan.external.enum import Enum

sampling_algo_t = Enum('sampling_algo_t', 'NUTS HMC Metropolis Fixed_param', module=__name__)
variational_algo_t = Enum('variational_algo_t', 'MEANFIELD FULLRANK', module=__name__)


class optim_algo_t(Enum):
    Newton = 1
    BFGS = 3
    LBFGS = 4

sampling_metric_t = Enum('sampling_metric_t', 'UNIT_E DIAG_E DENSE_E', module=__name__)
stan_args_method_t = Enum('stan_args_method_t', 'SAMPLING OPTIM TEST_GRADIENT VARIATIONAL', module=__name__)

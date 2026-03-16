#===============================================================================
# Copyright (c) 2012 - 2014, GPy authors (see AUTHORS.txt).
# Copyright (c) 2015, Max Zwiessele
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of paramax nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================


import numpy as np
from .domains import _POSITIVE,_NEGATIVE, _BOUNDED
import weakref, logging

import sys

_log_lim_val = np.log(np.finfo(np.float64).max)
_exp_lim_val = np.finfo(np.float64).max
_lim_val = 36.0
epsilon = np.finfo(np.float64).resolution

#===============================================================================
# Fixing constants
__fixed__ = "fixed"
FIXED = False
UNFIXED = True
#===============================================================================
logger = logging.getLogger(__name__)

class Transformation(object):
    domain = None
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance or cls._instance.__class__ is not cls:
            cls._instance = super(Transformation, cls).__new__(cls, *args, **kwargs)
        return cls._instance
    def f(self, opt_param):
        raise NotImplementedError
    def finv(self, model_param):
        raise NotImplementedError
    def log_jacobian(self, model_param):
        """
        compute the log of the jacobian of f, evaluated at f(x)= model_param
        """
        logger.warning("log Jacobian for transformation {} not implemented, approximating by 0.".format(self.__class__))
        return 0.
    def log_jacobian_grad(self, model_param):
        """
        compute the drivative of the log of the jacobian of f, evaluated at f(x)= model_param
        """
        logger.warning("gradient of log Jacobian for transformation {} not implemented, approximating by 0.".format(self.__class__))
        return 0.
    def gradfactor(self, model_param, dL_dmodel_param):
        """ df(opt_param)_dopt_param evaluated at self.f(opt_param)=model_param, times the gradient dL_dmodel_param,

        i.e.:
        define

        .. math::

            \frac{\frac{\partial L}{\partial f}\left(\left.\partial f(x)}{\partial x}\right|_{x=f^{-1}(f)\right)}
        """
        raise NotImplementedError
    def gradfactor_non_natural(self, model_param, dL_dmodel_param):
        return self.gradfactor(model_param, dL_dmodel_param)
    def initialize(self, f):
        """ produce a sensible initial value for f(x)"""
        raise NotImplementedError
    def plot(self, xlabel=r'transformed $\theta$', ylabel=r'$\theta$', axes=None, *args,**kw):
        assert "matplotlib" in sys.modules, "matplotlib package has not been imported."
        import matplotlib.pyplot as plt
        x = np.linspace(-8,8)
        plt.plot(x, self.f(x), *args, ax=axes, **kw)
        axes = plt.gca()
        axes.set_xlabel(xlabel)
        axes.set_ylabel(ylabel)
    def __str__(self):
        raise NotImplementedError
    def __repr__(self):
        return self.__class__.__name__

class Logexp(Transformation):
    domain = _POSITIVE
    def f(self, x):
        return np.where(x>_lim_val, x, np.log1p(np.exp(np.clip(x, -_log_lim_val, _lim_val)))) #+ epsilon
        #raises overflow warning: return np.where(x>_lim_val, x, np.log(1. + np.exp(x)))
    def finv(self, f):
        return np.where(f>_lim_val, f, np.log(np.expm1(f)))
    def gradfactor(self, f, df):
        return df*np.where(f>_lim_val, 1.,  - np.expm1(-f))
    def initialize(self, f):
        if np.any(f < 0.):
            logger.info("Warning: changing parameters to satisfy constraints")
        return np.abs(f)
    def log_jacobian(self, model_param):
        return np.where(model_param>_lim_val, model_param, np.log(np.expm1(model_param))) - model_param
    def log_jacobian_grad(self, model_param):
        return 1./(np.expm1(model_param))
    def __str__(self):
        return '+ve'

class Exponent(Transformation):
    domain = _POSITIVE
    def f(self, x):
        return np.where(x<_lim_val, np.where(x>-_lim_val, np.exp(x), np.exp(-_lim_val)), np.exp(_lim_val))
    def finv(self, x):
        return np.log(x)
    def gradfactor(self, f, df):
        return np.einsum('i,i->i', df, f)
    def initialize(self, f):
        if np.any(f < 0.):
            logger.info("Warning: changing parameters to satisfy constraints")
        return np.abs(f)
    def log_jacobian(self, model_param):
        return np.log(model_param)
    def log_jacobian_grad(self, model_param):
        return 1./model_param
    def __str__(self):
        return '+ve'

class NegativeLogexp(Transformation):
    domain = _NEGATIVE
    logexp = Logexp()
    def f(self, x):
        return -self.logexp.f(x)  # np.log(1. + np.exp(x))
    def finv(self, f):
        return self.logexp.finv(-f)  # np.log(np.exp(-f) - 1.)
    def gradfactor(self, f, df):
        return self.logexp.gradfactor(-f, -df)
    def initialize(self, f):
        return -self.logexp.initialize(-f)  # np.abs(f)
    def __str__(self):
        return '-ve'

class NegativeExponent(Exponent):
    domain = _NEGATIVE
    def f(self, x):
        return -Exponent.f(self, x)
    def finv(self, f):
        return -Exponent.finv(self, -f)
    def gradfactor(self, f, df):
        return -Exponent.gradfactor(self, f, df)
    def initialize(self, f):
        return -Exponent.initialize(self, f) #np.abs(f)
    def __str__(self):
        return '-ve'

class Square(Transformation):
    domain = _POSITIVE
    def f(self, x):
        return x ** 2
    def finv(self, x):
        return np.sqrt(x)
    def gradfactor(self, f, df):
        return np.einsum('i,i->i', df, 2 * np.sqrt(f))
    def initialize(self, f):
        return np.abs(f)
    def __str__(self):
        return '+sq'

class Logistic(Transformation):
    domain = _BOUNDED
    _instances = []
    def __new__(cls, lower=1e-6, upper=1e-6, *args, **kwargs):
        if cls._instances:
            cls._instances[:] = [instance for instance in cls._instances if instance()]
            for instance in cls._instances:
                if instance().lower == lower and instance().upper == upper:
                    return instance()
        newfunc = super(Transformation, cls).__new__
        if newfunc is object.__new__:
            o = newfunc(cls)
        else:
            o = newfunc(cls, lower, upper, *args, **kwargs)
        cls._instances.append(weakref.ref(o))
        return cls._instances[-1]()
    def __init__(self, lower, upper):
        assert lower < upper
        self.lower, self.upper = float(lower), float(upper)
        self.difference = self.upper - self.lower
    def f(self, x):
        if (x<-300.).any():
            x = x.copy()
            x[x<-300.] = -300.
        return self.lower + self.difference / (1. + np.exp(-x))
    def finv(self, f):
        return np.log(np.clip(f - self.lower, 1e-10, np.inf) / np.clip(self.upper - f, 1e-10, np.inf))
    def gradfactor(self, f, df):
        return np.einsum('i,i->i', df, (f - self.lower) * (self.upper - f) / self.difference)
    def initialize(self, f):
        if np.any(np.logical_or(f < self.lower, f > self.upper)):
            logger.info("Warning: changing parameters to satisfy constraints")
        #return np.where(np.logical_or(f < self.lower, f > self.upper), self.f(f * 0.), f)
        #FIXME: Max, zeros_like right?
        return np.where(np.logical_or(f < self.lower, f > self.upper), self.f(np.zeros_like(f)), f)
    def __str__(self):
        return '{},{}'.format(self.lower, self.upper)



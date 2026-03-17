# Copyright (c) 2012-2014, GPy authors (see AUTHORS.txt).
# Licensed under the BSD 3-clause license (see LICENSE.txt)

import datetime as dt
from scipy import optimize
from warnings import warn

#try:
#    import rasmussens_minimize as rasm
#    rasm_available = True
#except ImportError:
#    rasm_available = False
from .scg import SCG

class Optimizer(object):
    """
    Superclass for all the optimizers.

    :param x_init: initial set of parameters
    :param f_fp: function that returns the function AND the gradients at the same time
    :param f: function to optimize
    :param fp: gradients
    :param messages: print messages from the optimizer?
    :type messages: (True | False)
    :param max_f_eval: maximum number of function evaluations

    :rtype: optimizer object.

    """
    def __init__(self, messages=False, max_f_eval=1e4, max_iters=1e3,
                 ftol=None, gtol=None, xtol=None, bfgs_factor=None):
        self.opt_name = None
        #x_init = x_init
        # Turning messages off and using internal structure for print outs:
        self.messages = False #messages
        self.f_opt = None
        self.x_opt = None
        self.funct_eval = None
        self.status = None
        self.max_f_eval = int(max_iters)
        self.max_iters = int(max_iters)
        self.bfgs_factor = bfgs_factor
        self.trace = None
        self.time = "Not available"
        self.xtol = xtol
        self.gtol = gtol
        self.ftol = ftol

    def run(self, x_init, **kwargs):
        start = dt.datetime.now()
        self.opt(x_init, **kwargs)
        end = dt.datetime.now()
        self.time = str(end - start)

    def opt(self, x_init, f_fp=None, f=None, fp=None):
        raise NotImplementedError("this needs to be implemented to use the optimizer class")

    def __str__(self):
        diagnostics = "Optimizer: \t\t\t\t %s\n" % self.opt_name
        diagnostics += "f(x_opt): \t\t\t\t %.3f\n" % self.f_opt
        diagnostics += "Number of function evaluations: \t %d\n" % self.funct_eval
        diagnostics += "Optimization status: \t\t\t %s\n" % self.status
        diagnostics += "Time elapsed: \t\t\t\t %s\n" % self.time
        return diagnostics

    def __getstate__(self):
        return self.__dict__


class opt_tnc(Optimizer):
    def __init__(self, *args, **kwargs):
        Optimizer.__init__(self, *args, **kwargs)
        self.opt_name = "TNC (Scipy implementation)"

    def opt(self, x_init, f_fp=None, f=None, fp=None):
        """
        Run the TNC optimizer

        """
        tnc_rcstrings = ['Local minimum', 'Converged', 'XConverged', 'Maximum number of f evaluations reached',
             'Line search failed', 'Function is constant']

        assert f_fp != None, "TNC requires f_fp"

        opt_dict = {}
        if self.xtol is not None:
            opt_dict['xtol'] = self.xtol
        if self.ftol is not None:
            opt_dict['ftol'] = self.ftol
        if self.gtol is not None:
            opt_dict['pgtol'] = self.gtol

        opt_result = optimize.fmin_tnc(f_fp, x_init, messages=self.messages,
                       maxfun=self.max_f_eval, **opt_dict)
        self.x_opt = opt_result[0]
        self.f_opt = f_fp(self.x_opt)[0]
        self.funct_eval = opt_result[1]
        self.status = tnc_rcstrings[opt_result[2]]

class opt_lbfgsb(Optimizer):
    def __init__(self, *args, **kwargs):
        Optimizer.__init__(self, *args, **kwargs)
        self.opt_name = "L-BFGS-B (Scipy implementation)"

    def opt(self, x_init, f_fp=None, f=None, fp=None):
        """
        Run the optimizer

        """
        rcstrings = ['Converged', 'Maximum number of f evaluations reached', 'Error']

        assert f_fp != None, "BFGS requires f_fp"

        opt_dict = {}
        if self.xtol is not None:
            print("WARNING: l-bfgs-b doesn't have an xtol arg, so I'm going to ignore it")
        if self.ftol is not None:
            print("WARNING: l-bfgs-b doesn't have an ftol arg, so I'm going to ignore it")
        if self.gtol is not None:
            opt_dict['pgtol'] = self.gtol
        if self.bfgs_factor is not None:
            opt_dict['factr'] = self.bfgs_factor

        opt_result = optimize.fmin_l_bfgs_b(f_fp, x_init, maxfun=self.max_iters, maxiter=self.max_iters, **opt_dict)
        self.x_opt = opt_result[0]
        self.f_opt = f_fp(self.x_opt)[0]
        self.funct_eval = opt_result[2]['funcalls']
        self.status = rcstrings[opt_result[2]['warnflag']]

        #a more helpful error message is available in opt_result in the Error case
        if opt_result[2]['warnflag']==2: # pragma: no coverage, this is not needed to be covered
            self.status = 'Error' + str(opt_result[2]['task'])

class opt_bfgs(Optimizer):
    def __init__(self, *args, **kwargs):
        Optimizer.__init__(self, *args, **kwargs)
        self.opt_name = "BFGS (Scipy implementation)"

    def opt(self, x_init, f_fp=None, f=None, fp=None):
        """
        Run the optimizer

        """
        rcstrings = ['','Maximum number of iterations exceeded', 'Gradient and/or function calls not changing']

        opt_dict = {}
        if self.xtol is not None:
            print("WARNING: bfgs doesn't have an xtol arg, so I'm going to ignore it")
        if self.ftol is not None:
            print("WARNING: bfgs doesn't have an ftol arg, so I'm going to ignore it")
        if self.gtol is not None:
            opt_dict['gtol'] = self.gtol

        opt_result = optimize.fmin_bfgs(f, x_init, fp, disp=self.messages,
                                            maxiter=self.max_iters, full_output=True, **opt_dict)
        self.x_opt = opt_result[0]
        self.f_opt = f_fp(self.x_opt)[0]
        self.funct_eval = opt_result[4]
        self.status = rcstrings[opt_result[6]]

class opt_simplex(Optimizer):
    def __init__(self, *args, **kwargs):
        Optimizer.__init__(self, *args, **kwargs)
        self.opt_name = "Nelder-Mead simplex routine (via Scipy)"

    def opt(self, x_init, f_fp=None, f=None, fp=None):
        """
        The simplex optimizer does not require gradients.
        """

        statuses = ['Converged', 'Maximum number of function evaluations made', 'Maximum number of iterations reached']

        opt_dict = {}
        if self.xtol is not None:
            opt_dict['xtol'] = self.xtol
        if self.ftol is not None:
            opt_dict['ftol'] = self.ftol
        if self.gtol is not None:
            print("WARNING: simplex doesn't have an gtol arg, so I'm going to ignore it")

        opt_result = optimize.fmin(f, x_init, (), disp=self.messages,
                   maxfun=self.max_f_eval, full_output=True, **opt_dict)

        self.x_opt = opt_result[0]
        self.f_opt = opt_result[1]
        self.funct_eval = opt_result[3]
        self.status = statuses[opt_result[4]]
        self.trace = None


# class opt_rasm(Optimizer):
#     def __init__(self, *args, **kwargs):
#         Optimizer.__init__(self, *args, **kwargs)
#         self.opt_name = "Rasmussen's Conjugate Gradient"
#
#     def opt(self, x_init, f_fp=None, f=None, fp=None):
#         """
#         Run Rasmussen's Conjugate Gradient optimizer
#         """
#
#         assert f_fp != None, "Rasmussen's minimizer requires f_fp"
#         statuses = ['Converged', 'Line search failed', 'Maximum number of f evaluations reached',
#                 'NaNs in optimization']
#
#         opt_dict = {}
#         if self.xtol is not None:
#             print("WARNING: minimize doesn't have an xtol arg, so I'm going to ignore it")
#         if self.ftol is not None:
#             print("WARNING: minimize doesn't have an ftol arg, so I'm going to ignore it")
#         if self.gtol is not None:
#             print("WARNING: minimize doesn't have an gtol arg, so I'm going to ignore it")
#
#         opt_result = rasm.minimize(x_init, f_fp, (), messages=self.messages,
#                                    maxnumfuneval=self.max_f_eval)
#         self.x_opt = opt_result[0]
#         self.f_opt = opt_result[1][-1]
#         self.funct_eval = opt_result[2]
#         self.status = statuses[opt_result[3]]
#
#         self.trace = opt_result[1]

class opt_SCG(Optimizer):
    def __init__(self, *args, **kwargs):
        if 'max_f_eval' in kwargs:
            warn("max_f_eval deprecated for SCG optimizer: use max_iters instead!\nIgnoring max_f_eval!", FutureWarning)
        Optimizer.__init__(self, *args, **kwargs)

        self.opt_name = "Scaled Conjugate Gradients"

    def opt(self, x_init, f_fp=None, f=None, fp=None):
        assert not f is None
        assert not fp is None

        opt_result = SCG(f, fp, x_init,
                         maxiters=self.max_iters,
                         max_f_eval=self.max_f_eval,
                         xtol=self.xtol, ftol=self.ftol,
                         gtol=self.gtol)

        self.x_opt = opt_result[0]
        self.trace = opt_result[1]
        self.f_opt = self.trace[-1]
        self.funct_eval = opt_result[2]
        self.status = opt_result[3]

def _check_for_climin():
    try:
        import climin
    except ImportError: 
        raise ImportError("Need climin to run this optimizer. See https://github.com/BRML/climin.")

class Opt_Adadelta(Optimizer):
    def __init__(self, step_rate=0.1, decay=0.9, momentum=0, *args, **kwargs):
        Optimizer.__init__(self, *args, **kwargs)
        self.opt_name = "Adadelta (climin)"
        self.step_rate=step_rate
        self.decay = decay
        self.momentum = momentum

        _check_for_climin()


    def opt(self, x_init, f_fp=None, f=None, fp=None):
        assert not fp is None

        import climin

        opt = climin.adadelta.Adadelta(x_init, fp, step_rate=self.step_rate, decay=self.decay, momentum=self.momentum)

        for info in opt:
            if info['n_iter']>=self.max_iters:
                self.x_opt =  opt.wrt
                self.status = 'maximum number of function evaluations exceeded '
                break
        else: # pragma: no cover
            pass

class RProp(Optimizer):
    # We want the optimizer to know some things in the Optimizer implementation:
    def __init__(self, step_shrink=0.5, step_grow=1.2, min_step=1e-06, max_step=1, changes_max=0.1, *args, **kwargs):
        super(RProp, self).__init__(*args, **kwargs)
        self.opt_name = 'RProp (climin)'
        self.step_shrink = step_shrink
        self.step_grow = step_grow
        self.min_step = min_step
        self.max_step = max_step
        self.changes_max = changes_max

        _check_for_climin()
        
    def opt(self, x_init, f_fp=None, f=None, fp=None):
        # We only need the gradient of the 
        assert not fp is None

        import climin

        # Do the optimization, giving previously stored parameters
        opt = climin.rprop.Rprop(x_init, fp, 
                                 step_shrink=self.step_shrink, step_grow=self.step_grow, 
                                 min_step=self.min_step, max_step=self.max_step, 
                                 changes_max=self.changes_max)

        # Get the optimized state and transform it into Paramz readable format by setting
        # values on this object:
        # Important ones are x_opt and status:
        for info in opt:
            if info['n_iter']>=self.max_iters:
                self.x_opt =  opt.wrt
                self.status = 'maximum number of function evaluations exceeded'
                break
        else: # pragma: no cover
            pass

class Adam(Optimizer):
    # We want the optimizer to know some things in the Optimizer implementation:
    def __init__(self, step_rate=.0002,
                                  decay=0,
                                  decay_mom1=0.1,
                                  decay_mom2=0.001,
                                  momentum=0,
                 offset=1e-8, *args, **kwargs):
        super(Adam, self).__init__(*args, **kwargs)
        self.opt_name = 'Adam (climin)'
        self.step_rate = step_rate
        self.decay = 1-1e-8
        self.decay_mom1 = decay_mom1
        self.decay_mom2 = decay_mom2
        self.momentum = momentum
        self.offset = offset

        _check_for_climin()
        
    def opt(self, x_init, f_fp=None, f=None, fp=None):
        # We only need the gradient of the 
        assert not fp is None

        import climin

        # Do the optimization, giving previously stored parameters
        opt = climin.adam.Adam(x_init, fp,
                               step_rate=self.step_rate, decay=self.decay,
                               decay_mom1=self.decay_mom1, decay_mom2=self.decay_mom2,
                               momentum=self.momentum,offset=self.offset)

        # Get the optimized state and transform it into Paramz readable format by setting
        # values on this object:
        # Important ones are x_opt and status:
        for info in opt:
            if info['n_iter']>=self.max_iters:
                self.x_opt =  opt.wrt
                self.status = 'maximum number of function evaluations exceeded'
                break
        else: # pragma: no cover
            pass


def get_optimizer(f_min):

    optimizers = {'fmin_tnc': opt_tnc,
          'simplex': opt_simplex,
          'lbfgsb': opt_lbfgsb,
          'org-bfgs': opt_bfgs,
          'scg': opt_SCG,
          'adadelta':Opt_Adadelta,
                  'rprop':RProp,
                  'adam':Adam}

    #if rasm_available:
    #    optimizers['rasmussen'] = opt_rasm

    for opt_name in sorted(optimizers.keys()):
        if opt_name.lower().find(f_min.lower()) != -1:
            return optimizers[opt_name]

    raise KeyError('No optimizer was found matching the name: %s' % f_min)

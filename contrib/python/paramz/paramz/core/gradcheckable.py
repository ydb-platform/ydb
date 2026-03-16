#===============================================================================
# Copyright (c) 2015, Max Zwiessele
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
# * Neither the name of paramz.core.gradcheckable nor the names of its
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
from . import HierarchyError
from .pickleable import Pickleable
from .parentable import Parentable

class Gradcheckable(Pickleable, Parentable):
    """
    Adds the functionality for an object to be gradcheckable.
    It is just a thin wrapper of a call to the highest parent for now.
    TODO: Can be done better, by only changing parameters of the current parameter handle,
    such that object hierarchy only has to change for those.
    """
    def __init__(self, *a, **kw):
        super(Gradcheckable, self).__init__(*a, **kw)

    def checkgrad(self, verbose=0, step=1e-6, tolerance=1e-3, df_tolerance=1e-12):
        """
        Check the gradient of this parameter with respect to the highest parent's
        objective function.
        This is a three point estimate of the gradient, wiggling at the parameters
        with a stepsize step.
        The check passes if either the ratio or the difference between numerical and
        analytical gradient is smaller then tolerance.

        :param bool verbose: whether each parameter shall be checked individually.
        :param float step: the stepsize for the numerical three point gradient estimate.
        :param float tolerance: the tolerance for the gradient ratio or difference.
        :param float df_tolerance: the tolerance for df_tolerance

        .. note::
           The *dF_ratio* indicates the limit of accuracy of numerical gradients.
           If it is too small, e.g., smaller than 1e-12, the numerical gradients
           are usually not accurate enough for the tests (shown with blue).
        """
        # Make sure we always call the gradcheck on the highest parent
        # This ensures the assumption of the highest parent to hold the fixes
        # In the checkgrad function we take advantage of that, so it needs
        # to be set in place here.
        if self.has_parent():
            return self._highest_parent_._checkgrad(self, verbose=verbose, step=step, tolerance=tolerance, df_tolerance=df_tolerance)
        return self._checkgrad(self, verbose=verbose, step=step, tolerance=tolerance, df_tolerance=df_tolerance)

    def _checkgrad(self, param, verbose=0, step=1e-6, tolerance=1e-3, df_tolerance=1e-12):
        """
        Perform the checkgrad on the model.
        TODO: this can be done more efficiently, when doing it inside here
        """
        raise HierarchyError("This parameter is not in a model with a likelihood, and, therefore, cannot be gradient checked!")

# PuLP : Python LP Modeler
# Version 1.4.2

# Copyright (c) 2002-2005, Jean-Sebastien Roy (js@jeannot.org)
# Modifications Copyright (c) 2007- Stuart Anthony Mitchell (s.mitchell@auckland.ac.nz)
# $Id:solvers.py 1791 2008-04-23 22:54:34Z smit023 $

# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:

# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."""

import os
import warnings

from .. import constants
from .core import LpSolver_CMD, PulpSolverError, subprocess


class MIPCL_CMD(LpSolver_CMD):
    """The MIPCL_CMD solver"""

    name = "MIPCL_CMD"

    def __init__(
        self,
        path=None,
        keepFiles=False,
        mip=True,
        msg=True,
        options=None,
        timeLimit=None,
    ):
        """
        :param bool mip: if False, assume LP even if integer variables
        :param bool msg: if False, no log is shown
        :param float timeLimit: maximum time for solver (in seconds)
        :param list options: list of additional options to pass to solver
        :param bool keepFiles: if True, files are saved in the current directory and not deleted after solving
        :param str path: path to the solver binary
        """
        LpSolver_CMD.__init__(
            self,
            mip=mip,
            msg=msg,
            timeLimit=timeLimit,
            options=options,
            path=path,
            keepFiles=keepFiles,
        )

    def defaultPath(self):
        return self.executableExtension("mps_mipcl")

    def available(self):
        """True if the solver is available"""
        return self.executable(self.path)

    def actualSolve(self, lp):
        """Solve a well formulated lp problem"""
        if not self.executable(self.path):
            raise PulpSolverError("PuLP: cannot execute " + self.path)
        tmpMps, tmpSol = self.create_tmp_files(lp.name, "mps", "sol")
        if lp.sense == constants.LpMaximize:
            # we swap the objectives
            # because it does not handle maximization.
            warnings.warn(
                "MIPCL_CMD does not allow maximization, "
                "we will minimize the inverse of the objective function."
            )
            lp += -lp.objective
        lp.checkDuplicateVars()
        lp.checkLengthVars(52)
        lp.writeMPS(tmpMps, mpsSense=lp.sense)

        # just to report duplicated variables:
        try:
            os.remove(tmpSol)
        except:
            pass
        cmd = self.path
        cmd += f" {tmpMps}"
        cmd += f" -solfile {tmpSol}"
        if self.timeLimit is not None:
            cmd += f" -time {self.timeLimit}"
        for option in self.options:
            cmd += " " + option
        if lp.isMIP():
            if not self.mip:
                warnings.warn("MIPCL_CMD cannot solve the relaxation of a problem")
        if self.msg:
            pipe = None
        else:
            pipe = open(os.devnull, "w")

        return_code = subprocess.call(cmd.split(), stdout=pipe, stderr=pipe)
        # We need to undo the objective swap before finishing
        if lp.sense == constants.LpMaximize:
            lp += -lp.objective
        if return_code != 0:
            raise PulpSolverError("PuLP: Error while trying to execute " + self.path)
        if not os.path.exists(tmpSol):
            status = constants.LpStatusNotSolved
            status_sol = constants.LpSolutionNoSolutionFound
            values = None
        else:
            status, values, status_sol = self.readsol(tmpSol)
        self.delete_tmp_files(tmpMps, tmpSol)
        lp.assignStatus(status, status_sol)
        if status not in [constants.LpStatusInfeasible, constants.LpStatusNotSolved]:
            lp.assignVarsVals(values)

        return status

    @staticmethod
    def readsol(filename):
        """Read a MIPCL solution file"""
        with open(filename) as f:
            content = f.readlines()
        content = [l.strip() for l in content]
        values = {}
        if not len(content):
            return (
                constants.LpStatusNotSolved,
                values,
                constants.LpSolutionNoSolutionFound,
            )
        first_line = content[0]
        if first_line == "=infeas=":
            return constants.LpStatusInfeasible, values, constants.LpSolutionInfeasible
        objective, value = first_line.split()
        # this is a workaround.
        # Not sure if it always returns this limit when unbounded.
        if abs(float(value)) >= 9.999999995e10:
            return constants.LpStatusUnbounded, values, constants.LpSolutionUnbounded
        for line in content[1:]:
            name, value = line.split()
            values[name] = float(value)
        # I'm not sure how this solver announces the optimality
        # of a solution so we assume it is integer feasible
        return constants.LpStatusOptimal, values, constants.LpSolutionIntegerFeasible

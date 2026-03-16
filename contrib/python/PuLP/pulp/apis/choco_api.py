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


class CHOCO_CMD(LpSolver_CMD):
    """The CHOCO_CMD solver"""

    name = "CHOCO_CMD"

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
        return self.executableExtension("choco-parsers-with-dependencies.jar")

    def available(self):
        """True if the solver is available"""
        java_path = self.executableExtension("java")
        return self.executable(self.path) and self.executable(java_path)

    def actualSolve(self, lp):
        """Solve a well formulated lp problem"""
        java_path = self.executableExtension("java")
        if not self.executable(java_path):
            raise PulpSolverError(
                "PuLP: java needs to be installed and accesible in order to use CHOCO_CMD"
            )
        if not os.path.exists(self.path):
            raise PulpSolverError("PuLP: cannot execute " + self.path)
        tmpMps, tmpLp, tmpSol = self.create_tmp_files(lp.name, "mps", "lp", "sol")
        # just to report duplicated variables:
        lp.checkDuplicateVars()

        lp.writeMPS(tmpMps, mpsSense=lp.sense)
        try:
            os.remove(tmpSol)
        except:
            pass
        cmd = java_path + ' -cp "' + self.path + '" org.chocosolver.parser.mps.ChocoMPS'
        if self.timeLimit is not None:
            cmd += f" -limit [-{self.timeLimit}s]"
        cmd += " " + " ".join([f"{key} {value}" for key, value in self.options])
        cmd += f" {tmpMps}"
        if lp.sense == constants.LpMaximize:
            cmd += " -max"
        if lp.isMIP():
            if not self.mip:
                warnings.warn("CHOCO_CMD cannot solve the relaxation of a problem")
        # we always get the output to a file.
        # if not, we cannot read it afterwards
        # (we thus ignore the self.msg parameter)
        pipe = open(tmpSol, "w")

        return_code = subprocess.call(cmd, stdout=pipe, stderr=pipe, shell=True)

        if return_code != 0:
            raise PulpSolverError("PuLP: Error while trying to execute " + self.path)
        if not os.path.exists(tmpSol):
            status = constants.LpStatusNotSolved
            status_sol = constants.LpSolutionNoSolutionFound
            values = None
        else:
            status, values, status_sol = self.readsol(tmpSol)
        self.delete_tmp_files(tmpMps, tmpLp, tmpSol)

        lp.assignStatus(status, status_sol)
        if status not in [constants.LpStatusInfeasible, constants.LpStatusNotSolved]:
            lp.assignVarsVals(values)

        return status

    @staticmethod
    def readsol(filename):
        """Read a Choco solution file"""
        # TODO: figure out the unbounded status in choco solver
        chocoStatus = {
            "OPTIMUM FOUND": constants.LpStatusOptimal,
            "SATISFIABLE": constants.LpStatusOptimal,
            "UNSATISFIABLE": constants.LpStatusInfeasible,
            "UNKNOWN": constants.LpStatusNotSolved,
        }

        chocoSolStatus = {
            "OPTIMUM FOUND": constants.LpSolutionOptimal,
            "SATISFIABLE": constants.LpSolutionIntegerFeasible,
            "UNSATISFIABLE": constants.LpSolutionInfeasible,
            "UNKNOWN": constants.LpSolutionNoSolutionFound,
        }

        status = constants.LpStatusNotSolved
        sol_status = constants.LpSolutionNoSolutionFound
        values = {}
        with open(filename) as f:
            content = f.readlines()
        content = [l.strip() for l in content if l[:2] not in ["o ", "c "]]
        if not len(content):
            return status, values, sol_status
        if content[0][:2] == "s ":
            status_str = content[0][2:]
            status = chocoStatus[status_str]
            sol_status = chocoSolStatus[status_str]
        for line in content[1:]:
            name, value = line.split()
            values[name] = float(value)

        return status, values, sol_status

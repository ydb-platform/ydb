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

from .. import constants
from .core import (
    LpSolver,
    LpSolver_CMD,
    PulpSolverError,
    clock,
    log,
    operating_system,
    subprocess,
)

glpk_path = "glpsol"


class GLPK_CMD(LpSolver_CMD):
    """The GLPK LP solver"""

    name = "GLPK_CMD"

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
        return self.executableExtension(glpk_path)

    def available(self):
        """True if the solver is available"""
        return self.executable(self.path)

    def actualSolve(self, lp):
        """Solve a well formulated lp problem"""
        if not self.executable(self.path):
            raise PulpSolverError("PuLP: cannot execute " + self.path)
        tmpLp, tmpOut, tmpSol = self.create_tmp_files(lp.name, "lp", "out", "sol")
        lp.writeLP(tmpLp, writeSOS=0)
        proc = ["glpsol", "--cpxlp", tmpLp, "-o", tmpOut, "-w", tmpSol]
        if self.timeLimit:
            proc.extend(["--tmlim", str(self.timeLimit)])
        if not self.mip:
            proc.append("--nomip")
        proc.extend(self.options)

        self.solution_time = clock()
        if not self.msg:
            proc[0] = self.path
            pipe = open(os.devnull, "w")
            if operating_system == "win":
                # Prevent flashing windows if used from a GUI application
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                rc = subprocess.call(
                    proc, stdout=pipe, stderr=pipe, startupinfo=startupinfo
                )
            else:
                rc = subprocess.call(proc, stdout=pipe, stderr=pipe)
            if rc:
                raise PulpSolverError(
                    "PuLP: Error while trying to execute " + self.path
                )
            pipe.close()
        else:
            if os.name != "nt":
                rc = os.spawnvp(os.P_WAIT, self.path, proc)
            else:
                rc = os.spawnv(os.P_WAIT, self.executable(self.path), proc)
            if rc == 127:
                raise PulpSolverError(
                    "PuLP: Error while trying to execute " + self.path
                )
        self.solution_time += clock()

        if not os.path.exists(tmpSol):
            raise PulpSolverError("PuLP: Error while executing " + self.path)

        status, values = self.readsol(tmpOut, tmpSol)

        vars = dict([(var.name, var) for var in lp.variables()])
        for name, value in values.items():
            if name not in vars:  # __dummy
                continue
            var = vars[name]
            if var.cat == constants.LpInteger and self.mip:
                values[name] = int(value)
            else:
                values[name] = float(value)

        lp.assignVarsVals(values)
        lp.assignStatus(status)
        self.delete_tmp_files(tmpLp, tmpSol)
        return status

    def readsol(self, outFile, solFile):
        """Read GLPK solution files"""

        # first determine status and column names
        with open(outFile) as f:
            f.readline()
            rows = int(f.readline().split()[1])
            cols = int(f.readline().split()[1])
            f.readline()
            statusString = f.readline()[12:-1]
            glpkStatus = {
                "INTEGER OPTIMAL": constants.LpStatusOptimal,
                "INTEGER NON-OPTIMAL": constants.LpStatusOptimal,
                "OPTIMAL": constants.LpStatusOptimal,
                "INFEASIBLE (FINAL)": constants.LpStatusInfeasible,
                "INTEGER UNDEFINED": constants.LpStatusUndefined,
                "UNBOUNDED": constants.LpStatusUnbounded,
                "UNDEFINED": constants.LpStatusUndefined,
                "INTEGER EMPTY": constants.LpStatusInfeasible,
            }
            if statusString not in glpkStatus:
                raise PulpSolverError("Unknown status returned by GLPK")
            status = glpkStatus[statusString]
            isInteger = statusString in [
                "INTEGER NON-OPTIMAL",
                "INTEGER OPTIMAL",
                "INTEGER UNDEFINED",
                "INTEGER EMPTY",
            ]
            names = []
            for i in range(4):
                f.readline()
            for i in range(rows):
                line = f.readline().split()
                if len(line) == 2:
                    f.readline()
            for i in range(3):
                f.readline()
            for i in range(cols):
                line = f.readline().split()
                name = line[1]
                if len(line) == 2:
                    f.readline()
                names.append(name)

        # now actually read column values
        with open(solFile) as f:

            values = []
            status2 = constants.LpStatusUndefined
            vpos = 2

            while True:
                ln = f.readline()
                elems = ln.split()
                if elems[0] == "c":
                    continue
                if elems[0] == "e":
                    break
                if elems[0] == "j":
                    values.append(elems[vpos])
                if elems[0] == "s":
                    status2 = {
                        "o": constants.LpStatusOptimal,
                        "f": constants.LpStatusOptimal,
                        "n": constants.LpStatusInfeasible,
                        "u": constants.LpStatusUndefined,
                    }[elems[4]]
                    vpos = {"mip": 2, "bas": 3, "ipt": 2}[elems[1]]

            values = dict(zip(names, values))

            assert status == status2

            return status, values


GLPK = GLPK_CMD

# get the glpk name in global scope
glpk = None


class PYGLPK(LpSolver):
    """
    The glpk LP/MIP solver (via its python interface)

    Copyright Christophe-Marie Duquesne 2012

    The glpk variables are available (after a solve) in var.solverVar
    The glpk constraints are available in constraint.solverConstraint
    The Model is in prob.solverModel
    """

    name = "PYGLPK"

    try:
        # import the model into the global scope
        global glpk
        import glpk.glpkpi as glpk  # type: ignore[import-not-found]
    except:

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp, callback=None):
            """Solve a well formulated lp problem"""
            raise PulpSolverError("GLPK: Not Available")

    else:

        def __init__(
            self, mip=True, msg=True, timeLimit=None, gapRel=None, **solverParams
        ):
            """
            Initializes the glpk solver.

            @param mip: if False the solver will solve a MIP as an LP
            @param msg: displays information from the solver to stdout
            @param timeLimit: not handled
            @param gapRel: not handled
            @param solverParams: not handled
            """
            LpSolver.__init__(self, mip, msg)
            if not self.msg:
                glpk.glp_term_out(glpk.GLP_OFF)

        def findSolutionValues(self, lp):
            prob = lp.solverModel
            if self.mip and self.hasMIPConstraints(lp.solverModel):
                solutionStatus = glpk.glp_mip_status(prob)
            else:
                solutionStatus = glpk.glp_get_status(prob)
            glpkLpStatus = {
                glpk.GLP_OPT: constants.LpStatusOptimal,
                glpk.GLP_UNDEF: constants.LpStatusUndefined,
                glpk.GLP_FEAS: constants.LpStatusOptimal,
                glpk.GLP_INFEAS: constants.LpStatusInfeasible,
                glpk.GLP_NOFEAS: constants.LpStatusInfeasible,
                glpk.GLP_UNBND: constants.LpStatusUnbounded,
            }
            # populate pulp solution values
            for var in lp.variables():
                if self.mip and self.hasMIPConstraints(lp.solverModel):
                    var.varValue = glpk.glp_mip_col_val(prob, var.glpk_index)
                else:
                    var.varValue = glpk.glp_get_col_prim(prob, var.glpk_index)
                var.dj = glpk.glp_get_col_dual(prob, var.glpk_index)
            # put pi and slack variables against the constraints
            for constr in lp.constraints.values():
                if self.mip and self.hasMIPConstraints(lp.solverModel):
                    row_val = glpk.glp_mip_row_val(prob, constr.glpk_index)
                else:
                    row_val = glpk.glp_get_row_prim(prob, constr.glpk_index)
                constr.slack = -constr.constant - row_val
                constr.pi = glpk.glp_get_row_dual(prob, constr.glpk_index)
            lp.resolveOK = True
            for var in lp.variables():
                var.isModified = False
            status = glpkLpStatus.get(solutionStatus, constants.LpStatusUndefined)
            lp.assignStatus(status)
            return status

        def available(self):
            """True if the solver is available"""
            return True

        def hasMIPConstraints(self, solverModel):
            return (
                glpk.glp_get_num_int(solverModel) > 0
                or glpk.glp_get_num_bin(solverModel) > 0
            )

        def callSolver(self, lp, callback=None):
            """Solves the problem with glpk"""
            self.solveTime = -clock()
            glpk.glp_adv_basis(lp.solverModel, 0)
            glpk.glp_simplex(lp.solverModel, None)
            if self.mip and self.hasMIPConstraints(lp.solverModel):
                status = glpk.glp_get_status(lp.solverModel)
                if status in (glpk.GLP_OPT, glpk.GLP_UNDEF, glpk.GLP_FEAS):
                    glpk.glp_intopt(lp.solverModel, None)
            self.solveTime += clock()

        def buildSolverModel(self, lp):
            """
            Takes the pulp lp model and translates it into a glpk model
            """
            log.debug("create the glpk model")
            prob = glpk.glp_create_prob()
            glpk.glp_set_prob_name(prob, lp.name)
            log.debug("set the sense of the problem")
            if lp.sense == constants.LpMaximize:
                glpk.glp_set_obj_dir(prob, glpk.GLP_MAX)
            log.debug("add the constraints to the problem")
            glpk.glp_add_rows(prob, len(list(lp.constraints.keys())))
            for i, v in enumerate(lp.constraints.items(), start=1):
                name, constraint = v
                glpk.glp_set_row_name(prob, i, name)
                if constraint.sense == constants.LpConstraintLE:
                    glpk.glp_set_row_bnds(
                        prob, i, glpk.GLP_UP, 0.0, -constraint.constant
                    )
                elif constraint.sense == constants.LpConstraintGE:
                    glpk.glp_set_row_bnds(
                        prob, i, glpk.GLP_LO, -constraint.constant, 0.0
                    )
                elif constraint.sense == constants.LpConstraintEQ:
                    glpk.glp_set_row_bnds(
                        prob, i, glpk.GLP_FX, -constraint.constant, -constraint.constant
                    )
                else:
                    raise PulpSolverError("Detected an invalid constraint type")
                constraint.glpk_index = i
            log.debug("add the variables to the problem")
            glpk.glp_add_cols(prob, len(lp.variables()))
            for j, var in enumerate(lp.variables(), start=1):
                glpk.glp_set_col_name(prob, j, var.name)
                lb = 0.0
                ub = 0.0
                t = glpk.GLP_FR
                if not var.lowBound is None:
                    lb = var.lowBound
                    t = glpk.GLP_LO
                if not var.upBound is None:
                    ub = var.upBound
                    t = glpk.GLP_UP
                if not var.upBound is None and not var.lowBound is None:
                    if ub == lb:
                        t = glpk.GLP_FX
                    else:
                        t = glpk.GLP_DB
                glpk.glp_set_col_bnds(prob, j, t, lb, ub)
                if var.cat == constants.LpInteger:
                    glpk.glp_set_col_kind(prob, j, glpk.GLP_IV)
                    assert glpk.glp_get_col_kind(prob, j) == glpk.GLP_IV
                var.glpk_index = j
            log.debug("set the objective function")
            for var in lp.variables():
                value = lp.objective.get(var)
                if value:
                    glpk.glp_set_obj_coef(prob, var.glpk_index, value)
            log.debug("set the problem matrix")
            for constraint in lp.constraints.values():
                l = len(list(constraint.items()))
                ind = glpk.intArray(l + 1)
                val = glpk.doubleArray(l + 1)
                for j, v in enumerate(constraint.items(), start=1):
                    var, value = v
                    ind[j] = var.glpk_index
                    val[j] = value
                glpk.glp_set_mat_row(prob, constraint.glpk_index, l, ind, val)
            lp.solverModel = prob
            # glpk.glp_write_lp(prob, None, "glpk.lp")

        def actualSolve(self, lp, callback=None):
            """
            Solve a well formulated lp problem

            creates a glpk model, variables and constraints and attaches
            them to the lp model which it then solves
            """
            self.buildSolverModel(lp)
            # set the initial solution
            log.debug("Solve the Model using glpk")
            self.callSolver(lp, callback=callback)
            # get the solution information
            solutionStatus = self.findSolutionValues(lp)
            for var in lp.variables():
                var.modified = False
            for constraint in lp.constraints.values():
                constraint.modified = False
            return solutionStatus

        def actualResolve(self, lp, callback=None):
            """
            Solve a well formulated lp problem

            uses the old solver and modifies the rhs of the modified
            constraints
            """
            prob = lp.solverModel
            log.debug("Resolve the Model using glpk")
            for constraint in lp.constraints.values():
                i = constraint.glpk_index
                if constraint.modified:
                    if constraint.sense == constants.LpConstraintLE:
                        glpk.glp_set_row_bnds(
                            prob, i, glpk.GLP_UP, 0.0, -constraint.constant
                        )
                    elif constraint.sense == constants.LpConstraintGE:
                        glpk.glp_set_row_bnds(
                            prob, i, glpk.GLP_LO, -constraint.constant, 0.0
                        )
                    elif constraint.sense == constants.LpConstraintEQ:
                        glpk.glp_set_row_bnds(
                            prob,
                            i,
                            glpk.GLP_FX,
                            -constraint.constant,
                            -constraint.constant,
                        )
                    else:
                        raise PulpSolverError("Detected an invalid constraint type")
            self.callSolver(lp, callback=callback)
            # get the solution information
            solutionStatus = self.findSolutionValues(lp)
            for var in lp.variables():
                var.modified = False
            for constraint in lp.constraints.values():
                constraint.modified = False
            return solutionStatus

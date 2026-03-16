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

import sys
from typing import Optional

from .. import constants
from .core import LpSolver, PulpSolverError


class MOSEK(LpSolver):
    """Mosek lp and mip solver (via Mosek Optimizer API)."""

    name = "MOSEK"
    try:
        global mosek
        import mosek  # type: ignore[import-not-found]

        env = mosek.Env()  # type: ignore[name-defined]
    except ImportError:

        def available(self):
            """True if Mosek is available."""
            return False

        def actualSolve(self, lp, callback=None):
            """Solves a well-formulated lp problem."""
            raise PulpSolverError("MOSEK : Not Available")

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            timeLimit: Optional[float] = None,
            options: Optional[dict] = None,
            task_file_name="",
            sol_type=mosek.soltype.bas,  # type: ignore[name-defined]
        ):
            """Initializes the Mosek solver.

            Keyword arguments:

            @param mip: If False, then solve MIP as LP.

            @param msg: Enable Mosek log output.

            @param float timeLimit: maximum time for solver (in seconds)

            @param options: Accepts a dictionary of Mosek solver parameters. Ignore to
                            use default parameter values. Eg: options = {mosek.dparam.mio_max_time:30}
                            sets the maximum time spent by the Mixed Integer optimizer to 30 seconds.
                            Equivalently, one could also write: options = {"MSK_DPAR_MIO_MAX_TIME":30}
                            which uses the generic parameter name as used within the solver, instead of
                            using an object from the Mosek Optimizer API (Python), as before.

            @param task_file_name: Writes a Mosek task file of the given name. By default,
                            no task file will be written. Eg: task_file_name = "eg1.opf".

            @param sol_type: Mosek supports three types of solutions: mosek.soltype.bas
                            (Basic solution, default), mosek.soltype.itr (Interior-point
                            solution) and mosek.soltype.itg (Integer solution).

            For a full list of Mosek parameters (for the Mosek Optimizer API) and supported task file
            formats, please see https://docs.mosek.com/9.1/pythonapi/parameters.html#doc-all-parameter-list.
            """
            self.mip = mip
            self.msg = msg
            self.timeLimit = timeLimit
            self.task_file_name = task_file_name
            self.solution_type = sol_type
            if options is None:
                options = {}
            self.options = options
            if self.timeLimit is not None:
                timeLimit_keys = {"MSK_DPAR_MIO_MAX_TIME", mosek.dparam.mio_max_time}  # type: ignore[name-defined]
                if not timeLimit_keys.isdisjoint(self.options.keys()):
                    raise ValueError(
                        "timeLimit parameter has been provided trough `timeLimit` and `options`."
                    )
                self.options["MSK_DPAR_MIO_MAX_TIME"] = self.timeLimit

        def available(self):
            """True if Mosek is available."""
            return True

        def setOutStream(self, text):
            """Sets the log-output stream."""
            sys.stdout.write(text)
            sys.stdout.flush()

        def buildSolverModel(self, lp, inf=1e20):
            """Translate the problem into a Mosek task object."""
            self.cons = lp.constraints
            self.numcons = len(self.cons)
            self.cons_dict = {}
            i = 0
            for c in self.cons:
                self.cons_dict[c] = i
                i = i + 1
            self.vars = list(lp.variables())
            self.numvars = len(self.vars)
            self.var_dict = {}
            # Checking for repeated names
            lp.checkDuplicateVars()
            self.task = MOSEK.env.Task()
            self.task.appendcons(self.numcons)
            self.task.appendvars(self.numvars)
            if self.msg:
                self.task.set_Stream(mosek.streamtype.log, self.setOutStream)
            # Adding variables
            for i in range(self.numvars):
                vname = self.vars[i].name
                self.var_dict[vname] = i
                self.task.putvarname(i, vname)
                # Variable type (Default: Continuous)
                if self.mip & (self.vars[i].cat == constants.LpInteger):
                    self.task.putvartype(i, mosek.variabletype.type_int)
                    self.solution_type = mosek.soltype.itg
                # Variable bounds
                vbkey = mosek.boundkey.fr
                vup = inf
                vlow = -inf
                if self.vars[i].lowBound != None:
                    vlow = self.vars[i].lowBound
                    if self.vars[i].upBound != None:
                        vup = self.vars[i].upBound
                        vbkey = mosek.boundkey.ra
                    else:
                        vbkey = mosek.boundkey.lo
                elif self.vars[i].upBound != None:
                    vup = self.vars[i].upBound
                    vbkey = mosek.boundkey.up
                self.task.putvarbound(i, vbkey, vlow, vup)
                # Objective coefficient for the current variable.
                self.task.putcj(i, lp.objective.get(self.vars[i], 0.0))
            # Coefficient matrix
            self.A_rows, self.A_cols, self.A_vals = zip(
                *[
                    [self.cons_dict[row], self.var_dict[col], coeff]
                    for col, row, coeff in lp.coefficients()
                ]
            )
            self.task.putaijlist(self.A_rows, self.A_cols, self.A_vals)
            # Constraints
            self.constraint_data_list = []
            for c in self.cons:
                cname = self.cons[c].name
                if cname != None:
                    self.task.putconname(self.cons_dict[c], cname)
                else:
                    self.task.putconname(self.cons_dict[c], c)
                csense = self.cons[c].sense
                cconst = -self.cons[c].constant
                clow = -inf
                cup = inf
                # Constraint bounds
                if csense == constants.LpConstraintEQ:
                    cbkey = mosek.boundkey.fx
                    clow = cconst
                    cup = cconst
                elif csense == constants.LpConstraintGE:
                    cbkey = mosek.boundkey.lo
                    clow = cconst
                elif csense == constants.LpConstraintLE:
                    cbkey = mosek.boundkey.up
                    cup = cconst
                else:
                    raise PulpSolverError("Invalid constraint type.")
                self.constraint_data_list.append([self.cons_dict[c], cbkey, clow, cup])
            self.cons_id_list, self.cbkey_list, self.clow_list, self.cup_list = zip(
                *self.constraint_data_list
            )
            self.task.putconboundlist(
                self.cons_id_list, self.cbkey_list, self.clow_list, self.cup_list
            )
            # Objective sense
            if lp.sense == constants.LpMaximize:
                self.task.putobjsense(mosek.objsense.maximize)
            else:
                self.task.putobjsense(mosek.objsense.minimize)

        def findSolutionValues(self, lp):
            """
            Read the solution values and status from the Mosek task object. Note: Since the status
            map from mosek.solsta to LpStatus is not exact, it is recommended that one enables the
            log output and then refer to Mosek documentation for a better understanding of the
            solution (especially in the case of mip problems).
            """
            self.solsta = self.task.getsolsta(self.solution_type)
            self.solution_status_dict = {
                mosek.solsta.optimal: constants.LpStatusOptimal,
                mosek.solsta.prim_infeas_cer: constants.LpStatusInfeasible,
                mosek.solsta.dual_infeas_cer: constants.LpStatusUnbounded,
                mosek.solsta.unknown: constants.LpStatusUndefined,
                mosek.solsta.integer_optimal: constants.LpStatusOptimal,
                mosek.solsta.prim_illposed_cer: constants.LpStatusNotSolved,
                mosek.solsta.dual_illposed_cer: constants.LpStatusNotSolved,
                mosek.solsta.prim_feas: constants.LpStatusNotSolved,
                mosek.solsta.dual_feas: constants.LpStatusNotSolved,
                mosek.solsta.prim_and_dual_feas: constants.LpStatusNotSolved,
            }
            # Variable values.
            try:
                self.xx = [0.0] * self.numvars
                self.task.getxx(self.solution_type, self.xx)
                for var in lp.variables():
                    var.varValue = self.xx[self.var_dict[var.name]]
            except mosek.Error:
                pass
            # Constraint slack variables.
            try:
                self.xc = [0.0] * self.numcons
                self.task.getxc(self.solution_type, self.xc)
                for con in lp.constraints:
                    lp.constraints[con].slack = -(
                        self.cons[con].constant + self.xc[self.cons_dict[con]]
                    )
            except mosek.Error:
                pass
            # Reduced costs.
            if self.solution_type != mosek.soltype.itg:
                try:
                    self.x_rc = [0.0] * self.numvars
                    self.task.getreducedcosts(
                        self.solution_type, 0, self.numvars, self.x_rc
                    )
                    for var in lp.variables():
                        var.dj = self.x_rc[self.var_dict[var.name]]
                except mosek.Error:
                    pass
                # Constraint Pi variables.
                try:
                    self.y = [0.0] * self.numcons
                    self.task.gety(self.solution_type, self.y)
                    for con in lp.constraints:
                        lp.constraints[con].pi = self.y[self.cons_dict[con]]
                except mosek.Error:
                    pass

        def putparam(self, par, val):
            """
            Pass the values of valid parameters to Mosek.
            """
            if isinstance(par, mosek.dparam):
                self.task.putdouparam(par, val)
            elif isinstance(par, mosek.iparam):
                self.task.putintparam(par, val)
            elif isinstance(par, mosek.sparam):
                self.task.putstrparam(par, val)
            elif isinstance(par, str):
                if par.startswith("MSK_DPAR_"):
                    self.task.putnadouparam(par, val)
                elif par.startswith("MSK_IPAR_"):
                    self.task.putnaintparam(par, val)
                elif par.startswith("MSK_SPAR_"):
                    self.task.putnastrparam(par, val)
                else:
                    raise PulpSolverError(
                        "Invalid MOSEK parameter: '{}'. Check MOSEK documentation for a list of valid parameters.".format(
                            par
                        )
                    )

        def actualSolve(self, lp):  # type: ignore[misc]
            """
            Solve a well-formulated lp problem.
            """
            self.buildSolverModel(lp)
            # Set solver parameters
            for msk_par in self.options:
                self.putparam(msk_par, self.options[msk_par])
            # Task file
            if self.task_file_name:
                self.task.writedata(self.task_file_name)
            # Optimize
            self.task.optimize()
            # Mosek solver log (default: standard output stream)
            if self.msg:
                self.task.solutionsummary(mosek.streamtype.msg)
            self.findSolutionValues(lp)
            lp.assignStatus(self.solution_status_dict[self.solsta])
            for var in lp.variables():
                var.modified = False
            for con in lp.constraints.values():
                con.modified = False
            return lp.status

        def actualResolve(self, lp, inf=1e20, **kwargs):
            """
            Modify constraints and re-solve an lp. The Mosek task object created in the first solve is used.
            """
            for c in self.cons:
                if self.cons[c].modified:
                    csense = self.cons[c].sense
                    cconst = -self.cons[c].constant
                    clow = -inf
                    cup = inf
                    # Constraint bounds
                    if csense == constants.LpConstraintEQ:
                        cbkey = mosek.boundkey.fx
                        clow = cconst
                        cup = cconst
                    elif csense == constants.LpConstraintGE:
                        cbkey = mosek.boundkey.lo
                        clow = cconst
                    elif csense == constants.LpConstraintLE:
                        cbkey = mosek.boundkey.up
                        cup = cconst
                    else:
                        raise PulpSolverError("Invalid constraint type.")
                    self.task.putconbound(self.cons_dict[c], cbkey, clow, cup)
            # Re-solve
            self.task.optimize()
            self.findSolutionValues(lp)
            lp.assignStatus(self.solution_status_dict[self.solsta])
            for var in lp.variables():
                var.modified = False
            for con in lp.constraints.values():
                con.modified = False
            return lp.status

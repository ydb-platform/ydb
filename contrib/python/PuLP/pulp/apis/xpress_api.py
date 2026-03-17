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

import re
import sys

from .. import constants
from .core import LpSolver, LpSolver_CMD, PulpSolverError, subprocess


def _ismip(lp):
    """Check whether lp is a MIP.

    From an XPRESS point of view, a problem is also a MIP if it contains
    SOS constraints."""
    return lp.isMIP() or len(lp.sos1) or len(lp.sos2)


class XPRESS(LpSolver_CMD):
    """The XPRESS LP solver that uses the XPRESS command line tool
    in a subprocess"""

    name = "XPRESS"

    def __init__(
        self,
        mip=True,
        msg=True,
        timeLimit=None,
        gapRel=None,
        options=None,
        keepFiles=False,
        path=None,
        heurFreq=None,
        heurStra=None,
        coverCuts=None,
        preSolve=None,
        warmStart=False,
    ):
        """
        Initializes the Xpress solver.

        :param bool mip: if False, assume LP even if integer variables
        :param bool msg: if False, no log is shown
        :param float timeLimit: maximum time for solver (in seconds)
        :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
        :param heurFreq: the frequency at which heuristics are used in the tree search
        :param heurStra: heuristic strategy
        :param coverCuts: the number of rounds of lifted cover inequalities at the top node
        :param preSolve: whether presolving should be performed before the main algorithm
        :param options: Adding more options, e.g. options = ["NODESELECTION=1", "HEURDEPTH=5"]
                        More about Xpress options and control parameters please see
                        https://www.fico.com/fico-xpress-optimization/docs/latest/solver/optimizer/HTML/chapter7.html
        :param bool warmStart: if True, then use current variable values as start
        """
        LpSolver_CMD.__init__(
            self,
            gapRel=gapRel,
            mip=mip,
            msg=msg,
            timeLimit=timeLimit,
            options=options,
            path=path,
            keepFiles=keepFiles,
            heurFreq=heurFreq,
            heurStra=heurStra,
            coverCuts=coverCuts,
            preSolve=preSolve,
            warmStart=warmStart,
        )

    def defaultPath(self):
        return self.executableExtension("optimizer")

    def available(self):
        """True if the solver is available"""
        return self.executable(self.path)

    def actualSolve(self, lp):
        """Solve a well formulated lp problem"""
        if not self.executable(self.path):
            raise PulpSolverError("PuLP: cannot execute " + self.path)
        tmpLp, tmpSol, tmpCmd, tmpAttr, tmpStart = self.create_tmp_files(
            lp.name, "lp", "prt", "cmd", "attr", "slx"
        )
        variables = lp.writeLP(tmpLp, writeSOS=1, mip=self.mip)
        if self.optionsDict.get("warmStart", False):
            start = [(v.name, v.value()) for v in variables if v.value() is not None]
            self.writeslxsol(tmpStart, start)
        # Explicitly capture some attributes so that we can easily get
        # information about the solution.
        attrNames = []
        if _ismip(lp) and self.mip:
            attrNames.extend(["mipobjval", "bestbound", "mipstatus"])
            statusmap = {
                0: constants.LpStatusUndefined,  # XPRS_MIP_NOT_LOADED
                1: constants.LpStatusUndefined,  # XPRS_MIP_LP_NOT_OPTIMAL
                2: constants.LpStatusUndefined,  # XPRS_MIP_LP_OPTIMAL
                3: constants.LpStatusUndefined,  # XPRS_MIP_NO_SOL_FOUND
                4: constants.LpStatusUndefined,  # XPRS_MIP_SOLUTION
                5: constants.LpStatusInfeasible,  # XPRS_MIP_INFEAS
                6: constants.LpStatusOptimal,  # XPRS_MIP_OPTIMAL
                7: constants.LpStatusUndefined,  # XPRS_MIP_UNBOUNDED
            }
            statuskey = "mipstatus"
        else:
            attrNames.extend(["lpobjval", "lpstatus"])
            statusmap = {
                0: constants.LpStatusNotSolved,  # XPRS_LP_UNSTARTED
                1: constants.LpStatusOptimal,  # XPRS_LP_OPTIMAL
                2: constants.LpStatusInfeasible,  # XPRS_LP_INFEAS
                3: constants.LpStatusUndefined,  # XPRS_LP_CUTOFF
                4: constants.LpStatusUndefined,  # XPRS_LP_UNFINISHED
                5: constants.LpStatusUnbounded,  # XPRS_LP_UNBOUNDED
                6: constants.LpStatusUndefined,  # XPRS_LP_CUTOFF_IN_DUAL
                7: constants.LpStatusNotSolved,  # XPRS_LP_UNSOLVED
                8: constants.LpStatusUndefined,  # XPRS_LP_NONCONVEX
            }
            statuskey = "lpstatus"
        with open(tmpCmd, "w") as cmd:
            if not self.msg:
                cmd.write("OUTPUTLOG=0\n")
            # The readprob command must be in lower case for correct filename handling
            cmd.write("readprob " + self.quote_path(tmpLp) + "\n")
            if self.timeLimit is not None:
                cmd.write("MAXTIME=%d\n" % self.timeLimit)
            targetGap = self.optionsDict.get("gapRel")
            if targetGap is not None:
                cmd.write(f"MIPRELSTOP={targetGap:f}\n")
            heurFreq = self.optionsDict.get("heurFreq")
            if heurFreq is not None:
                cmd.write("HEURFREQ=%d\n" % heurFreq)
            heurStra = self.optionsDict.get("heurStra")
            if heurStra is not None:
                cmd.write("HEURSTRATEGY=%d\n" % heurStra)
            coverCuts = self.optionsDict.get("coverCuts")
            if coverCuts is not None:
                cmd.write("COVERCUTS=%d\n" % coverCuts)
            preSolve = self.optionsDict.get("preSolve")
            if preSolve is not None:
                cmd.write("PRESOLVE=%d\n" % preSolve)
            if self.optionsDict.get("warmStart", False):
                cmd.write("readslxsol " + self.quote_path(tmpStart) + "\n")
            for option in self.options:
                cmd.write(option + "\n")
            if _ismip(lp) and self.mip:
                cmd.write("mipoptimize\n")
            else:
                cmd.write("lpoptimize\n")
            # The writeprtsol command must be in lower case for correct filename handling
            cmd.write("writeprtsol " + self.quote_path(tmpSol) + "\n")
            cmd.write(
                f"set fh [open {self.quote_path(tmpAttr)} w]; list\n"
            )  # `list` to suppress output

            for attr in attrNames:
                cmd.write(f'puts $fh "{attr}=${attr}"\n')
            cmd.write("close $fh\n")
            cmd.write("QUIT\n")
        with open(tmpCmd) as cmd:
            consume = False
            subout = None
            suberr = None
            if not self.msg:
                # Xpress writes a banner before we can disable output. So
                # we have to explicitly consume the banner.
                if sys.hexversion >= 0x03030000:
                    subout = subprocess.DEVNULL
                    suberr = subprocess.DEVNULL
                else:
                    # We could also use open(os.devnull, 'w') but then we
                    # would be responsible for closing the file.
                    subout = subprocess.PIPE
                    suberr = subprocess.STDOUT
                    consume = True
            xpress = subprocess.Popen(
                [self.path, lp.name],
                shell=True,
                stdin=cmd,
                stdout=subout,
                stderr=suberr,
                universal_newlines=True,
            )
            if consume:
                # Special case in which messages are disabled and we have
                # to consume any output
                for _ in xpress.stdout:
                    pass

            if xpress.wait() != 0:
                raise PulpSolverError("PuLP: Error while executing " + self.path)
        values, redcost, slacks, duals, attrs = self.readsol(tmpSol, tmpAttr)
        self.delete_tmp_files(tmpLp, tmpSol, tmpCmd, tmpAttr)
        status = statusmap.get(attrs.get(statuskey, -1), constants.LpStatusUndefined)
        lp.assignVarsVals(values)
        lp.assignVarsDj(redcost)
        lp.assignConsSlack(slacks)
        lp.assignConsPi(duals)
        lp.assignStatus(status)
        return status

    @staticmethod
    def readsol(filename, attrfile):
        """Read an XPRESS solution file"""
        values = {}
        redcost = {}
        slacks = {}
        duals = {}
        with open(filename) as f:
            for lineno, _line in enumerate(f):
                # The first 6 lines are status information
                if lineno < 6:
                    continue
                elif lineno == 6:
                    # Line with status information
                    _line = _line.split()
                    rows = int(_line[2])
                    cols = int(_line[5])
                elif lineno < 10:
                    # Empty line, "Solution Statistics", objective direction
                    pass
                elif lineno == 10:
                    # Solution status
                    pass
                else:
                    # There is some more stuff and then follows the "Rows" and
                    # "Columns" section. That other stuff does not match the
                    # format of the rows/columns lines, so we can keep the
                    # parser simple
                    line = _line.split()
                    if len(line) > 1:
                        if line[0] == "C":
                            # A column
                            # (C, Number, Name, At, Value, Input Cost, Reduced Cost)
                            name = line[2]
                            values[name] = float(line[4])
                            redcost[name] = float(line[6])
                        elif len(line[0]) == 1 and line[0] in "LGRE":
                            # A row
                            # ([LGRE], Number, Name, At, Value, Slack, Dual, RHS)
                            name = line[2]
                            slacks[name] = float(line[5])
                            duals[name] = float(line[6])
        # Read the attributes that we wrote explicitly
        attrs = dict()
        with open(attrfile) as f:
            for line in f:
                fields = line.strip().split("=")
                if len(fields) == 2 and fields[0].lower() == fields[0]:
                    value = fields[1].strip()
                    try:
                        value = int(fields[1].strip())
                    except ValueError:
                        try:
                            value = float(fields[1].strip())
                        except ValueError:
                            pass
                    attrs[fields[0].strip()] = value
        return values, redcost, slacks, duals, attrs

    def writeslxsol(self, name, *values):
        """
        Write a solution file in SLX format.
        The function can write multiple solutions to the same file, each
        solution must be passed as a list of (name,value) pairs. Solutions
        are written in the order specified and are given names "solutionN"
        where N is the index of the solution in the list.

        :param string name: file name
        :param list values: list of lists of (name,value) pairs
        """
        with open(name, "w") as slx:
            for i, sol in enumerate(values):
                slx.write("NAME solution%d\n" % i)
                for name, value in sol:
                    slx.write(f" C      {name} {value:.16f}\n")
            slx.write("ENDATA\n")

    @staticmethod
    def quote_path(path):
        r"""
        Quotes a path for the Xpress optimizer console, by wrapping it in
        double quotes and escaping the following characters, which would
        otherwise be interpreted by the Tcl shell: \ $ " [
        """
        return '"' + re.sub(r'([\\$"[])', r"\\\1", path) + '"'


XPRESS_CMD = XPRESS

xpress = None


class XPRESS_PY(LpSolver):
    """The XPRESS LP solver that uses XPRESS Python API"""

    name = "XPRESS_PY"

    def __init__(
        self,
        mip=True,
        msg=True,
        timeLimit=None,
        gapRel=None,
        heurFreq=None,
        heurStra=None,
        coverCuts=None,
        preSolve=None,
        warmStart=None,
        export=None,
        options=None,
    ):
        """
        Initializes the Xpress solver.

        :param bool mip: if False, assume LP even if integer variables
        :param bool msg: if False, no log is shown
        :param float timeLimit: maximum time for solver (in seconds)
        :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
        :param heurFreq: the frequency at which heuristics are used in the tree search
        :param heurStra: heuristic strategy
        :param coverCuts: the number of rounds of lifted cover inequalities at the top node
        :param preSolve: whether presolving should be performed before the main algorithm
        :param bool warmStart: if set then use current variable values as warm start
        :param string export: if set then the model will be exported to this file before solving
        :param options: Adding more options. This is a list the elements of which
                        are either (name,value) pairs or strings "name=value".
                        More about Xpress options and control parameters please see
                        https://www.fico.com/fico-xpress-optimization/docs/latest/solver/optimizer/HTML/chapter7.html
        """
        if timeLimit is not None:
            # The Xpress time limit has this interpretation:
            # timelimit <0: Stop after -timelimit, no matter what
            # timelimit >0: Stop after timelimit only if a feasible solution
            #               exists. We overwrite this meaning here since it is
            #               somewhat counterintuitive when compared to other
            #               solvers. You can always pass a positive timlimit
            #               via `options` to get that behavior.
            timeLimit = -abs(timeLimit)
        LpSolver.__init__(
            self,
            gapRel=gapRel,
            mip=mip,
            msg=msg,
            timeLimit=timeLimit,
            options=options,
            heurFreq=heurFreq,
            heurStra=heurStra,
            coverCuts=coverCuts,
            preSolve=preSolve,
            warmStart=warmStart,
        )
        self._available = None
        self._export = export

    def available(self):
        """True if the solver is available"""
        if self._available is None:
            try:
                global xpress
                import xpress  # type: ignore[import-not-found, import-untyped, unused-ignore]

                # Always disable the global output. We only want output if
                # we install callbacks explicitly
                xpress.setOutputEnabled(False)
                self._available = True
            except:
                self._available = False
        return self._available

    def callSolver(self, lp, prepare=None):
        """Perform the actual solve from actualSolve() or actualResolve().

        :param prepare:  a function that is called with `lp` as argument
                         and allows final tweaks to `lp.solverModel` before
                         the low level solve is started.
        """
        try:
            model = lp.solverModel
            # Mark all variables and constraints as unmodified so that
            # actualResolve will do the correct thing.
            for v in lp.variables():
                v.modified = False
            for c in lp.constraints.values():
                c.modified = False

            if self._export is not None:
                if self._export.lower().endswith(".lp"):
                    model.write(self._export, "l")
                else:
                    model.write(self._export)
            if prepare is not None:
                prepare(lp)
            if _ismip(lp) and not self.mip:
                # Solve only the LP relaxation
                model.lpoptimize()
            else:
                # In all other cases, solve() does the correct thing
                model.solve()
        except (xpress.ModelError, xpress.InterfaceError, xpress.SolverError) as err:
            raise PulpSolverError(str(err))

    def findSolutionValues(self, lp):
        try:
            model = lp.solverModel

            # Build ordered lists of solver vars/cons for new API
            # PuLP stores Xpress handles in v._xprs / c._xprs; index 1 is the handle.
            xpress_vars = []
            for v in lp.variables():
                h = (
                    v._xprs[1]
                    if isinstance(v._xprs, (list, tuple)) and len(v._xprs) > 1
                    else v._xprs
                )
                xpress_vars.append((v, h))

            xpress_cons = []
            for n, c in lp.constraints.items():
                h = (
                    c._xprs[1]
                    if isinstance(c._xprs, (list, tuple)) and len(c._xprs) > 1
                    else c._xprs
                )
                xpress_cons.append((n, c, h))

            if _ismip(lp) and self.mip:
                # ------- MIP branch -------
                vals = slacks = duals = djs = None
                statusmap = {
                    0: constants.LpStatusUndefined,  # XPRS_MIP_NOT_LOADED
                    1: constants.LpStatusUndefined,  # XPRS_MIP_LP_NOT_OPTIMAL
                    2: constants.LpStatusUndefined,  # XPRS_MIP_LP_OPTIMAL
                    3: constants.LpStatusUndefined,  # XPRS_MIP_NO_SOL_FOUND
                    4: constants.LpStatusUndefined,  # XPRS_MIP_SOLUTION
                    5: constants.LpStatusInfeasible,  # XPRS_MIP_INFEAS
                    6: constants.LpStatusOptimal,  # XPRS_MIP_OPTIMAL
                    7: constants.LpStatusUndefined,  # XPRS_MIP_UNBOUNDED
                }
                statuskey = "mipstatus"

                # New API first
                try:
                    var_vals = model.getSolution([h for _, h in xpress_vars])
                    slacks = (
                        model.getSlacks([h for _, _, h in xpress_cons])
                        if xpress_cons
                        else None
                    )
                    vals = var_vals
                except Exception as e:
                    # Fallback to deprecated API (avoids DeprecationWarning only if not hit)
                    try:
                        print(e)
                        x_list, s_list = [], []
                        model.getmipsol(x_list, s_list)
                        vals, slacks = (
                            x_list if x_list else None,
                            s_list if s_list else None,
                        )
                    except Exception:
                        vals = slacks = None

                duals = None
                djs = None

            else:
                # ------- LP (continuous) branch -------
                vals = slacks = duals = djs = None
                statusmap = {
                    0: constants.LpStatusNotSolved,  # XPRS_LP_UNSTARTED
                    1: constants.LpStatusOptimal,  # XPRS_LP_OPTIMAL
                    2: constants.LpStatusInfeasible,  # XPRS_LP_INFEAS
                    3: constants.LpStatusUndefined,  # XPRS_LP_CUTOFF
                    4: constants.LpStatusUndefined,  # XPRS_LP_UNFINISHED
                    5: constants.LpStatusUnbounded,  # XPRS_LP_UNBOUNDED
                    6: constants.LpStatusUndefined,  # XPRS_LP_CUTOFF_IN_DUAL
                    7: constants.LpStatusNotSolved,  # XPRS_LP_UNSOLVED
                    8: constants.LpStatusUndefined,  # XPRS_LP_NONCONVEX
                }
                statuskey = "lpstatus"

                # New API first
                try:
                    var_vals = model.getSolution([h for _, h in xpress_vars])
                    vals = var_vals
                    slacks = (
                        model.getSlacks([h for _, _, h in xpress_cons])
                        if xpress_cons
                        else None
                    )
                    duals = (
                        model.getDuals([h for _, _, h in xpress_cons])
                        if xpress_cons
                        else None
                    )
                    djs = model.getRedCosts([h for _, h in xpress_vars])
                except Exception:
                    # Fallback to deprecated API
                    try:
                        x_list, s_list, d_list, rc_list = [], [], [], []
                        model.getlpsol(x_list, s_list, d_list, rc_list)
                        vals = x_list if x_list else None
                        slacks = s_list if s_list else None
                        duals = d_list if d_list else None
                        djs = rc_list if rc_list else None
                    except Exception:
                        vals = slacks = duals = djs = None

            # ---- write back into PuLP structures ----
            if vals is not None:
                lp.assignVarsVals(
                    {v.name: val for (v, _), val in zip(xpress_vars, vals)}
                )

            if djs is not None:
                lp.assignVarsDj({v.name: rc for (v, _), rc in zip(xpress_vars, djs)})

            if duals is not None:
                # constraints dict preserves insertion order in Python 3.7+
                lp.assignConsPi({n: pi for (n, c, _), pi in zip(xpress_cons, duals)})

            if slacks is not None:
                lp.assignConsSlack({n: s for (n, c, _), s in zip(xpress_cons, slacks)})

            status = statusmap.get(
                model.getAttrib(statuskey), constants.LpStatusUndefined
            )
            lp.assignStatus(status)
            return status

        except (xpress.ModelError, xpress.InterfaceError, xpress.SolverError) as err:
            raise PulpSolverError(str(err))

    def actualSolve(self, lp, prepare=None):
        """Solve a well formulated lp problem"""
        if not self.available():
            # Import again to get a more verbose error message
            message = "XPRESS Python API not available"
            try:
                import xpress
            except ImportError as err:
                message = str(err)
            raise PulpSolverError(message)

        self.buildSolverModel(lp)
        self.callSolver(lp, prepare)
        return self.findSolutionValues(lp)

    def buildSolverModel(self, lp):
        """
        Takes the pulp lp model and translates it into an xpress model
        """
        self._extract(lp)
        try:
            # Apply controls, warmstart etc. We do this here rather than in
            # callSolver() so that the caller has a chance to overwrite things
            # either using the `prepare` argument to callSolver() or by
            # explicitly calling
            #   self.buildSolverModel()
            #   self.callSolver()
            #   self.findSolutionValues()
            # This also avoids setting warmstart information passed to the
            # constructor from actualResolve(), which would almost certainly
            # be unintended.
            model = lp.solverModel
            # Apply controls that were passed to the constructor
            for key, name in [
                ("gapRel", "MIPRELSTOP"),
                ("timeLimit", "MAXTIME"),
                ("heurFreq", "HEURFREQ"),
                ("heurStra", "HEURSTRATEGY"),
                ("coverCuts", "COVERCUTS"),
                ("preSolve", "PRESOLVE"),
            ]:
                value = self.optionsDict.get(key, None)
                if value is not None:
                    model.setControl(name, value)

            # Apply any other controls. These overwrite controls that were
            # passed explicitly into the constructor.
            for option in self.options:
                if isinstance(option, tuple):
                    name = option[0]
                    value = option[1]
                else:
                    fields = option.split("=", 1)
                    if len(fields) != 2:
                        raise PulpSolverError("Invalid option " + str(option))
                    name = fields[0].strip()
                    value = fields[1].strip()
                try:
                    model.setControl(name, int(value))
                    continue
                except ValueError:
                    pass
                try:
                    model.setControl(name, float(value))
                    continue
                except ValueError:
                    pass
                model.setControl(name, value)
            # Setup warmstart information
            if self.optionsDict.get("warmStart", False):
                solval = list()
                colind = list()
                for v in sorted(lp.variables(), key=lambda x: x._xprs[0]):
                    if v.value() is not None:
                        solval.append(v.value())
                        colind.append(v._xprs[0])
                if _ismip(lp) and self.mip:
                    # If we have a value for every variable then use
                    # loadmipsol(), which requires a dense solution. Otherwise
                    # use addmipsol() which allows sparse vectors.
                    if len(solval) == model.attributes.cols:
                        model.loadmipsol(solval)
                    else:
                        model.addmipsol(solval, colind, "warmstart")
                else:
                    model.loadlpsol(solval, None, None, None)
            # Setup message callback if output is requested
            if self.msg:

                def message(prob, data, msg, msgtype):
                    if msgtype > 0:
                        print(msg)

                model.addcbmessage(message)
        except (xpress.ModelError, xpress.InterfaceError, xpress.SolverError) as err:
            raise PulpSolverError(str(err))

    def actualResolve(self, lp, prepare=None):
        """Resolve a problem that was previously solved by actualSolve()."""
        try:
            rhsind = list()
            rhsval = list()
            for name in sorted(lp.constraints):
                con = lp.constraints[name]
                if not con.modified:
                    continue
                if not hasattr(con, "_xprs"):
                    # Adding constraints is not implemented at the moment
                    raise PulpSolverError("Cannot add new constraints")
                # At the moment only RHS can change in pulp.py
                rhsind.append(con._xprs[0])
                rhsval.append(-con.constant)
            if len(rhsind) > 0:
                lp.solverModel.chgrhs(rhsind, rhsval)

            bndind = list()
            bndtype = list()
            bndval = list()
            for v in lp.variables():
                if not v.modified:
                    continue
                if not hasattr(v, "_xprs"):
                    # Adding variables is not implemented at the moment
                    raise PulpSolverError("Cannot add new variables")
                # At the moment only bounds can change in pulp.py
                bndind.append(v._xprs[0])
                bndtype.append("L")
                bndval.append(-xpress.infinity if v.lowBound is None else v.lowBound)
                bndind.append(v._xprs[0])
                bndtype.append("G")
                bndval.append(xpress.infinity if v.upBound is None else v.upBound)
            if len(bndtype) > 0:
                lp.solverModel.chgbounds(bndind, bndtype, bndval)

            self.callSolver(lp, prepare)
            return self.findSolutionValues(lp)
        except (xpress.ModelError, xpress.InterfaceError, xpress.SolverError) as err:
            raise PulpSolverError(str(err))

    @staticmethod
    def _reset(lp):
        """Reset any XPRESS specific information in lp."""
        if hasattr(lp, "solverModel"):
            delattr(lp, "solverModel")
        for v in lp.variables():
            if hasattr(v, "_xprs"):
                delattr(v, "_xprs")
        for c in lp.constraints.values():
            if hasattr(c, "_xprs"):
                delattr(c, "_xprs")

    def _extract(self, lp):
        """Extract a given model to an XPRESS Python API instance.

        The function stores XPRESS specific information in the `solverModel` property
        of `lp` and each variable and constraint. These information can be
        removed by calling `_reset`.
        """
        self._reset(lp)
        try:
            # Map PuLP senses -> XPRESS tokens (prefer xp.leq/geq/eq if present; else fall back to 'L','G','E')
            _XP_LEQ = getattr(xpress, "leq", "L")
            _XP_GEQ = getattr(xpress, "geq", "G")
            _XP_EQ = getattr(xpress, "eq", "E")

            _SENSE_MAP = {
                constants.LpConstraintLE: _XP_LEQ,
                constants.LpConstraintGE: _XP_GEQ,
                constants.LpConstraintEQ: _XP_EQ,
            }

            def _xp_make_constraint(lhs, rhs, xp_sense, name=None):
                """
                Create an XPRESS constraint in a way that works with both APIs:
                new: xpress.constraint(body=..., type=..., rhs=..., name=...)
                old: xpress.constraint(body=..., sense=..., rhs=..., name=...)
                """
                # Prefer the new keyword first
                try:
                    return xpress.constraint(
                        body=lhs, type=xp_sense, rhs=rhs, name=name
                    )
                except TypeError:
                    # Fallback to old keyword
                    try:
                        return xpress.constraint(
                            body=lhs, sense=xp_sense, rhs=rhs, name=name
                        )
                    except TypeError as e:
                        raise PulpSolverError(
                            f"XPRESS constraint constructor is incompatible: {e}"
                        )

            model = xpress.problem()
            if lp.sense == constants.LpMaximize:
                model.chgobjsense(xpress.maximize)

            # Create variables. We first collect the info for all variables
            # and then create all of them in one shot. This is supposed to
            # be faster in case we have to create a lot of variables.
            obj = list()
            lb = list()
            ub = list()
            ctype = list()
            names = list()
            for v in lp.variables():
                lb.append(-xpress.infinity if v.lowBound is None else v.lowBound)
                ub.append(xpress.infinity if v.upBound is None else v.upBound)
                obj.append(lp.objective.get(v, 0.0))
                if v.cat == constants.LpInteger:
                    ctype.append("I")
                elif v.cat == constants.LpBinary:
                    ctype.append("B")
                else:
                    ctype.append("C")
                names.append(v.name)
            model.addcols(obj, [0] * (len(obj) + 1), [], [], lb, ub, names, ctype)
            for j, (v, x) in enumerate(zip(lp.variables(), model.getVariable())):
                v._xprs = (j, x)

            # Generate constraints. Sort by name to get deterministic
            # ordering of constraints.
            # Constraints are generated in blocks of 100 constraints to speed
            # up things a bit but still keep memory usage small.
            cons = list()
            for i, name in enumerate(sorted(lp.constraints)):
                con = lp.constraints[name]
                # Sort the variables by index to get deterministic
                # ordering of variables in the row.
                lhs = xpress.Sum(
                    a * x._xprs[1]
                    for x, a in sorted(con.items(), key=lambda x: x[0]._xprs[0])
                )
                rhs = -con.constant

                xp_sense = _SENSE_MAP.get(con.sense)
                if xp_sense is None:
                    raise PulpSolverError(f"Unsupported constraint type {con.sense}")

                c = _xp_make_constraint(
                    lhs=lhs, rhs=rhs, xp_sense=xp_sense, name=con.name
                )
                cons.append((i, c, con))
                if len(cons) > 100:
                    model.addConstraint([c for _, c, _ in cons])
                    for i, c, con in cons:
                        con._xprs = (i, c)
                    cons = list()
            if len(cons) > 0:
                model.addConstraint([c for _, c, _ in cons])
                for i, c, con in cons:
                    con._xprs = (i, c)

            # SOS constraints
            def addsos(m, sosdict, sostype):
                """Extract sos constraints from PuLP."""
                soslist = []
                # Sort by name to get deterministic ordering. Note that
                # names may be plain integers, that is why we use str(name)
                # to pass them to the SOS constructor.
                for name in sorted(sosdict):
                    indices = []
                    weights = []
                    for v, val in sosdict[name].items():
                        indices.append(v._xprs[0])
                        weights.append(val)
                    soslist.append(xpress.sos(indices, weights, sostype, str(name)))
                if len(soslist):
                    m.addSOS(soslist)

            addsos(model, lp.sos1, 1)
            addsos(model, lp.sos2, 2)

            lp.solverModel = model
        except (xpress.ModelError, xpress.InterfaceError, xpress.SolverError) as err:
            # Undo everything
            self._reset(lp)
            raise PulpSolverError(str(err))

    def getAttribute(self, lp, which):
        """Get an arbitrary attribute for the model that was previously
        solved using actualSolve()."""
        return lp.solverModel.getAttrib(which)

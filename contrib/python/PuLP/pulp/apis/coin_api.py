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
from __future__ import annotations

import os
from typing import TYPE_CHECKING

from .. import constants
from .core import (
    LpSolver,
    LpSolver_CMD,
    PulpSolverError,
    arch,
    clock,
    devnull,
    log,
    operating_system,
    subprocess,
)

if TYPE_CHECKING:
    from .. import LpProblem

import ctypes
import warnings
from tempfile import mktemp

cbc_path = "cbc"
if operating_system == "win":
    cbc_path += ".exe"

coinMP_path = ["libCoinMP.so"]
# workaround for (https://github.com/coin-or/pulp/issues/802)
if operating_system == "osx":
    arch = "i64"
pulp_cbc_path = os.path.join(
    os.path.dirname(__file__), f"../solverdir/cbc/{operating_system}/{arch}/{cbc_path}"
)


class COIN_CMD(LpSolver_CMD):
    """The COIN CLP/CBC LP solver
    now only uses cbc
    """

    name = "COIN_CMD"

    def defaultPath(self):
        return self.executableExtension(cbc_path)

    def __init__(
        self,
        mip=True,
        msg=True,
        timeLimit=None,
        gapRel=None,
        gapAbs=None,
        presolve=None,
        cuts=None,
        strong=None,
        options=None,
        warmStart=False,
        keepFiles=False,
        path=None,
        threads=None,
        logPath=None,
        timeMode="elapsed",
        maxNodes=None,
    ):
        """
        :param bool mip: if False, assume LP even if integer variables
        :param bool msg: if False, no log is shown
        :param float timeLimit: maximum time for solver (in seconds)
        :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
        :param float gapAbs: absolute gap tolerance for the solver to stop
        :param int threads: sets the maximum number of threads
        :param list options: list of additional options to pass to solver
        :param bool warmStart: if True, the solver will use the current value of variables as a start
        :param bool keepFiles: if True, files are saved in the current directory and not deleted after solving
        :param str path: path to the solver binary
        :param str logPath: path to the log file
        :param bool presolve: if True, adds presolve on, if False, adds presolve off
        :param bool cuts: if True, adds gomory on knapsack on probing on, if False adds cuts off
        :param int strong: number of variables to look at in strong branching (range is 0 to 2147483647)
        :param str timeMode: "elapsed": count wall-time to timeLimit; "cpu": count cpu-time
        :param int maxNodes: max number of nodes during branching. Stops the solving when reached.
        """
        if warmStart and not keepFiles and operating_system == "win":
            warnings.warn(
                "When using CBC on Windows, warmStart requires keepFiles=True."
            )

        LpSolver_CMD.__init__(
            self,
            gapRel=gapRel,
            mip=mip,
            msg=msg,
            timeLimit=timeLimit,
            presolve=presolve,
            cuts=cuts,
            strong=strong,
            options=options,
            warmStart=warmStart,
            path=path,
            keepFiles=keepFiles,
            threads=threads,
            gapAbs=gapAbs,
            logPath=logPath,
            timeMode=timeMode,
            maxNodes=maxNodes,
        )

    def copy(self):
        """Make a copy of self"""
        aCopy = LpSolver_CMD.copy(self)
        aCopy.optionsDict = self.optionsDict
        return aCopy

    def actualSolve(self, lp: LpProblem, **kwargs):
        """Solve a well formulated lp problem"""
        return self.solve_CBC(lp, **kwargs)

    def available(self):
        """True if the solver is available"""
        return self.executable(self.path)

    def solve_CBC(self, lp: LpProblem, use_mps=True):
        """Solve a MIP problem using CBC"""
        if not self.executable(self.path):
            raise PulpSolverError(
                f"Pulp: cannot execute {self.path} cwd: {os.getcwd()}"
            )
        tmpLp, tmpMps, tmpSol, tmpMst = self.create_tmp_files(
            lp.name, "lp", "mps", "sol", "mst"
        )
        if use_mps:
            vs, variablesNames, constraintsNames, objectiveName = lp.writeMPS(
                tmpMps, rename=1
            )
            cmds = " " + tmpMps + " "
            if lp.sense == constants.LpMaximize:
                cmds += "-max "
        else:
            vs = lp.writeLP(tmpLp)
            # In the Lp we do not create new variable or constraint names:
            variablesNames = {v.name: v.name for v in vs}
            constraintsNames = {c: c for c in lp.constraints}
            cmds = " " + tmpLp + " "
        if self.optionsDict.get("warmStart", False):
            self.writesol(tmpMst, lp, vs, variablesNames, constraintsNames)
            cmds += f"-mips {tmpMst} "
        if self.timeLimit is not None:
            cmds += f"-sec {self.timeLimit} "
        if self.optionsDict.get("presolve") is not None:
            if self.optionsDict["presolve"]:
                # presolve is True: add 'presolve on'
                cmds += f"-presolve on "
            else:
                # presolve is False: add 'presolve off'
                cmds += f"-presolve off "
        if self.optionsDict.get("cuts") is not None:
            if self.optionsDict["cuts"]:
                # activate gomory, knapsack, and probing cuts
                cmds += f"-gomory on knapsack on probing on "
            else:
                # turn off all cuts
                cmds += f"-cuts off "
        options = self.options + self.getOptions()
        for option in options:
            cmds += "-" + option + " "
        if self.mip:
            cmds += "-branch "
        else:
            cmds += "-initialSolve "
        cmds += "-printingOptions all "
        cmds += "-solution " + tmpSol + " "
        logPath = self.optionsDict.get("logPath")
        if logPath:
            if self.msg:
                warnings.warn(
                    "`logPath` argument replaces `msg=1`. The output will be redirected to the log file."
                )
            pipe = open(self.optionsDict["logPath"], "w")
        else:
            pipe = self.get_pipe()
        log.debug(self.path + cmds)
        args = []
        args.append(self.path)
        args.extend(cmds[1:].split())
        if not self.msg and operating_system == "win":
            # Prevent flashing windows if used from a GUI application
            startupinfo = subprocess.STARTUPINFO()  # type: ignore[attr-defined,unused-ignore]
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW  # type: ignore[attr-defined,unused-ignore]
            cbc = subprocess.Popen(
                args, stdout=pipe, stderr=pipe, stdin=devnull, startupinfo=startupinfo
            )
        else:
            cbc = subprocess.Popen(args, stdout=pipe, stderr=pipe, stdin=devnull)
        if cbc.wait() != 0:
            if pipe:
                pipe.close()
            raise PulpSolverError(
                "Pulp: Error while trying to execute, use msg=True for more details"
                + self.path
            )
        try:
            pipe.close()
        except:
            pass

        if not os.path.exists(tmpSol):
            raise PulpSolverError("Pulp: Error while executing " + self.path)
        (
            status,
            values,
            reducedCosts,
            shadowPrices,
            slacks,
            sol_status,
        ) = self.readsol_MPS(tmpSol, lp, vs, variablesNames, constraintsNames)
        lp.assignVarsVals(values)
        lp.assignVarsDj(reducedCosts)
        lp.assignConsPi(shadowPrices)
        lp.assignConsSlack(slacks, activity=True)
        lp.assignStatus(status, sol_status)
        self.delete_tmp_files(tmpMps, tmpLp, tmpSol, tmpMst)
        return status

    def getOptions(self):
        params_eq = dict(
            gapRel="ratio {}",
            gapAbs="allow {}",
            threads="threads {}",
            strong="strong {}",
            timeMode="timeMode {}",
            maxNodes="maxNodes {}",
        )

        return [
            v.format(self.optionsDict[k])
            for k, v in params_eq.items()
            if self.optionsDict.get(k) is not None
        ]

    def readsol_MPS(
        self, filename, lp, vs, variablesNames, constraintsNames, objectiveName=None
    ):
        """
        Read a CBC solution file generated from an mps or lp file (possible different names)
        """
        values = {v.name: 0 for v in vs}

        reverseVn = {v: k for k, v in variablesNames.items()}
        reverseCn = {v: k for k, v in constraintsNames.items()}

        reducedCosts = {}
        shadowPrices = {}
        slacks = {}
        status, sol_status = self.get_status(filename)
        with open(filename) as f:
            for l in f:
                if len(l) <= 2:
                    break
                l = l.split()
                # incase the solution is infeasible
                if l[0] == "**":
                    l = l[1:]
                vn = l[1]
                val = l[2]
                dj = l[3]
                if vn in reverseVn:
                    values[reverseVn[vn]] = float(val)
                    reducedCosts[reverseVn[vn]] = float(dj)
                if vn in reverseCn:
                    slacks[reverseCn[vn]] = float(val)
                    shadowPrices[reverseCn[vn]] = float(dj)
        return status, values, reducedCosts, shadowPrices, slacks, sol_status

    def writesol(self, filename, lp, vs, variablesNames, constraintsNames):
        """
        Writes a CBC solution file generated from an mps / lp file (possible different names)
        returns True on success
        """
        values = {v.name: v.value() if v.value() is not None else 0 for v in vs}
        value_lines = []
        value_lines += [
            (i, v, values[k], 0) for i, (k, v) in enumerate(variablesNames.items())
        ]
        lines = ["Stopped on time - objective value 0\n"]
        lines += ["{:>7} {} {:>15} {:>23}\n".format(*tup) for tup in value_lines]

        with open(filename, "w") as f:
            f.writelines(lines)

        return True

    def readsol_LP(self, filename, lp, vs):
        """
        Read a CBC solution file generated from an lp (good names)
        returns status, values, reducedCosts, shadowPrices, slacks, sol_status
        """
        variablesNames = {v.name: v.name for v in vs}
        constraintsNames = {c: c for c in lp.constraints}
        return self.readsol_MPS(filename, lp, vs, variablesNames, constraintsNames)

    def get_status(self, filename):
        cbcStatus = {
            "Optimal": constants.LpStatusOptimal,
            "Infeasible": constants.LpStatusInfeasible,
            "Integer": constants.LpStatusInfeasible,
            "Unbounded": constants.LpStatusUnbounded,
            "Stopped": constants.LpStatusNotSolved,
        }

        cbcSolStatus = {
            "Optimal": constants.LpSolutionOptimal,
            "Infeasible": constants.LpSolutionInfeasible,
            "Unbounded": constants.LpSolutionUnbounded,
            "Stopped": constants.LpSolutionNoSolutionFound,
        }

        with open(filename) as f:
            statusstrs = f.readline().split()

        status = cbcStatus.get(statusstrs[0], constants.LpStatusUndefined)
        sol_status = cbcSolStatus.get(
            statusstrs[0], constants.LpSolutionNoSolutionFound
        )
        # here we could use some regex expression.
        # Not sure what's more desirable
        if status == constants.LpStatusNotSolved and len(statusstrs) >= 5:
            if statusstrs[4] == "objective":
                status = constants.LpStatusOptimal
                sol_status = constants.LpSolutionIntegerFeasible
        return status, sol_status


COIN = COIN_CMD


class PULP_CBC_CMD(COIN_CMD):
    """
    This solver uses a precompiled version of cbc provided with the package
    """

    name = "PULP_CBC_CMD"
    pulp_cbc_path = pulp_cbc_path
    try:
        if os.name != "nt":
            if not os.access(pulp_cbc_path, os.X_OK):
                import stat

                os.chmod(pulp_cbc_path, stat.S_IXUSR + stat.S_IXOTH)
    except:  # probably due to incorrect permissions

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp, callback=None):
            """Solve a well formulated lp problem"""
            raise PulpSolverError(
                "PULP_CBC_CMD: Not Available (check permissions on %s)"
                % self.pulp_cbc_path
            )

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            timeLimit=None,
            gapRel=None,
            gapAbs=None,
            presolve=None,
            cuts=None,
            strong=None,
            options=None,
            warmStart=False,
            keepFiles=False,
            path=None,
            threads=None,
            logPath=None,
            timeMode="elapsed",
            maxNodes=None,
        ):
            if path is not None:
                raise PulpSolverError("Use COIN_CMD if you want to set a path")
            # check that the file is executable
            COIN_CMD.__init__(
                self,
                path=self.pulp_cbc_path,
                mip=mip,
                msg=msg,
                timeLimit=timeLimit,
                gapRel=gapRel,
                gapAbs=gapAbs,
                presolve=presolve,
                cuts=cuts,
                strong=strong,
                options=options,
                warmStart=warmStart,
                keepFiles=keepFiles,
                threads=threads,
                logPath=logPath,
                timeMode=timeMode,
                maxNodes=maxNodes,
            )


def COINMP_DLL_load_dll(path: list[str]):
    """
    function that loads the DLL useful for debugging installation problems
    path is a list of paths actually
    """
    if os.name == "nt":
        lib = ctypes.windll.LoadLibrary(str(path[-1]))  # type: ignore[attr-defined,unused-ignore]
    else:
        # linux hack to get working
        mode = ctypes.RTLD_GLOBAL
        for libpath in path[:-1]:
            # RTLD_LAZY = 0x00001
            ctypes.CDLL(libpath, mode=mode)
        lib = ctypes.CDLL(path[-1], mode=mode)  # type: ignore[assignment,unused-ignore]
    return lib


class COINMP_DLL(LpSolver):
    """
    The COIN_MP LP MIP solver (via a DLL or linux so)

    :param timeLimit: The number of seconds before forcing the solver to exit
    :param epgap: The fractional mip tolerance
    """

    name = "COINMP_DLL"
    try:
        lib = COINMP_DLL_load_dll(coinMP_path)
    except (ImportError, OSError):

        @classmethod
        def available(cls):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp):
            """Solve a well formulated lp problem"""
            raise PulpSolverError("COINMP_DLL: Not Available")

    else:
        COIN_INT_LOGLEVEL = 7
        COIN_REAL_MAXSECONDS = 16
        COIN_REAL_MIPMAXSEC = 19
        COIN_REAL_MIPFRACGAP = 34
        lib.CoinGetInfinity.restype = ctypes.c_double
        lib.CoinGetVersionStr.restype = ctypes.c_char_p
        lib.CoinGetSolutionText.restype = ctypes.c_char_p
        lib.CoinGetObjectValue.restype = ctypes.c_double
        lib.CoinGetMipBestBound.restype = ctypes.c_double

        def __init__(
            self,
            cuts=1,
            presolve=1,
            dual=1,
            crash=0,
            scale=1,
            rounding=1,
            integerPresolve=1,
            strong=5,
            *args,
            **kwargs,
        ):
            LpSolver.__init__(self, *args, **kwargs)
            self.fracGap = None
            gapRel = self.optionsDict.get("gapRel")
            if gapRel is not None:
                self.fracGap = float(gapRel)
            if self.timeLimit is not None:
                self.timeLimit = float(self.timeLimit)
            # Todo: these options are not yet implemented
            self.cuts = cuts
            self.presolve = presolve
            self.dual = dual
            self.crash = crash
            self.scale = scale
            self.rounding = rounding
            self.integerPresolve = integerPresolve
            self.strong = strong

        def copy(self):
            """Make a copy of self"""
            aCopy = LpSolver.copy(self)
            aCopy.cuts = self.cuts
            aCopy.presolve = self.presolve
            aCopy.dual = self.dual
            aCopy.crash = self.crash
            aCopy.scale = self.scale
            aCopy.rounding = self.rounding
            aCopy.integerPresolve = self.integerPresolve
            aCopy.strong = self.strong
            return aCopy

        @classmethod
        def available(cls):
            """True if the solver is available"""
            return True

        def getSolverVersion(self):
            """
            returns a solver version string

            example:
            >>> COINMP_DLL().getSolverVersion() # doctest: +ELLIPSIS
            '...'
            """
            return self.lib.CoinGetVersionStr()

        def actualSolve(self, lp):
            """Solve a well formulated lp problem"""
            # TODO alter so that msg parameter is handled correctly
            self.debug = 0
            # initialise solver
            self.lib.CoinInitSolver("")
            # create problem
            self.hProb = hProb = self.lib.CoinCreateProblem(lp.name)
            # set problem options
            self.lib.CoinSetIntOption(
                hProb, self.COIN_INT_LOGLEVEL, ctypes.c_int(self.msg)
            )

            if self.timeLimit:
                if self.mip:
                    self.lib.CoinSetRealOption(
                        hProb, self.COIN_REAL_MIPMAXSEC, ctypes.c_double(self.timeLimit)
                    )
                else:
                    self.lib.CoinSetRealOption(
                        hProb,
                        self.COIN_REAL_MAXSECONDS,
                        ctypes.c_double(self.timeLimit),
                    )
            if self.fracGap:
                # Hopefully this is the bound gap tolerance
                self.lib.CoinSetRealOption(
                    hProb, self.COIN_REAL_MIPFRACGAP, ctypes.c_double(self.fracGap)
                )
            # CoinGetInfinity is needed for varibles with no bounds
            coinDblMax = self.lib.CoinGetInfinity()
            if self.debug:
                print("Before getCoinMPArrays")
            (
                numVars,
                numRows,
                numels,
                rangeCount,
                objectSense,
                objectCoeffs,
                objectConst,
                rhsValues,
                rangeValues,
                rowType,
                startsBase,
                lenBase,
                indBase,
                elemBase,
                lowerBounds,
                upperBounds,
                initValues,
                colNames,
                rowNames,
                columnType,
                n2v,
                n2c,
            ) = self.getCplexStyleArrays(lp)
            self.lib.CoinLoadProblem(
                hProb,
                numVars,
                numRows,
                numels,
                rangeCount,
                objectSense,
                objectConst,
                objectCoeffs,
                lowerBounds,
                upperBounds,
                rowType,
                rhsValues,
                rangeValues,
                startsBase,
                lenBase,
                indBase,
                elemBase,
                colNames,
                rowNames,
                "Objective",
            )
            if lp.isMIP() and self.mip:
                self.lib.CoinLoadInteger(hProb, columnType)

            if self.msg == 0:
                self.lib.CoinRegisterMsgLogCallback(
                    hProb, ctypes.c_char_p(""), ctypes.POINTER(ctypes.c_int)()
                )
            self.coinTime = -clock()
            self.lib.CoinOptimizeProblem(hProb, 0)
            self.coinTime += clock()

            # TODO: check Integer Feasible status
            CoinLpStatus = {
                0: constants.LpStatusOptimal,
                1: constants.LpStatusInfeasible,
                2: constants.LpStatusInfeasible,
                3: constants.LpStatusNotSolved,
                4: constants.LpStatusNotSolved,
                5: constants.LpStatusNotSolved,
                -1: constants.LpStatusUndefined,
            }
            solutionStatus = self.lib.CoinGetSolutionStatus(hProb)
            solutionText = self.lib.CoinGetSolutionText(hProb)
            objectValue = self.lib.CoinGetObjectValue(hProb)

            # get the solution values
            NumVarDoubleArray = ctypes.c_double * numVars
            NumRowsDoubleArray = ctypes.c_double * numRows
            cActivity = NumVarDoubleArray()
            cReducedCost = NumVarDoubleArray()
            cSlackValues = NumRowsDoubleArray()
            cShadowPrices = NumRowsDoubleArray()
            self.lib.CoinGetSolutionValues(
                hProb,
                ctypes.byref(cActivity),
                ctypes.byref(cReducedCost),
                ctypes.byref(cSlackValues),
                ctypes.byref(cShadowPrices),
            )

            variablevalues = {}
            variabledjvalues = {}
            constraintpivalues = {}
            constraintslackvalues = {}
            if lp.isMIP() and self.mip:
                lp.bestBound = self.lib.CoinGetMipBestBound(hProb)
            for i in range(numVars):
                variablevalues[self.n2v[i].name] = cActivity[i]
                variabledjvalues[self.n2v[i].name] = cReducedCost[i]
            lp.assignVarsVals(variablevalues)
            lp.assignVarsDj(variabledjvalues)
            # put pi and slack variables against the constraints
            for i in range(numRows):
                constraintpivalues[self.n2c[i]] = cShadowPrices[i]
                constraintslackvalues[self.n2c[i]] = cSlackValues[i]
            lp.assignConsPi(constraintpivalues)
            lp.assignConsSlack(constraintslackvalues)

            self.lib.CoinFreeSolver()
            status = CoinLpStatus[self.lib.CoinGetSolutionStatus(hProb)]
            lp.assignStatus(status)
            return status


if COINMP_DLL.available():
    COIN = COINMP_DLL  # type: ignore[assignment,misc]

yaposib = None


class YAPOSIB(LpSolver):
    """
    COIN OSI (via its python interface)

    Copyright Christophe-Marie Duquesne 2012

    The yaposib variables are available (after a solve) in var.solverVar
    The yaposib constraints are available in constraint.solverConstraint
    The Model is in prob.solverModel
    """

    name = "YAPOSIB"
    try:
        # import the model into the global scope
        global yaposib
        import yaposib  # type: ignore[import-not-found]
    except ImportError:

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp, callback=None):
            """Solve a well formulated lp problem"""
            raise PulpSolverError("YAPOSIB: Not Available")

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            timeLimit=None,
            epgap=None,
            solverName=None,
            **solverParams,
        ):
            """
            Initializes the yaposib solver.

            @param mip:          if False the solver will solve a MIP as
                                 an LP
            @param msg:          displays information from the solver to
                                 stdout
            @param timeLimit:    not supported
            @param epgap:        not supported
            @param solverParams: not supported
            """
            LpSolver.__init__(self, mip, msg)
            if solverName:
                self.solverName = solverName
            else:
                self.solverName = yaposib.available_solvers()[0]

        def findSolutionValues(self, lp):
            model = lp.solverModel
            solutionStatus = model.status
            yaposibLpStatus = {
                "optimal": constants.LpStatusOptimal,
                "undefined": constants.LpStatusUndefined,
                "abandoned": constants.LpStatusInfeasible,
                "infeasible": constants.LpStatusInfeasible,
                "limitreached": constants.LpStatusInfeasible,
            }
            # populate pulp solution values
            for var in lp.variables():
                var.varValue = var.solverVar.solution
                var.dj = var.solverVar.reducedcost
            # put pi and slack variables against the constraints
            for constr in lp.constraints.values():
                constr.pi = constr.solverConstraint.dual
                constr.slack = -constr.constant - constr.solverConstraint.activity
            if self.msg:
                print("yaposib status=", solutionStatus)
            lp.resolveOK = True
            for var in lp.variables():
                var.isModified = False
            status = yaposibLpStatus.get(solutionStatus, constants.LpStatusUndefined)
            lp.assignStatus(status)
            return status

        def available(self):
            """True if the solver is available"""
            return True

        def callSolver(self, lp, callback=None):
            """Solves the problem with yaposib"""
            savestdout = None
            if self.msg == 0:
                # close stdout to get rid of messages
                tempfile = open(mktemp(), "w")
                savestdout = os.dup(1)
                os.close(1)
                if os.dup(tempfile.fileno()) != 1:
                    raise PulpSolverError("couldn't redirect stdout - dup() error")
            self.solveTime = -clock()
            lp.solverModel.solve(self.mip)
            self.solveTime += clock()
            if self.msg == 0:
                # reopen stdout
                os.close(1)
                os.dup(savestdout)
                os.close(savestdout)

        def buildSolverModel(self, lp):
            """
            Takes the pulp lp model and translates it into a yaposib model
            """
            log.debug("create the yaposib model")
            lp.solverModel = yaposib.Problem(self.solverName)
            prob = lp.solverModel
            prob.name = lp.name
            log.debug("set the sense of the problem")
            if lp.sense == constants.LpMaximize:
                prob.obj.maximize = True
            log.debug("add the variables to the problem")
            for var in lp.variables():
                col = prob.cols.add(yaposib.vec([]))
                col.name = var.name
                if not var.lowBound is None:
                    col.lowerbound = var.lowBound
                if not var.upBound is None:
                    col.upperbound = var.upBound
                if var.cat == constants.LpInteger:
                    col.integer = True
                prob.obj[col.index] = lp.objective.get(var, 0.0)
                var.solverVar = col
            log.debug("add the Constraints to the problem")
            for name, constraint in lp.constraints.items():
                row = prob.rows.add(
                    yaposib.vec(
                        [
                            (var.solverVar.index, value)
                            for var, value in constraint.items()
                        ]
                    )
                )
                if constraint.sense == constants.LpConstraintLE:
                    row.upperbound = -constraint.constant
                elif constraint.sense == constants.LpConstraintGE:
                    row.lowerbound = -constraint.constant
                elif constraint.sense == constants.LpConstraintEQ:
                    row.upperbound = -constraint.constant
                    row.lowerbound = -constraint.constant
                else:
                    raise PulpSolverError("Detected an invalid constraint type")
                row.name = name
                constraint.solverConstraint = row

        def actualSolve(self, lp, callback=None):
            """
            Solve a well formulated lp problem

            creates a yaposib model, variables and constraints and attaches
            them to the lp model which it then solves
            """
            self.buildSolverModel(lp)
            # set the initial solution
            log.debug("Solve the model using yaposib")
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
            log.debug("Resolve the model using yaposib")
            for constraint in lp.constraints.values():
                row = constraint.solverConstraint
                if constraint.modified:
                    if constraint.sense == constants.LpConstraintLE:
                        row.upperbound = -constraint.constant
                    elif constraint.sense == constants.LpConstraintGE:
                        row.lowerbound = -constraint.constant
                    elif constraint.sense == constants.LpConstraintEQ:
                        row.upperbound = -constraint.constant
                        row.lowerbound = -constraint.constant
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


cy = None


class CYLP(LpSolver):
    # https://github.com/coin-or/CyLP
    name = "CyLP"
    try:
        global cy
        from cylp import cy  # type: ignore[import-not-found, import-untyped, unused-ignore]
    except:

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp, callback=None):
            """Solve a well formulated lp problem"""
            raise PulpSolverError("CyLP: Not Available")

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            gapAbs=None,
            gapRel=None,
            threads=None,
            timeLimit=None,
            **solverParams,
        ):
            """
            :param bool mip: if False, assume LP even if integer variables
            :param bool msg: if False, no log is shown
            :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
            :param float gapAbs: absolute gap tolerance for the solver to stop
            :param int threads: sets the maximum number of threads
            :param float timeLimit: maximum time for solver (in seconds)
            :param dict solverParams: list of named options to pass directly to the HiGHS solver
            """
            super().__init__(mip=mip, msg=msg, timeLimit=timeLimit, **solverParams)
            self.gapAbs = gapAbs
            self.gapRel = gapRel
            self.threads = threads

        def actualSolve(self, lp, **kwargs):  # type: ignore[misc]
            self.buildSolverModel(lp)
            my_status = self.callSolver(lp)
            # get the solution information
            self.findSolutionValues(lp)

            my_map = {
                0: constants.LpStatusOptimal,
                1: constants.LpStatusInfeasible,
                2: constants.LpStatusInfeasible,
                4: constants.LpStatusNotSolved,
                5: constants.LpStatusUndefined,
                -1: constants.LpStatusUndefined,
            }
            sol_stats = lp.solverModel.status
            print(sol_stats)
            my_map_2 = {
                "linear relaxation unbounded": constants.LpStatusInfeasible,
                "relaxation infeasible": constants.LpStatusInfeasible,
                "solution": constants.LpStatusOptimal,
                "problem proven infeasible": constants.LpStatusInfeasible,
                "relaxation abandoned": constants.LpStatusNotSolved,
            }

            return my_map_2.get(sol_stats, constants.LpStatusUndefined)

        def findSolutionValues(self, lp):
            my_vars = lp.variables()
            my_values = lp.solverModel.primalVariableSolution
            for var, value in zip(my_vars, my_values):
                var.varValue = value
            # status = lp.solverModel.getStatusCode()
            # -1 - unknown e.g. before solve or if postSolve says not optimal

            # 0 - optimal
            # 1 - primal infeasible
            # 2 - dual infeasible
            # 3 - stopped on iterations or time
            # 4 - stopped due to errors
            # 5 - stopped by event handler (virtual int ClpEventHandler::event())
            # lp.solverModel.primalVariableSolution
            # variables
            # solution
            # reducedCosts
            pass

        def available(self):
            return True

        def callSolver(self, lp):
            self.solveTime = -clock()
            if self.timeLimit:
                lp.solverModel.maximumSeconds = self.timeLimit
            if self.gapRel:
                lp.solverModel.allowableFractionGap = self.gapRel
            if self.gapAbs:
                lp.solverModel.allowableGap = self.gapAbs
            if self.threads:
                lp.solverModel.numberThreads = self.threads

            if max_nodes := self.optionsDict.get("maxNodes"):
                lp.solverModel.maximumNodes = max_nodes

            stop_status = lp.solverModel.solve()
            self.solveTime += clock()
            return stop_status

        def buildSolverModel(self, lp):

            my_model = cy.CyClpSimplex()
            tmpMps = f"{lp.name}-pulp.mps"
            lp.writeMPS(tmpMps)
            success = my_model.readMps(tmpMps)
            try:
                os.remove(tmpMps)
            except:
                pass
            if success != 0:
                raise PulpSolverError("Error reading MPS file")
            cbc_model = my_model.getCbcModel()
            # my_model.initialSolve()
            # cbc_model.solve()
            # cbc_model.status
            # my_model.primalVariableSolution
            # my_model.variableNames
            # my_model.variables
            # my_model.readMps("test.mps")
            # my_model.solution
            # my_model.status
            # my_model.getStatusCode()
            # my_model.getStatusString()
            lp.solverModel = cbc_model

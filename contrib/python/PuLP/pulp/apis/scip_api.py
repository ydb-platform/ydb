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

import operator
import os
import sys
import warnings
from typing import Dict, List, Optional, Tuple

from .. import constants
from .core import LpSolver, LpSolver_CMD, PulpSolverError, subprocess

scip_path = "scip"
fscip_path = "fscip"


class SCIP_CMD(LpSolver_CMD):
    """The SCIP optimization solver"""

    name = "SCIP_CMD"

    def __init__(
        self,
        path=None,
        mip=True,
        keepFiles=False,
        msg=True,
        options=None,
        timeLimit=None,
        gapRel=None,
        gapAbs=None,
        maxNodes=None,
        logPath=None,
        threads=None,
    ):
        """
        :param bool mip: if False, assume LP even if integer variables
        :param bool msg: if False, no log is shown
        :param list options: list of additional options to pass to solver
        :param bool keepFiles: if True, files are saved in the current directory and not deleted after solving
        :param str path: path to the solver binary
        :param float timeLimit: maximum time for solver (in seconds)
        :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
        :param float gapAbs: absolute gap tolerance for the solver to stop
        :param int maxNodes: max number of nodes during branching. Stops the solving when reached.
        :param int threads: sets the maximum number of threads
        :param str logPath: path to the log file
        """
        LpSolver_CMD.__init__(
            self,
            mip=mip,
            msg=msg,
            options=options,
            path=path,
            keepFiles=keepFiles,
            timeLimit=timeLimit,
            gapRel=gapRel,
            gapAbs=gapAbs,
            maxNodes=maxNodes,
            threads=threads,
            logPath=logPath,
        )

    SCIP_STATUSES = {
        "unknown": constants.LpStatusUndefined,
        "user interrupt": constants.LpStatusNotSolved,
        "node limit reached": constants.LpStatusNotSolved,
        "total node limit reached": constants.LpStatusNotSolved,
        "stall node limit reached": constants.LpStatusNotSolved,
        "time limit reached": constants.LpStatusNotSolved,
        "memory limit reached": constants.LpStatusNotSolved,
        "gap limit reached": constants.LpStatusOptimal,
        "solution limit reached": constants.LpStatusNotSolved,
        "solution improvement limit reached": constants.LpStatusNotSolved,
        "restart limit reached": constants.LpStatusNotSolved,
        "optimal solution found": constants.LpStatusOptimal,
        "infeasible": constants.LpStatusInfeasible,
        "unbounded": constants.LpStatusUnbounded,
        "infeasible or unbounded": constants.LpStatusNotSolved,
    }
    NO_SOLUTION_STATUSES = {
        constants.LpStatusInfeasible,
        constants.LpStatusUnbounded,
        constants.LpStatusNotSolved,
    }

    def defaultPath(self):
        return self.executableExtension(scip_path)

    def available(self):
        """True if the solver is available"""
        return self.executable(self.path)

    def actualSolve(self, lp):
        """Solve a well formulated lp problem"""
        if not self.executable(self.path):
            raise PulpSolverError("PuLP: cannot execute " + self.path)

        tmpLp, tmpSol, tmpOptions = self.create_tmp_files(lp.name, "lp", "sol", "set")
        lp.writeLP(tmpLp)

        file_options: List[str] = []  # type: ignore[annotation-unchecked]
        if self.timeLimit is not None:
            file_options.append(f"limits/time={self.timeLimit}")
        if "gapRel" in self.optionsDict:
            file_options.append(f"limits/gap={self.optionsDict['gapRel']}")
        if "gapAbs" in self.optionsDict:
            file_options.append(f"limits/absgap={self.optionsDict['gapAbs']}")
        if "maxNodes" in self.optionsDict:
            file_options.append(f"limits/nodes={self.optionsDict['maxNodes']}")
        if "threads" in self.optionsDict and int(self.optionsDict["threads"]) > 1:
            warnings.warn(
                "SCIP can only run with a single thread - use FSCIP_CMD for a parallel version of SCIP"
            )
        if not self.mip:
            warnings.warn(f"{self.name} does not allow a problem to be relaxed")

        command: List[str] = []  # type: ignore[annotation-unchecked]
        command.append(self.path)
        command.extend(["-s", tmpOptions])
        if not self.msg:
            command.append("-q")
        if "logPath" in self.optionsDict:
            command.extend(["-l", self.optionsDict["logPath"]])

        options = iter(self.options)
        for option in options:
            # identify cli options by a leading dash (-) and treat other options as file options
            if option.startswith("-"):
                # assumption: all cli options require an argument which is provided as a separate parameter
                argument = next(options)
                command.extend([option, argument])
            else:
                # assumption: all file options require an argument which is provided after the equal sign (=)
                if "=" not in option:
                    argument = next(options)
                    option += f"={argument}"
                file_options.append(option)

        # append scip commands after parsing self.options to allow the user to specify additional -c arguments
        command.extend(["-c", f'read "{tmpLp}"'])
        command.extend(["-c", "optimize"])
        command.extend(["-c", f'write solution "{tmpSol}"'])
        command.extend(["-c", "quit"])

        with open(tmpOptions, "w") as options_file:
            options_file.write("\n".join(file_options))

        pipe = self.get_pipe()
        subprocess.check_call(command, stdout=pipe, stderr=pipe)

        # Close the pipe now if we used it.
        if pipe is not None:
            pipe.close()

        if not os.path.exists(tmpSol):
            raise PulpSolverError("PuLP: Error while executing " + self.path)
        status, values = self.readsol(tmpSol)
        # Make sure to add back in any 0-valued variables SCIP leaves out.
        finalVals = {}
        for v in lp.variables():
            finalVals[v.name] = values.get(v.name, 0.0)

        lp.assignVarsVals(finalVals)
        lp.assignStatus(status)
        self.delete_tmp_files(tmpLp, tmpSol, tmpOptions)
        return status

    @staticmethod
    def readsol(filename):
        """Read a SCIP solution file"""
        with open(filename) as f:
            # First line must contain 'solution status: <something>'
            try:
                line = f.readline()
                comps = line.split(": ")
                assert comps[0] == "solution status"
                assert len(comps) == 2
            except Exception:
                raise PulpSolverError(f"Can't get SCIP solver status: {line!r}")

            status = SCIP_CMD.SCIP_STATUSES.get(
                comps[1].strip(), constants.LpStatusUndefined
            )
            values = {}

            # Look for an objective value. If we can't find one, stop.
            try:
                line = f.readline()
                comps = line.split(": ")
                assert comps[0] == "objective value"
                assert len(comps) == 2
                float(comps[1].strip())
            except Exception:
                # we assume there was not solution found
                return status, values

            # Parse the variable values.
            for line in f:
                try:
                    comps = line.split()
                    values[comps[0]] = float(comps[1])
                except:
                    raise PulpSolverError(f"Can't read SCIP solver output: {line!r}")

            # if we have a solution, we should change status to Optimal by conventio
            status = constants.LpStatusOptimal

            return status, values


SCIP = SCIP_CMD


class FSCIP_CMD(LpSolver_CMD):
    """The multi-threaded FiberSCIP version of the SCIP optimization solver"""

    name = "FSCIP_CMD"

    def __init__(
        self,
        path=None,
        mip=True,
        keepFiles=False,
        msg=True,
        options=None,
        timeLimit=None,
        gapRel=None,
        gapAbs=None,
        maxNodes=None,
        threads=None,
        logPath=None,
    ):
        """
        :param bool msg: if False, no log is shown
        :param bool mip: if False, assume LP even if integer variables
        :param list options: list of additional options to pass to solver
        :param bool keepFiles: if True, files are saved in the current directory and not deleted after solving
        :param str path: path to the solver binary
        :param float timeLimit: maximum time for solver (in seconds)
        :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
        :param float gapAbs: absolute gap tolerance for the solver to stop
        :param int maxNodes: max number of nodes during branching. Stops the solving when reached.
        :param int threads: sets the maximum number of threads
        :param str logPath: path to the log file
        """
        LpSolver_CMD.__init__(
            self,
            mip=mip,
            msg=msg,
            options=options,
            path=path,
            keepFiles=keepFiles,
            timeLimit=timeLimit,
            gapRel=gapRel,
            gapAbs=gapAbs,
            maxNodes=maxNodes,
            threads=threads,
            logPath=logPath,
        )

    FSCIP_STATUSES = {
        "No Solution": constants.LpStatusNotSolved,
        "Final Solution": constants.LpStatusOptimal,
    }
    NO_SOLUTION_STATUSES = {
        constants.LpStatusInfeasible,
        constants.LpStatusUnbounded,
        constants.LpStatusNotSolved,
    }

    def defaultPath(self):
        return self.executableExtension(fscip_path)

    def available(self):
        """True if the solver is available"""
        return self.executable(self.path)

    def actualSolve(self, lp):
        """Solve a well formulated lp problem"""
        if not self.executable(self.path):
            raise PulpSolverError("PuLP: cannot execute " + self.path)

        tmpLp, tmpSol, tmpOptions, tmpParams = self.create_tmp_files(
            lp.name, "lp", "sol", "set", "prm"
        )
        lp.writeLP(tmpLp)

        file_options: List[str] = []  # type: ignore[annotation-unchecked]
        if self.timeLimit is not None:
            file_options.append(f"limits/time={self.timeLimit}")
        if "gapRel" in self.optionsDict:
            file_options.append(f"limits/gap={self.optionsDict['gapRel']}")
        if "gapAbs" in self.optionsDict:
            file_options.append(f"limits/absgap={self.optionsDict['gapAbs']}")
        if "maxNodes" in self.optionsDict:
            file_options.append(f"limits/nodes={self.optionsDict['maxNodes']}")
        if not self.mip:
            warnings.warn(f"{self.name} does not allow a problem to be relaxed")

        file_parameters: List[str] = []  # type: ignore[annotation-unchecked]
        # disable presolving in the LoadCoordinator to make sure a solution file is always written
        file_parameters.append("NoPreprocessingInLC = TRUE")

        command: List[str] = []  # type: ignore[annotation-unchecked]
        command.append(self.path)
        command.append(tmpParams)
        command.append(tmpLp)
        command.extend(["-s", tmpOptions])
        command.extend(["-fsol", tmpSol])
        if not self.msg:
            command.append("-q")
        if "logPath" in self.optionsDict:
            command.extend(["-l", self.optionsDict["logPath"]])
        if "threads" in self.optionsDict:
            command.extend(["-sth", f"{self.optionsDict['threads']}"])

        options = iter(self.options)
        for option in options:
            # identify cli options by a leading dash (-) and treat other options as file options
            if option.startswith("-"):
                # assumption: all cli options require an argument which is provided as a separate parameter
                argument = next(options)
                command.extend([option, argument])
            else:
                # assumption: all file options contain a slash (/)
                is_file_options = "/" in option

                # assumption: all file options and parameters require an argument which is provided after the equal sign (=)
                if "=" not in option:
                    argument = next(options)
                    option += f"={argument}"

                if is_file_options:
                    file_options.append(option)
                else:
                    file_parameters.append(option)

        # wipe the solution file since FSCIP does not overwrite it if no solution was found which causes parsing errors
        self.silent_remove(tmpSol)
        with open(tmpOptions, "w") as options_file:
            options_file.write("\n".join(file_options))
        with open(tmpParams, "w") as parameters_file:
            parameters_file.write("\n".join(file_parameters))

        pipe = self.get_pipe()
        subprocess.check_call(command, stdout=pipe, stderr=pipe)

        if pipe is not None:
            pipe.close()

        if not os.path.exists(tmpSol):
            raise PulpSolverError("PuLP: Error while executing " + self.path)
        status, values = self.readsol(tmpSol)
        # Make sure to add back in any 0-valued variables SCIP leaves out.
        finalVals = {}
        for v in lp.variables():
            finalVals[v.name] = values.get(v.name, 0.0)

        lp.assignVarsVals(finalVals)
        lp.assignStatus(status)
        self.delete_tmp_files(tmpLp, tmpSol, tmpOptions, tmpParams)
        return status

    @staticmethod
    def parse_status(string: str) -> Optional[int]:
        for fscip_status, pulp_status in FSCIP_CMD.FSCIP_STATUSES.items():
            if fscip_status in string:
                return pulp_status
        return None

    @staticmethod
    def parse_objective(string: str) -> Optional[float]:
        fields = string.split(":")
        if len(fields) != 2:
            return None

        label, objective = fields
        if label != "objective value":
            return None

        objective = objective.strip()
        try:
            objective = float(objective)  # type: ignore[assignment]
        except ValueError:
            return None

        return objective  # type: ignore[return-value]

    @staticmethod
    def parse_variable(string: str) -> Optional[Tuple[str, float]]:
        fields = string.split()
        if len(fields) < 2:
            return None

        name, value = fields[:2]
        try:
            value = float(value)  # type: ignore[assignment]
        except ValueError:
            return None

        return name, value  # type: ignore[return-value]

    @staticmethod
    def readsol(filename):
        """Read a FSCIP solution file"""
        with open(filename) as file:
            # First line must contain a solution status
            status_line = file.readline()
            status = FSCIP_CMD.parse_status(status_line)
            if status is None:
                raise PulpSolverError(f"Can't get FSCIP solver status: {status_line!r}")

            if status in FSCIP_CMD.NO_SOLUTION_STATUSES:
                return status, {}

            # Look for an objective value. If we can't find one, stop.
            objective_line = file.readline()
            objective = FSCIP_CMD.parse_objective(objective_line)
            if objective is None:
                raise PulpSolverError(
                    f"Can't get FSCIP solver objective: {objective_line!r}"
                )

            # Parse the variable values.
            variables: Dict[str, float] = {}  # type: ignore[annotation-unchecked]
            for variable_line in file:
                variable = FSCIP_CMD.parse_variable(variable_line)
                if variable is None:
                    raise PulpSolverError(
                        f"Can't read FSCIP solver output: {variable_line!r}"
                    )

                name, value = variable
                variables[name] = value

            return status, variables


FSCIP = FSCIP_CMD


class SCIP_PY(LpSolver):
    """
    The SCIP Optimization Suite (via its python interface)

    The SCIP internals are available after calling solve as:
    - each variable in variable.solverVar
    - each constraint in constraint.solverConstraint
    - the model in problem.solverModel
    """

    name = "SCIP_PY"

    try:
        global scip
        import pyscipopt as scip  # type: ignore[import-not-found, import-untyped, unused-ignore]

    except ImportError:

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp):
            """Solve a well formulated lp problem"""
            raise PulpSolverError(f"The {self.name} solver is not available")

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            options=None,
            timeLimit=None,
            gapRel=None,
            gapAbs=None,
            maxNodes=None,
            logPath=None,
            threads=None,
            warmStart=False,
        ):
            """
            :param bool mip: if False, assume LP even if integer variables
            :param bool msg: if False, no log is shown
            :param list options: list of additional options to pass to solver
            :param float timeLimit: maximum time for solver (in seconds)
            :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
            :param float gapAbs: absolute gap tolerance for the solver to stop
            :param int maxNodes: max number of nodes during branching. Stops the solving when reached.
            :param str logPath: path to the log file
            :param int threads: sets the maximum number of threads
            :param bool warmStart: if True, the solver will use the current value of variables as a start
            """
            super().__init__(
                mip=mip,
                msg=msg,
                options=options,
                timeLimit=timeLimit,
                gapRel=gapRel,
                gapAbs=gapAbs,
                maxNodes=maxNodes,
                logPath=logPath,
                threads=threads,
                warmStart=warmStart,
            )

        def findSolutionValues(self, lp):
            lp.resolveOK = True

            solutionStatus = lp.solverModel.getStatus()
            scip_to_pulp_status = {
                "optimal": constants.LpStatusOptimal,
                "unbounded": constants.LpStatusUnbounded,
                "infeasible": constants.LpStatusInfeasible,
                "inforunbd": constants.LpStatusNotSolved,
                "timelimit": constants.LpStatusNotSolved,
                "userinterrupt": constants.LpStatusNotSolved,
                "nodelimit": constants.LpStatusNotSolved,
                "totalnodelimit": constants.LpStatusNotSolved,
                "stallnodelimit": constants.LpStatusNotSolved,
                "gaplimit": constants.LpStatusNotSolved,
                "memlimit": constants.LpStatusNotSolved,
                "sollimit": constants.LpStatusNotSolved,
                "bestsollimit": constants.LpStatusNotSolved,
                "restartlimit": constants.LpStatusNotSolved,
                "unknown": constants.LpStatusUndefined,
            }
            possible_solution_found_statuses = (
                "optimal",
                "timelimit",
                "userinterrupt",
                "nodelimit",
                "totalnodelimit",
                "stallnodelimit",
                "gaplimit",
                "memlimit",
            )
            status = scip_to_pulp_status[solutionStatus]

            if solutionStatus in possible_solution_found_statuses:
                try:  # Feasible solution found
                    solution = lp.solverModel.getBestSol()
                    for variable in lp._variables:
                        variable.varValue = solution[variable.solverVar]
                    for constraint in lp.constraints.values():
                        constraint.slack = lp.solverModel.getSlack(
                            constraint.solverConstraint, solution
                        )
                    if status == constants.LpStatusOptimal:
                        lp.assignStatus(status, constants.LpSolutionOptimal)
                    else:
                        status = constants.LpStatusOptimal
                        lp.assignStatus(status, constants.LpSolutionIntegerFeasible)
                except:  # No solution found
                    lp.assignStatus(status, constants.LpSolutionNoSolutionFound)
            else:
                lp.assignStatus(status)

                # TODO: check if problem is an LP i.e. does not have integer variables
                # if :
                #     for variable in lp._variables:
                #         variable.dj = lp.solverModel.getVarRedcost(variable.solverVar)
                #     for constraint in lp.constraints.values():
                #         constraint.pi = lp.solverModel.getDualSolVal(constraint.solverConstraint)

            return status

        def available(self):
            """True if the solver is available"""
            # if pyscipopt can be installed (and therefore imported) it has access to scip
            return True

        def callSolver(self, lp):
            """Solves the problem with scip"""
            lp.solverModel.optimize()

        def buildSolverModel(self, lp):
            """
            Takes the pulp lp model and translates it into a scip model
            """
            ##################################################
            # create model
            ##################################################
            lp.solverModel = scip.Model(lp.name)
            if lp.sense == constants.LpMaximize:
                lp.solverModel.setMaximize()
            else:
                lp.solverModel.setMinimize()

            ##################################################
            # add options
            ##################################################
            if not self.msg:
                lp.solverModel.hideOutput()
            if self.timeLimit is not None:
                lp.solverModel.setParam("limits/time", self.timeLimit)
            if "gapRel" in self.optionsDict:
                lp.solverModel.setParam("limits/gap", self.optionsDict["gapRel"])
            if "gapAbs" in self.optionsDict:
                lp.solverModel.setParam("limits/absgap", self.optionsDict["gapAbs"])
            if "maxNodes" in self.optionsDict:
                lp.solverModel.setParam("limits/nodes", self.optionsDict["maxNodes"])
            if "logPath" in self.optionsDict:
                lp.solverModel.setLogfile(self.optionsDict["logPath"])
            if "threads" in self.optionsDict and int(self.optionsDict["threads"]) > 1:
                warnings.warn(
                    f"The solver {self.name} can only run with a single thread"
                )
            if not self.mip:
                warnings.warn(f"{self.name} does not allow a problem to be relaxed")

            options = iter(self.options)
            for option in options:
                # assumption: all file options require an argument which is provided after the equal sign (=)
                if "=" in option:
                    name, value = option.split("=", maxsplit=2)
                else:
                    name, value = option, next(options)
                lp.solverModel.setParam(name, value)

            ##################################################
            # add variables
            ##################################################
            category_to_vtype = {
                constants.LpBinary: "B",
                constants.LpContinuous: "C",
                constants.LpInteger: "I",
            }
            for var in lp.variables():
                var.solverVar = lp.solverModel.addVar(
                    name=var.name,
                    vtype=category_to_vtype[var.cat],
                    lb=var.lowBound,  # a lower bound of None represents -infinity
                    ub=var.upBound,  # an upper bound of None represents +infinity
                    obj=lp.objective.get(var, 0.0),
                )

            ##################################################
            # add constraints
            ##################################################
            sense_to_operator = {
                constants.LpConstraintLE: operator.le,
                constants.LpConstraintGE: operator.ge,
                constants.LpConstraintEQ: operator.eq,
            }
            for name, constraint in lp.constraints.items():
                constraint.solverConstraint = lp.solverModel.addCons(
                    cons=sense_to_operator[constraint.sense](
                        scip.quicksum(
                            coefficient * variable.solverVar
                            for variable, coefficient in constraint.items()
                        ),
                        -constraint.constant,
                    ),
                    name=name,
                )

            ##################################################
            # add warm start
            ##################################################
            if self.optionsDict.get("warmStart", False):
                s = lp.solverModel.createPartialSol()
                for var in lp.variables():
                    if var.varValue is not None:
                        # Warm start variables having an initial value
                        lp.solverModel.setSolVal(s, var.solverVar, var.varValue)
                lp.solverModel.addSol(s)

        def actualSolve(self, lp):
            """
            Solve a well formulated lp problem

            creates a scip model, variables and constraints and attaches
            them to the lp model which it then solves
            """
            self.buildSolverModel(lp)
            self.callSolver(lp)
            solutionStatus = self.findSolutionValues(lp)
            for variable in lp._variables:
                variable.modified = False
            for constraint in lp.constraints.values():
                constraint.modified = False
            return solutionStatus

        def actualResolve(self, lp):
            """
            Solve a well formulated lp problem

            uses the old solver and modifies the rhs of the modified constraints
            """
            # TODO: add ability to resolve pysciptopt models
            # - http://listserv.zib.de/pipermail/scip/2020-May/003977.html
            # - https://scipopt.org/doc-8.0.0/html/REOPT.php
            raise PulpSolverError(
                f"The {self.name} solver does not implement resolving"
            )

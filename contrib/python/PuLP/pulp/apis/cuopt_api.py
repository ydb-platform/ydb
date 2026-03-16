import ctypes
import os
import subprocess
import sys
import warnings
from uuid import uuid4
from ..constants import (
    LpBinary,
    LpConstraintEQ,
    LpConstraintGE,
    LpConstraintLE,
    LpContinuous,
    LpInteger,
    LpMaximize,
    LpMinimize,
    LpStatusInfeasible,
    LpStatusNotSolved,
    LpStatusOptimal,
    LpStatusUnbounded,
    LpStatusUndefined,
)
from .core import (
    LpSolver,
    LpSolver_CMD,
    PulpSolverError,
    clock,
    ctypesArrayFill,
    sparse,
)

# Constraint Sense Converter
sense_conv = {
    LpConstraintLE: "L",
    LpConstraintGE: "G",
    LpConstraintEQ: "E",
}


class CUOPT(LpSolver):
    """
    The CUOPT Optimizer via its python interface
    """

    name = "CUOPT"

    try:
        global cuopt
        import cuopt  # type: ignore[import-not-found, import-untyped, unused-ignore]

        global np
        import numpy as np  # type: ignore[import-not-found, import-untyped, unused-ignore]
    except:

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp, callback=None):
            """Solve a well formulated lp problem"""
            raise PulpSolverError("CUOPT: Not available")

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            timeLimit=None,
            gapRel=None,
            warmStart=False,
            logPath=None,
            **solverParams,
        ):
            """
            :param bool mip: if False, assume LP even if integer variables
            :param bool msg: if False, no log is shown
            :param float timeLimit: maximum time for solver (in seconds)
            :param float gapRel: relative gap tolerance for the solver to stop (in fraction)
            :param bool warmStart: if True, the solver will use the current value of variables as a start
            :param str logPath: path to the log file
            :param solverParams: solver setting paramters for cuopt
            """

            LpSolver.__init__(
                self,
                mip=mip,
                msg=msg,
                timeLimit=timeLimit,
                gapRel=gapRel,
                logPath=logPath,
                warmStart=warmStart,
            )

            from cuopt.linear_programming import data_model  # type: ignore[import-not-found, import-untyped, unused-ignore]

            self.model = data_model.DataModel()
            self.var_list = None
            self.solver_params = solverParams

        def findSolutionValues(self, lp, solution):
            solutionStatus = solution.get_termination_status()
            if self.msg:
                print("CUOPT status=", solution.get_termination_reason())

            CuoptStatus = {
                0: LpStatusNotSolved,  # No Termination
                1: LpStatusOptimal,  # Optimal
                2: LpStatusInfeasible,  # Infeasible
                3: LpStatusUnbounded,  # Unbounded
                4: LpStatusNotSolved,  # Iteration Limit
                5: LpStatusNotSolved,  # Timelimit
                6: LpStatusNotSolved,  # Numerical Error
                7: LpStatusNotSolved,  # Primal Feasible
                8: LpStatusNotSolved,  # Feasible Found
                9: LpStatusNotSolved,  # Concurrent Limit
            }

            lp.resolveOK = True
            for var in lp._variables:
                var.isModified = False

            status = CuoptStatus.get(solutionStatus, LpStatusUndefined)
            lp.assignStatus(status)

            values = solution.get_primal_solution()

            for var, value in zip(lp._variables, values):
                var.varValue = value

            if not solution.get_problem_category():
                # TODO: Compute Slack

                redcosts = solution.get_reduced_cost()
                for var, value in zip(lp._variables, redcosts):
                    var.dj = value

                duals = solution.get_dual_solution()
                for constr, value in zip(lp.constraints.values(), duals):
                    constr.pi = value

            return status

        def available(self):
            """True if the solver is available"""
            return True

        def callSolver(self, lp, callback=None):
            """Solves the problem with CUOPT"""
            from cuopt.linear_programming import solver_settings, solver  # type: ignore[import-not-found, import-untyped, unused-ignore]

            self.solveTime = -clock()
            # TODO: Add callback
            log_file = self.optionsDict.get("logPath") or ""

            settings = solver_settings.SolverSettings()
            settings.set_parameter("infeasibility_detection", True)
            settings.set_parameter("log_to_console", self.msg)
            if log_file:
                settings.set_parameter("log_file", log_file)
            if self.timeLimit:
                settings.set_parameter("time_limit", self.timeLimit)
            for key, value in self.solver_params.items():
                if key == "optimality_tolerance":
                    settings.set_optimality_tolerance(value)
            gapRel = self.optionsDict.get("gapRel")
            if gapRel:
                settings.set_parameter("relative_gap_tolerance", gapRel)
            solution = solver.Solve(lp.solverModel, settings)

            self.solveTime += clock()
            return solution

        def buildSolverModel(self, lp):
            """
            Takes the pulp lp model and translates it into a COPT model
            """
            lp.solverModel = self.model

            if lp.sense == LpMaximize:
                lp.solverModel.set_maximize(True)

            var_lb, var_ub, var_type, var_name = [], [], [], []
            obj_coeff = []
            var_dict = {}

            for i, var in enumerate(lp.variables()):
                obj_coeff.append(lp.objective.get(var, 0.0))
                lowBound = var.lowBound
                if lowBound is None:
                    lowBound = -np.inf
                upBound = var.upBound
                if upBound is None:
                    upBound = np.inf
                varType = "C"
                if var.cat == LpInteger and self.mip:
                    varType = "I"
                if var.cat == LpBinary and self.mip:
                    varType = "I"
                    lowBound = 0
                    upBound = 1
                var_lb.append(lowBound)
                var_ub.append(upBound)
                var_type.append(varType)
                var_name.append(var.name)
                var_dict[var.name] = i
                var.solverVar = {
                    var.name: {"lb": var_lb, "ub": var_ub, "type": var_type}
                }
            lp.solverModel.set_variable_lower_bounds(np.array(var_lb))
            lp.solverModel.set_variable_upper_bounds(np.array(var_ub))
            lp.solverModel.set_variable_types(np.array(var_type))
            lp.solverModel.set_variable_names(np.array(var_name))

            rhs, sense = [], []
            matrix_data, matrix_indices, matrix_indptr = [], [], [0]

            for name, constraint in lp.constraints.items():
                row_coeffs = []
                matrix_data.extend(list(constraint.values()))
                matrix_indices.extend([var_dict[v.name] for v in constraint.keys()])
                matrix_indptr.append(len(matrix_data))
                try:
                    c_sense = sense_conv[constraint.sense]
                except:
                    raise PulpSolverError("Detected an invalid constraint type")
                rhs.append(-constraint.constant)
                sense.append(c_sense)
            lp.solverModel.set_csr_constraint_matrix(
                np.array(matrix_data), np.array(matrix_indices), np.array(matrix_indptr)
            )
            lp.solverModel.set_constraint_bounds(np.array(rhs))
            lp.solverModel.set_row_types(np.array(sense))

            lp.solverModel.set_objective_coefficients(np.array(obj_coeff))
            lp.solverModel.set_objective_offset(lp.objective.constant)

        def actualSolve(self, lp, callback=None):
            """
            Solve a well formulated lp problem

            creates a COPT model, variables and constraints and attaches
            them to the lp model which it then solves
            """
            self.buildSolverModel(lp)
            solution = self.callSolver(lp, callback=callback)

            solutionStatus = self.findSolutionValues(lp, solution)
            for var in lp._variables:
                var.modified = False
            for constraint in lp.constraints.values():
                constraint.modified = False
            return solutionStatus

        def actualResolve(self, lp, callback=None):
            """
            Solve a well formulated lp problem

            uses the old solver and modifies the rhs of the modified constraints
            """
            rhs = lp.solverModel.get_constraint_bounds()
            sense = lp.solverModel.get_row_types()

            for i, name, constraint in enumerate(lp.constraints.items()):
                if constraint.modified:
                    sense[i] = sense_conv[constraint.sense]
                    rhs[i] = -constraint.constant
                    constraint.solverConstraint[name]["bound"] = rhs[i]
                    constraint.solverConstraint[name]["sense"] = sense[i]
            lp.solverModel.set_constraint_bounds(rhs)
            lp.solverModel.set_row_types(sense)

            self.callSolver(lp, callback=callback)

            solutionStatus = self.findSolutionValues(lp)
            for var in lp._variables:
                var.modified = False
            for constraint in lp.constraints.values():
                constraint.modified = False
            return solutionStatus

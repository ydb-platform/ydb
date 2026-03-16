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

# COPT string convention
if sys.version_info >= (3, 0):
    coptstr = lambda x: bytes(x, "utf-8")
else:
    coptstr = lambda x: x

byref = ctypes.byref


class COPT_CMD(LpSolver_CMD):
    """
    The COPT command-line solver
    """

    name = "COPT_CMD"

    def __init__(
        self,
        path=None,
        keepFiles=0,
        mip=True,
        msg=True,
        mip_start=False,
        warmStart=False,
        logfile=None,
        **params,
    ):
        """
        Initialize command-line solver
        """
        LpSolver_CMD.__init__(self, path, keepFiles, mip, msg, [])

        self.mipstart = warmStart
        self.logfile = logfile
        self.solverparams = params

    def defaultPath(self):
        """
        The default path of 'copt_cmd'
        """
        return self.executableExtension("copt_cmd")

    def available(self):
        """
        True if 'copt_cmd' is available
        """
        return self.executable(self.path)

    def actualSolve(self, lp):
        """
        Solve a well formulated LP problem

        This function borrowed implementation of CPLEX_CMD.actualSolve and
        GUROBI_CMD.actualSolve, with some modifications.
        """
        if not self.available():
            raise PulpSolverError("COPT_PULP: Failed to execute '{}'".format(self.path))

        if not self.keepFiles:
            uuid = uuid4().hex
            tmpLp = os.path.join(self.tmpDir, "{}-pulp.lp".format(uuid))
            tmpSol = os.path.join(self.tmpDir, "{}-pulp.sol".format(uuid))
            tmpMst = os.path.join(self.tmpDir, "{}-pulp.mst".format(uuid))
        else:
            # Replace space with underscore to make filepath better
            tmpName = lp.name
            tmpName = tmpName.replace(" ", "_")

            tmpLp = tmpName + "-pulp.lp"
            tmpSol = tmpName + "-pulp.sol"
            tmpMst = tmpName + "-pulp.mst"

        lpvars = lp.writeLP(tmpLp, writeSOS=1)

        # Generate solving commands
        solvecmds = self.path
        solvecmds += " -c "
        solvecmds += '"read ' + tmpLp + ";"

        if lp.isMIP() and self.mipstart:
            self.writemst(tmpMst, lpvars)
            solvecmds += "read " + tmpMst + ";"

        if self.logfile is not None:
            solvecmds += "set logfile {};".format(self.logfile)

        if self.solverparams is not None:
            for parname, parval in self.solverparams.items():
                solvecmds += "set {0} {1};".format(parname, parval)

        if lp.isMIP() and not self.mip:
            solvecmds += "optimizelp;"
        else:
            solvecmds += "optimize;"

        solvecmds += "write " + tmpSol + ";"
        solvecmds += 'exit"'

        try:
            os.remove(tmpSol)
        except:
            pass

        if self.msg:
            msgpipe = None
        else:
            msgpipe = open(os.devnull, "w")

        rc = subprocess.call(solvecmds, shell=True, stdout=msgpipe, stderr=msgpipe)

        if msgpipe is not None:
            msgpipe.close()

        # Get and analyze result
        if rc != 0:
            raise PulpSolverError("COPT_PULP: Failed to execute '{}'".format(self.path))

        if not os.path.exists(tmpSol):
            status = LpStatusNotSolved
        else:
            status, values = self.readsol(tmpSol)

        if not self.keepFiles:
            for oldfile in [tmpLp, tmpSol, tmpMst]:
                try:
                    os.remove(oldfile)
                except:
                    pass

        if status == LpStatusOptimal:
            lp.assignVarsVals(values)

        # lp.assignStatus(status)
        lp.status = status

        return status

    def readsol(self, filename):
        """
        Read COPT solution file
        """
        with open(filename) as solfile:
            try:
                next(solfile)
            except StopIteration:
                warnings.warn("COPT_PULP: No solution was returned")
                return LpStatusNotSolved, {}

            # TODO: No information about status, assumed to be optimal
            status = LpStatusOptimal

            values = {}
            for line in solfile:
                if line[0] != "#":
                    varname, varval = line.split()
                    values[varname] = float(varval)
        return status, values

    def writemst(self, filename, lpvars):
        """
        Write COPT MIP start file
        """
        mstvals = [(v.name, v.value()) for v in lpvars if v.value() is not None]
        mstline = []
        for varname, varval in mstvals:
            mstline.append("{0} {1}".format(varname, varval))

        with open(filename, "w") as mstfile:
            mstfile.write("\n".join(mstline))
        return True


def COPT_DLL_loadlib():
    """
    Load COPT shared library in all supported platforms
    """
    from glob import glob

    libfile = None
    libpath = None
    libhome = os.getenv("COPT_HOME")

    if sys.platform == "win32":
        libfile = glob(os.path.join(libhome, "bin", "copt.dll"))
    elif sys.platform == "linux":
        libfile = glob(os.path.join(libhome, "lib", "libcopt.so"))
    elif sys.platform == "darwin":
        libfile = glob(os.path.join(libhome, "lib", "libcopt.dylib"))
    else:
        raise PulpSolverError("COPT_PULP: Unsupported operating system")

    # Find desired library in given search path
    if libfile:
        libpath = libfile[0]

    if libpath is None:
        raise PulpSolverError(
            "COPT_PULP: Failed to locate solver library, "
            "please refer to COPT manual for installation guide"
        )
    else:
        if sys.platform == "win32":
            coptlib = ctypes.windll.LoadLibrary(libpath)
        else:
            coptlib = ctypes.cdll.LoadLibrary(libpath)

    return coptlib


# COPT LP/MIP status map
coptlpstat = {
    0: LpStatusNotSolved,
    1: LpStatusOptimal,
    2: LpStatusInfeasible,
    3: LpStatusUnbounded,
    4: LpStatusNotSolved,
    5: LpStatusNotSolved,
    6: LpStatusNotSolved,
    8: LpStatusNotSolved,
    9: LpStatusNotSolved,
    10: LpStatusNotSolved,
}

# COPT variable types map
coptctype = {
    LpContinuous: coptstr("C"),
    LpBinary: coptstr("B"),
    LpInteger: coptstr("I"),
}

# COPT constraint types map
coptrsense = {
    LpConstraintEQ: coptstr("E"),
    LpConstraintLE: coptstr("L"),
    LpConstraintGE: coptstr("G"),
}

# COPT objective senses map
coptobjsen = {LpMinimize: 1, LpMaximize: -1}


class COPT_DLL(LpSolver):
    """
    The COPT dynamic library solver
    """

    name = "COPT_DLL"

    try:
        coptlib = COPT_DLL_loadlib()
    except Exception as e:
        err = e
        """The COPT dynamic library solver (DLL). Something went wrong!!!!"""

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp):
            """Solve a well formulated lp problem"""
            raise PulpSolverError(f"COPT_DLL: Not Available:\n{self.err}")

    else:
        # COPT API name map
        CreateEnv = coptlib.COPT_CreateEnv
        DeleteEnv = coptlib.COPT_DeleteEnv
        CreateProb = coptlib.COPT_CreateProb
        DeleteProb = coptlib.COPT_DeleteProb
        LoadProb = coptlib.COPT_LoadProb
        AddCols = coptlib.COPT_AddCols
        WriteMps = coptlib.COPT_WriteMps
        WriteLp = coptlib.COPT_WriteLp
        WriteBin = coptlib.COPT_WriteBin
        WriteSol = coptlib.COPT_WriteSol
        WriteBasis = coptlib.COPT_WriteBasis
        WriteMst = coptlib.COPT_WriteMst
        WriteParam = coptlib.COPT_WriteParam
        AddMipStart = coptlib.COPT_AddMipStart
        SolveLp = coptlib.COPT_SolveLp
        Solve = coptlib.COPT_Solve
        GetSolution = coptlib.COPT_GetSolution
        GetLpSolution = coptlib.COPT_GetLpSolution
        GetIntParam = coptlib.COPT_GetIntParam
        SetIntParam = coptlib.COPT_SetIntParam
        GetDblParam = coptlib.COPT_GetDblParam
        SetDblParam = coptlib.COPT_SetDblParam
        GetIntAttr = coptlib.COPT_GetIntAttr
        GetDblAttr = coptlib.COPT_GetDblAttr
        SearchParamAttr = coptlib.COPT_SearchParamAttr
        SetLogFile = coptlib.COPT_SetLogFile

        def __init__(
            self,
            mip=True,
            msg=True,
            mip_start=False,
            warmStart=False,
            logfile=None,
            **params,
        ):
            """
            Initialize COPT solver
            """

            LpSolver.__init__(self, mip, msg)

            # Initialize COPT environment and problem
            self.coptenv = None
            self.coptprob = None

            # Use MIP start information
            self.mipstart = warmStart

            # Create COPT environment and problem
            self.create()

            # Set log file
            if logfile is not None:
                rc = self.SetLogFile(self.coptprob, coptstr(logfile))
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to set log file")

            # Set parameters to problem
            if not self.msg:
                self.setParam("Logging", 0)

            for parname, parval in params.items():
                self.setParam(parname, parval)

        def available(self):
            """
            True if dynamic library is available
            """
            return True

        def actualSolve(self, lp):
            """
            Solve a well formulated LP/MIP problem

            This function borrowed implementation of CPLEX_DLL.actualSolve,
            with some modifications.
            """
            # Extract problem data and load it into COPT
            (
                ncol,
                nrow,
                nnonz,
                objsen,
                objconst,
                colcost,
                colbeg,
                colcnt,
                colind,
                colval,
                coltype,
                collb,
                colub,
                rowsense,
                rowrhs,
                colname,
                rowname,
            ) = self.extract(lp)

            rc = self.LoadProb(
                self.coptprob,
                ncol,
                nrow,
                objsen,
                objconst,
                colcost,
                colbeg,
                colcnt,
                colind,
                colval,
                coltype,
                collb,
                colub,
                rowsense,
                rowrhs,
                None,
                colname,
                rowname,
            )
            if rc != 0:
                raise PulpSolverError("COPT_PULP: Failed to load problem")

            if lp.isMIP() and self.mip:
                # Load MIP start information
                if self.mipstart:
                    mstdict = {
                        self.v2n[v]: v.value()
                        for v in lp.variables()
                        if v.value() is not None
                    }

                    if mstdict:
                        mstkeys = ctypesArrayFill(list(mstdict.keys()), ctypes.c_int)
                        mstvals = ctypesArrayFill(
                            list(mstdict.values()), ctypes.c_double
                        )

                        rc = self.AddMipStart(
                            self.coptprob, len(mstkeys), mstkeys, mstvals
                        )
                        if rc != 0:
                            raise PulpSolverError(
                                "COPT_PULP: Failed to add MIP start information"
                            )

                # Solve the problem
                rc = self.Solve(self.coptprob)
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to solve the MIP problem")
            elif lp.isMIP() and not self.mip:
                # Solve MIP as LP
                rc = self.SolveLp(self.coptprob)
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to solve MIP as LP")
            else:
                # Solve the LP problem
                rc = self.SolveLp(self.coptprob)
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to solve the LP problem")

            # Get problem status and solution
            status = self.getsolution(lp, ncol, nrow)

            # Reset attributes
            for var in lp.variables():
                var.modified = False

            return status

        def extract(self, lp):
            """
            Extract data from PuLP lp structure

            This function borrowed implementation of LpSolver.getCplexStyleArrays,
            with some modifications.
            """
            cols = list(lp.variables())
            ncol = len(cols)
            nrow = len(lp.constraints)

            collb = (ctypes.c_double * ncol)()
            colub = (ctypes.c_double * ncol)()
            colcost = (ctypes.c_double * ncol)()
            coltype = (ctypes.c_char * ncol)()
            colname = (ctypes.c_char_p * ncol)()

            rowrhs = (ctypes.c_double * nrow)()
            rowsense = (ctypes.c_char * nrow)()
            rowname = (ctypes.c_char_p * nrow)()

            spmat = sparse.Matrix(list(range(nrow)), list(range(ncol)))

            # Objective sense and constant offset
            objsen = coptobjsen[lp.sense]
            objconst = ctypes.c_double(0.0)

            # Associate each variable with a ordinal
            self.v2n = dict(((cols[i], i) for i in range(ncol)))
            self.vname2n = dict(((cols[i].name, i) for i in range(ncol)))
            self.n2v = dict((i, cols[i]) for i in range(ncol))
            self.c2n = {}
            self.n2c = {}
            self.addedVars = ncol
            self.addedRows = nrow

            # Extract objective cost
            for col, val in lp.objective.items():
                colcost[self.v2n[col]] = val

            # Extract variable types, names and lower/upper bounds
            for col in lp.variables():
                colname[self.v2n[col]] = coptstr(col.name)

                if col.lowBound is not None:
                    collb[self.v2n[col]] = col.lowBound
                else:
                    collb[self.v2n[col]] = -1e30

                if col.upBound is not None:
                    colub[self.v2n[col]] = col.upBound
                else:
                    colub[self.v2n[col]] = 1e30

            # Extract column types
            if lp.isMIP():
                for var in lp.variables():
                    coltype[self.v2n[var]] = coptctype[var.cat]
            else:
                coltype = None

            # Extract constraint rhs, senses and names
            idx = 0
            for row in lp.constraints:
                rowrhs[idx] = -lp.constraints[row].constant
                rowsense[idx] = coptrsense[lp.constraints[row].sense]
                rowname[idx] = coptstr(row)

                self.c2n[row] = idx
                self.n2c[idx] = row
                idx += 1

            # Extract coefficient matrix and generate CSC-format matrix
            for col, row, coeff in lp.coefficients():
                spmat.add(self.c2n[row], self.vname2n[col], coeff)

            nnonz, _colbeg, _colcnt, _colind, _colval = spmat.col_based_arrays()

            colbeg = ctypesArrayFill(_colbeg, ctypes.c_int)
            colcnt = ctypesArrayFill(_colcnt, ctypes.c_int)
            colind = ctypesArrayFill(_colind, ctypes.c_int)
            colval = ctypesArrayFill(_colval, ctypes.c_double)

            return (
                ncol,
                nrow,
                nnonz,
                objsen,
                objconst,
                colcost,
                colbeg,
                colcnt,
                colind,
                colval,
                coltype,
                collb,
                colub,
                rowsense,
                rowrhs,
                colname,
                rowname,
            )

        def create(self):
            """
            Create COPT environment and problem

            This function borrowed implementation of CPLEX_DLL.grabLicense,
            with some modifications.
            """
            # In case recreate COPT environment and problem
            self.delete()

            self.coptenv = ctypes.c_void_p()
            self.coptprob = ctypes.c_void_p()

            # Create COPT environment
            rc = self.CreateEnv(byref(self.coptenv))
            if rc != 0:
                raise PulpSolverError("COPT_PULP: Failed to create environment")

            # Create COPT problem
            rc = self.CreateProb(self.coptenv, byref(self.coptprob))
            if rc != 0:
                raise PulpSolverError("COPT_PULP: Failed to create problem")

        def __del__(self):
            """
            Destructor of COPT_DLL class
            """
            self.delete()

        def delete(self):
            """
            Release COPT problem and environment

            This function borrowed implementation of CPLEX_DLL.releaseLicense,
            with some modifications.
            """
            # Valid environment and problem exist
            if self.coptenv is not None and self.coptprob is not None:
                # Delete problem
                rc = self.DeleteProb(byref(self.coptprob))
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to delete problem")

                # Delete environment
                rc = self.DeleteEnv(byref(self.coptenv))
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to delete environment")

                # Reset to None
                self.coptenv = None
                self.coptprob = None

        def getsolution(self, lp, ncols, nrows):
            """Get problem solution

            This function borrowed implementation of CPLEX_DLL.findSolutionValues,
            with some modifications.
            """
            status = ctypes.c_int()
            x = (ctypes.c_double * ncols)()
            dj = (ctypes.c_double * ncols)()
            pi = (ctypes.c_double * nrows)()
            slack = (ctypes.c_double * nrows)()

            var_x = {}
            var_dj = {}
            con_pi = {}
            con_slack = {}

            if lp.isMIP() and self.mip:
                hasmipsol = ctypes.c_int()
                # Get MIP problem satus
                rc = self.GetIntAttr(self.coptprob, coptstr("MipStatus"), byref(status))
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to get MIP status")
                # Has MIP solution
                rc = self.GetIntAttr(
                    self.coptprob, coptstr("HasMipSol"), byref(hasmipsol)
                )
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to check if MIP solution exists"
                    )

                # Optimal/Feasible MIP solution
                if status.value == 1 or hasmipsol.value == 1:
                    rc = self.GetSolution(self.coptprob, byref(x))
                    if rc != 0:
                        raise PulpSolverError("COPT_PULP: Failed to get MIP solution")

                    for i in range(ncols):
                        var_x[self.n2v[i].name] = x[i]

                # Assign MIP solution to variables
                lp.assignVarsVals(var_x)
            else:
                # Get LP problem status
                rc = self.GetIntAttr(self.coptprob, coptstr("LpStatus"), byref(status))
                if rc != 0:
                    raise PulpSolverError("COPT_PULP: Failed to get LP status")

                # Optimal LP solution
                if status.value == 1:
                    rc = self.GetLpSolution(
                        self.coptprob, byref(x), byref(slack), byref(pi), byref(dj)
                    )
                    if rc != 0:
                        raise PulpSolverError("COPT_PULP: Failed to get LP solution")

                    for i in range(ncols):
                        var_x[self.n2v[i].name] = x[i]
                        var_dj[self.n2v[i].name] = dj[i]

                    # NOTE: slacks in COPT are activities of rows
                    for i in range(nrows):
                        con_pi[self.n2c[i]] = pi[i]
                        con_slack[self.n2c[i]] = slack[i]

                # Assign LP solution to variables and constraints
                lp.assignVarsVals(var_x)
                lp.assignVarsDj(var_dj)
                lp.assignConsPi(con_pi)
                lp.assignConsSlack(con_slack)

            # Reset attributes
            lp.resolveOK = True
            for var in lp.variables():
                var.isModified = False

            lp.status = coptlpstat.get(status.value, LpStatusUndefined)
            return lp.status

        def write(self, filename):
            """
            Write problem, basis, parameter or solution to file
            """
            file_path = coptstr(filename)
            file_name, file_ext = os.path.splitext(file_path)

            if not file_ext:
                raise PulpSolverError("COPT_PULP: Failed to determine output file type")
            elif file_ext == coptstr(".mps"):
                rc = self.WriteMps(self.coptprob, file_path)
            elif file_ext == coptstr(".lp"):
                rc = self.WriteLp(self.coptprob, file_path)
            elif file_ext == coptstr(".bin"):
                rc = self.WriteBin(self.coptprob, file_path)
            elif file_ext == coptstr(".sol"):
                rc = self.WriteSol(self.coptprob, file_path)
            elif file_ext == coptstr(".bas"):
                rc = self.WriteBasis(self.coptprob, file_path)
            elif file_ext == coptstr(".mst"):
                rc = self.WriteMst(self.coptprob, file_path)
            elif file_ext == coptstr(".par"):
                rc = self.WriteParam(self.coptprob, file_path)
            else:
                raise PulpSolverError("COPT_PULP: Unsupported file type")

            if rc != 0:
                raise PulpSolverError(
                    "COPT_PULP: Failed to write file '{}'".format(filename)
                )

        def setParam(self, name, val):
            """
            Set parameter to COPT problem
            """
            par_type = ctypes.c_int()
            par_name = coptstr(name)

            rc = self.SearchParamAttr(self.coptprob, par_name, byref(par_type))
            if rc != 0:
                raise PulpSolverError(
                    "COPT_PULP: Failed to check type for '{}'".format(par_name)
                )

            if par_type.value == 0:
                rc = self.SetDblParam(self.coptprob, par_name, ctypes.c_double(val))
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to set double parameter '{}'".format(
                            par_name
                        )
                    )
            elif par_type.value == 1:
                rc = self.SetIntParam(self.coptprob, par_name, ctypes.c_int(val))
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to set integer parameter '{}'".format(
                            par_name
                        )
                    )
            else:
                raise PulpSolverError(
                    "COPT_PULP: Invalid parameter '{}'".format(par_name)
                )

        def getParam(self, name):
            """
            Get current value of parameter
            """
            par_dblval = ctypes.c_double()
            par_intval = ctypes.c_int()
            par_type = ctypes.c_int()
            par_name = coptstr(name)

            rc = self.SearchParamAttr(self.coptprob, par_name, byref(par_type))
            if rc != 0:
                raise PulpSolverError(
                    "COPT_PULP: Failed to check type for '{}'".format(par_name)
                )

            if par_type.value == 0:
                rc = self.GetDblParam(self.coptprob, par_name, byref(par_dblval))
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to get double parameter '{}'".format(
                            par_name
                        )
                    )
                else:
                    retval = par_dblval.value
            elif par_type.value == 1:
                rc = self.GetIntParam(self.coptprob, par_name, byref(par_intval))
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to get integer parameter '{}'".format(
                            par_name
                        )
                    )
                else:
                    retval = par_intval.value
            else:
                raise PulpSolverError(
                    "COPT_PULP: Invalid parameter '{}'".format(par_name)
                )

            return retval

        def getAttr(self, name):
            """
            Get attribute of the problem
            """
            attr_dblval = ctypes.c_double()
            attr_intval = ctypes.c_int()
            attr_type = ctypes.c_int()
            attr_name = coptstr(name)

            # Check attribute type by name
            rc = self.SearchParamAttr(self.coptprob, attr_name, byref(attr_type))
            if rc != 0:
                raise PulpSolverError(
                    "COPT_PULP: Failed to check type for '{}'".format(attr_name)
                )

            if attr_type.value == 2:
                rc = self.GetDblAttr(self.coptprob, attr_name, byref(attr_dblval))
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to get double attribute '{}'".format(
                            attr_name
                        )
                    )
                else:
                    retval = attr_dblval.value
            elif attr_type.value == 3:
                rc = self.GetIntAttr(self.coptprob, attr_name, byref(attr_intval))
                if rc != 0:
                    raise PulpSolverError(
                        "COPT_PULP: Failed to get integer attribute '{}'".format(
                            attr_name
                        )
                    )
                else:
                    retval = attr_intval.value
            else:
                raise PulpSolverError(
                    "COPT_PULP: Invalid attribute '{}'".format(attr_name)
                )

            return retval


class COPT(LpSolver):
    """
    The COPT Optimizer via its python interface

    The COPT variables are available (after a solve) in var.solverVar
    Constraints in constraint.solverConstraint
    and the Model is in prob.solverModel
    """

    name = "COPT"

    try:
        global coptpy
        import coptpy  # type: ignore[import-not-found, import-untyped, unused-ignore]
    except:

        def available(self):
            """True if the solver is available"""
            return False

        def actualSolve(self, lp, callback=None):
            """Solve a well formulated lp problem"""
            raise PulpSolverError("COPT: Not available")

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
            self.coptenv = coptpy.Envr()
            self.coptmdl = self.coptenv.createModel()

            if not self.msg:
                self.coptmdl.setParam("Logging", 0)
            for key, value in solverParams.items():
                self.coptmdl.setParam(key, value)

        def findSolutionValues(self, lp):
            model = lp.solverModel
            solutionStatus = model.status

            CoptLpStatus = {
                coptpy.COPT.UNSTARTED: LpStatusNotSolved,
                coptpy.COPT.OPTIMAL: LpStatusOptimal,
                coptpy.COPT.INFEASIBLE: LpStatusInfeasible,
                coptpy.COPT.UNBOUNDED: LpStatusUnbounded,
                coptpy.COPT.INF_OR_UNB: LpStatusInfeasible,
                coptpy.COPT.NUMERICAL: LpStatusNotSolved,
                coptpy.COPT.NODELIMIT: LpStatusNotSolved,
                coptpy.COPT.TIMEOUT: LpStatusNotSolved,
                coptpy.COPT.UNFINISHED: LpStatusNotSolved,
                coptpy.COPT.INTERRUPTED: LpStatusNotSolved,
            }

            if self.msg:
                print("COPT status=", solutionStatus)

            lp.resolveOK = True
            for var in lp._variables:
                var.isModified = False

            status = CoptLpStatus.get(solutionStatus, LpStatusUndefined)
            lp.assignStatus(status)
            if status != LpStatusOptimal:
                return status

            values = model.getInfo("Value", model.getVars())
            for var, value in zip(lp._variables, values):
                var.varValue = value

            if not model.ismip:
                # NOTE: slacks in COPT are activities of rows
                slacks = model.getInfo("Slack", model.getConstrs())
                for constr, value in zip(lp.constraints.values(), slacks):
                    constr.slack = value

                redcosts = model.getInfo("RedCost", model.getVars())
                for var, value in zip(lp._variables, redcosts):
                    var.dj = value

                duals = model.getInfo("Dual", model.getConstrs())
                for constr, value in zip(lp.constraints.values(), duals):
                    constr.pi = value

            return status

        def available(self):
            """True if the solver is available"""
            return True

        def callSolver(self, lp, callback=None):
            """Solves the problem with COPT"""
            self.solveTime = -clock()
            if callback is not None:
                lp.solverModel.setCallback(
                    callback,
                    coptpy.COPT.CBCONTEXT_MIPRELAX | coptpy.COPT.CBCONTEXT_MIPSOL,
                )
            lp.solverModel.solve()
            self.solveTime += clock()

        def buildSolverModel(self, lp):
            """
            Takes the pulp lp model and translates it into a COPT model
            """
            lp.solverModel = self.coptmdl

            if lp.sense == LpMaximize:
                lp.solverModel.objsense = coptpy.COPT.MAXIMIZE
            if self.timeLimit:
                lp.solverModel.setParam("TimeLimit", self.timeLimit)

            gapRel = self.optionsDict.get("gapRel")
            logPath = self.optionsDict.get("logPath")
            if gapRel:
                lp.solverModel.setParam("RelGap", gapRel)
            if logPath:
                lp.solverModel.setLogFile(logPath)

            for var in lp.variables():
                lowBound = var.lowBound
                if lowBound is None:
                    lowBound = -coptpy.COPT.INFINITY
                upBound = var.upBound
                if upBound is None:
                    upBound = coptpy.COPT.INFINITY
                obj = lp.objective.get(var, 0.0)
                varType = coptpy.COPT.CONTINUOUS
                if var.cat == LpInteger and self.mip:
                    varType = coptpy.COPT.INTEGER
                var.solverVar = lp.solverModel.addVar(
                    lowBound, upBound, vtype=varType, obj=obj, name=var.name
                )

            if self.optionsDict.get("warmStart", False):
                for var in lp._variables:
                    if var.varValue is not None:
                        self.coptmdl.setMipStart(var.solverVar, var.varValue)
                self.coptmdl.loadMipStart()

            for name, constraint in lp.constraints.items():
                expr = coptpy.LinExpr(
                    [v.solverVar for v in constraint.keys()], list(constraint.values())
                )
                if constraint.sense == LpConstraintLE:
                    relation = coptpy.COPT.LESS_EQUAL
                elif constraint.sense == LpConstraintGE:
                    relation = coptpy.COPT.GREATER_EQUAL
                elif constraint.sense == LpConstraintEQ:
                    relation = coptpy.COPT.EQUAL
                else:
                    raise PulpSolverError("Detected an invalid constraint type")
                constraint.solverConstraint = lp.solverModel.addConstr(
                    expr, relation, -constraint.constant, name
                )

        def actualSolve(self, lp, callback=None):
            """
            Solve a well formulated lp problem

            creates a COPT model, variables and constraints and attaches
            them to the lp model which it then solves
            """
            self.buildSolverModel(lp)
            self.callSolver(lp, callback=callback)

            solutionStatus = self.findSolutionValues(lp)
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
            for constraint in lp.constraints.values():
                if constraint.modified:
                    if constraint.sense == LpConstraintLE:
                        constraint.solverConstraint.ub = -constraint.constant
                    elif constraint.sense == LpConstraintGE:
                        constraint.solverConstraint.lb = -constraint.constant
                    else:
                        constraint.solverConstraint.lb = -constraint.constant
                        constraint.solverConstraint.ub = -constraint.constant

            self.callSolver(lp, callback=callback)

            solutionStatus = self.findSolutionValues(lp)
            for var in lp._variables:
                var.modified = False
            for constraint in lp.constraints.values():
                constraint.modified = False
            return solutionStatus

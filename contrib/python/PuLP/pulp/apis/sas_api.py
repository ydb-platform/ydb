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
import sys
import warnings
from contextlib import redirect_stdout
from io import StringIO
from typing import Union
from uuid import uuid4

from .. import constants
from .core import LpSolver, LpSolver_CMD, PulpSolverError, log

# The maximum length of the names of variables and constraints.
MAX_NAME_LENGTH = 256

# This combines all status codes from OPTLP/solvelp and OPTMILP/solvemilp
SOLSTATUS_TO_STATUS = {
    "OPTIMAL": constants.LpStatusOptimal,
    "OPTIMAL_AGAP": constants.LpStatusOptimal,
    "OPTIMAL_RGAP": constants.LpStatusOptimal,
    "OPTIMAL_COND": constants.LpStatusOptimal,
    "TARGET": constants.LpStatusOptimal,
    "CONDITIONAL_OPTIMAL": constants.LpStatusOptimal,
    "FEASIBLE": constants.LpStatusNotSolved,
    "INFEASIBLE": constants.LpStatusInfeasible,
    "UNBOUNDED": constants.LpStatusUnbounded,
    "INFEASIBLE_OR_UNBOUNDED": constants.LpStatusUnbounded,
    "SOLUTION_LIM": constants.LpStatusNotSolved,
    "NODE_LIM_SOL": constants.LpStatusNotSolved,
    "NODE_LIM_NOSOL": constants.LpStatusNotSolved,
    "ITERATION_LIMIT_REACHED": constants.LpStatusNotSolved,
    "TIME_LIM_SOL": constants.LpStatusNotSolved,
    "TIME_LIM_NOSOL": constants.LpStatusNotSolved,
    "TIME_LIMIT_REACHED": constants.LpStatusNotSolved,
    "ABORTED": constants.LpStatusNotSolved,
    "ABORT_SOL": constants.LpStatusNotSolved,
    "ABORT_NOSOL": constants.LpStatusNotSolved,
    "OUTMEM_SOL": constants.LpStatusNotSolved,
    "OUTMEM_NOSOL": constants.LpStatusNotSolved,
    "FAILED": constants.LpStatusNotSolved,
    "FAIL_SOL": constants.LpStatusNotSolved,
    "FAIL_NOSOL": constants.LpStatusNotSolved,
    "ERROR": constants.LpStatusNotSolved,
}

SASPY_OPTIONS = ["cfgname", "cfgfile"]

SWAT_OPTIONS = [
    "hostname",
    "port",
    "username",
    "password",
    "session",
    "locale",
    "name",
    "nworkers",
    "authinfo",
    "protocol",
    "path",
    "ssl_ca_list",
    "authcode",
    "pkce",
]


class SASsolver(LpSolver_CMD):
    name = "SASsolver"

    def __init__(
        self,
        mip=True,
        msg=True,
        warmStart=False,
        keepFiles=False,
        timeLimit=None,
        **solverParams,
    ):
        """
        :param bool mip: if False, assume LP even if integer variables
        :param bool msg: if False, no log is shown
        :param bool warmStart: if False, no warm start
        :param bool keepFiles: if False, the generated mps mst files will be removed
        :param solverParams: SAS proc OPTMILP or OPTLP parameters
        """
        LpSolver_CMD.__init__(
            self,
            mip=mip,
            msg=msg,
            keepFiles=keepFiles,
            timeLimit=timeLimit,
            warmStart=warmStart,
            **solverParams,
        )

    def _create_statement_str(self, statement):
        """Helper function to create the strings for the statements of the proc OPTLP/OPTMILP code."""
        stmt = self.optionsDict.get(statement, None)
        if stmt:
            return (
                statement.strip()
                + " "
                + " ".join(option + "=" + str(value) for option, value in stmt.items())
                + ";"
            )
        else:
            return ""

    def defaultPath(self):
        return os.path.abspath(os.getcwd())

    def _write_sol(self, filename, vs):
        """Writes a SAS solution file"""
        values = [(v.name, v.value()) for v in vs if v.value() is not None]
        with open(filename, "w") as f:
            f.write("_VAR_,_VALUE_\n")
            for name, value in values:
                f.write(f"{name},{value}\n")
        return True

    def _get_max_upload_len(self, fileName):
        maxLen = 0
        with open(fileName, "r") as f:
            for line in f.readlines():
                maxLen = max(maxLen, max([len(word) for word in line.split(" ")]))
        return maxLen + 1

    def _read_solution(self, lp, primal_out, dual_out, proc):
        status = SOLSTATUS_TO_STATUS[self._macro.get("SOLUTION_STATUS", "ERROR")]
        primal_out = primal_out.set_index("_VAR_", drop=True)
        values = primal_out["_VALUE_"].to_dict()
        lp.assignVarsVals(values)

        if proc == "OPTLP":
            rc = primal_out["_R_COST_"].to_dict()
            lp.assignVarsDj(rc)

            dual_out = dual_out.set_index("_ROW_", drop=True)

            prices = dual_out["_VALUE_"].to_dict()
            lp.assignConsPi(prices)

            slacks = dual_out["_ACTIVITY_"].to_dict()
            lp.assignConsSlack(slacks, activity=True)

        lp.assignStatus(status)
        return status


class SAS94(SASsolver):
    name = "SAS94"

    try:
        global saspy
        import saspy  # type: ignore[import-not-found, import-untyped, unused-ignore]

    except:

        def available(self):
            """True if SAS94 is available."""
            return False

        def actualSolve(self, lp, callback=None):
            """Solves a well-formulated lp problem."""
            raise PulpSolverError("SAS94 : Not Available")

    else:
        saspy.logger.setLevel(log.level)  # type: ignore[name-defined]

        def __init__(
            self,
            mip=True,
            msg=True,
            keepFiles=False,
            warmStart=False,
            timeLimit=None,
            **solverParams,
        ):
            """
            :param bool mip: if False, assume LP even if integer variables
            :param bool msg: if False, no log is shown
            :param bool keepFiles: if False, mps and mst files will not be saved
            :param bool warmStart: if False, no warmstart or initial primal solution provided
            :param solverParams: SAS proc OPTMILP or OPTLP parameters
            """
            # Extract saspy connection options
            self._saspy_options = {}
            for option in SASPY_OPTIONS:
                value = solverParams.pop(option, None)
                if value:
                    self._saspy_options[option] = value

            SASsolver.__init__(
                self,
                mip=mip,
                msg=msg,
                keepFiles=keepFiles,
                warmStart=warmStart,
                timeLimit=timeLimit,
                **solverParams,
            )

            # Try to connect to saspy, if this fails, don't throw the error here.
            # Instead we return False on available() to be consistent with
            # other interfaces.
            self.sas = None
            try:
                saspy.logger.disabled = True
                self.sas = saspy.SASsession(**self._saspy_options)
                saspy.logger.disabled = False
            except Exception:
                pass

        def __del__(self):
            if self.sas:
                self.sas.endsas()

        def available(self):
            """True if SAS94 is available."""
            if self.sas:
                return True
            else:
                return False

        def actualSolve(self, lp):  # type: ignore[misc]
            """Solve a well formulated lp problem"""
            log.debug("Running SAS")

            if not self.sas:
                raise PulpSolverError(
                    "SAS94: Cannot connect to a SAS session. Try the cfgfile option or adjust options in that file."
                )

            sas = self.sas
            if len(lp.sos1) or len(lp.sos2):
                raise PulpSolverError(
                    "SAS94: Currently SAS doesn't support SOS1 and SOS2."
                )

            postfix = uuid4().hex[:16]
            mpsName = f"pulp{postfix}.mps"
            localMps = os.path.join(self.tmpDir, mpsName)
            remoteMps = f"/tmp/{mpsName}"  # Remote machine is always Linux
            mstName = f"pulp{postfix}.mst"
            localMst = os.path.join(self.tmpDir, mstName)
            remoteMst = f"/tmp/{mstName}"  # Remote machine is always Linux

            vs = lp.writeMPS(localMps, with_objsense=False)

            nameLen = self._get_max_upload_len(localMps)
            if nameLen > MAX_NAME_LENGTH:
                raise PulpSolverError(
                    f"SAS94: The lengths of the variable or constraint names \
                                    (including indices) should not exceed {MAX_NAME_LENGTH}."
                )

            # If we use a remote SAS installation, need to upload the file
            upload_mps = False
            usedMps = localMps
            if not sas.file_info(localMps, quiet=True):
                sas.upload(localMps, remoteMps, overwrite=True)
                usedMps = remoteMps
                upload_mps = True

            # Figure out if the problem has integer variables
            with_opt = self.optionsDict.pop("with", None)
            if with_opt == "lp":
                proc = "OPTLP"
            elif with_opt == "milp":
                proc = "OPTMILP"
            else:
                proc = "OPTMILP" if (lp.isMIP() and self.mip) else "OPTLP"

            optionList = [
                "warmStart",
                "decomp",
                "decompmaster",
                "decompsubprob",
                "rootnode",
            ]

            solverOptions = {
                key: self.optionsDict[key]
                for key in self.optionsDict.keys()
                if key not in optionList
            }
            warmStart = self.optionsDict.get("warmStart", None)

            # Get Obj Sense
            if lp.sense == constants.LpMaximize:
                solverOptions["objsense"] = "max"
            elif lp.sense == constants.LpMinimize:
                solverOptions["objsense"] = "min"
            else:
                raise PulpSolverError("SAS94 : Objective sense should be min or max.")

            # Get timeLimit. SAS solvers use MAXTIME instead of timeLimit as a parameter.
            if self.timeLimit:
                solverOptions["MAXTIME"] = self.timeLimit
            # Get the rootnode options
            decomp_str = self._create_statement_str("decomp")
            decompmaster_str = self._create_statement_str("decompmaster")
            decompmasterip_str = self._create_statement_str("decompmasterip")
            decompsubprob_str = self._create_statement_str("decompsubprob")
            rootnode_str = self._create_statement_str("rootnode")

            if lp.isMIP() and (proc == "OPTLP" or not self.mip):
                warnings.warn(
                    "SAS94 will solve the relaxed problem of the MILP instance."
                )
            # Handle warmstart
            warmstart_str = ""
            upload_pin = False
            if warmStart:
                self._write_sol(filename=localMst, vs=vs)

                # If we use a remote SAS installation, need to upload the file
                usedMst = localMst
                if not sas.file_info(localMst, quiet=True):
                    sas.upload(
                        localMst,
                        remoteMst,
                        overwrite=True,
                    )
                    usedMst = remoteMst
                    upload_pin = True

                # Set the warmstart basis option
                if proc == "OPTMILP":
                    warmstart_str = """
                                    proc import datafile='{primalin}'
                                        out=primalin{postfix}
                                        dbms=csv
                                        replace;
                                        getnames=yes;
                                        run;
                                    """.format(
                        primalin=usedMst,
                        postfix=postfix,
                    )
                    solverOptions["primalin"] = f"primalin{postfix}"
                elif proc == "OPTLP":
                    pass

            # Convert options to string
            opt_str = " ".join(
                option + "=" + str(value) for option, value in solverOptions.items()
            )

            # Set some SAS options to make the log more clean
            sas_options = "option notes nonumber nodate nosource pagesize=max;"

            # Find the version of 9.4 we are using
            major_version = sas.sasver[0]
            minor_version = sas.sasver.split("M", 1)[1][0]
            if major_version == "9" and int(minor_version) < 5:
                raise NotImplementedError(
                    "Support for SAS 9.4 M4 and earlier is not implemented."
                )
            elif major_version == "9" and int(minor_version) == 5:
                # In 9.4M5 we have to create an MPS data set from an MPS file first
                # Earlier versions will not work because the MPS format in incompatible'
                res = sas.submit(
                    """
                                {sas_options}
                                option notes nonumber nodate nosource pagesize=max;
                                {warmstart}
                                %MPS2SASD(MPSFILE="{mpsfile}", OUTDATA=mpsdata{postfix}, MAXLEN={maxLen}, FORMAT=FREE);
                                proc {proc} data=mpsdata{postfix} {options} primalout=primalout{postfix} dualout=dualout{postfix};
                                {decomp}
                                {decompmaster}
                                {decompmasterip}
                                {decompsubprob}
                                {rootnode}
                                proc delete data=mpsdata{postfix};
                                run;
                                """.format(
                        sas_options=sas_options,
                        warmstart=warmstart_str,
                        postfix=postfix,
                        mpsfile=usedMps,
                        proc=proc,
                        maxLen=min(nameLen, MAX_NAME_LENGTH),
                        options=opt_str,
                        decomp=decomp_str,
                        decompmaster=decompmaster_str,
                        decompmasterip=decompmasterip_str,
                        decompsubprob=decompsubprob_str,
                        rootnode=rootnode_str,
                    ),
                    results="TEXT",
                )
            else:
                # Since 9.4M6+ optlp/optmilp can read mps files directly
                res = sas.submit(
                    """
                                {sas_options}
                                {warmstart}
                                proc {proc} mpsfile=\"{mpsfile}\" {options} primalout=primalout{postfix} dualout=dualout{postfix};
                                {decomp}
                                {decompmaster}
                                {decompmasterip}
                                {decompsubprob}
                                {rootnode}
                                run;
                                """.format(
                        sas_options=sas_options,
                        warmstart=warmstart_str,
                        postfix=postfix,
                        proc=proc,
                        mpsfile=usedMps,
                        options=opt_str,
                        decomp=decomp_str,
                        decompmaster=decompmaster_str,
                        decompmasterip=decompmasterip_str,
                        decompsubprob=decompsubprob_str,
                        rootnode=rootnode_str,
                    ),
                    results="TEXT",
                )

            # Clean up local files
            self.delete_tmp_files(localMps, localMst)

            # Clean up uploaded files
            if upload_mps:
                sas.file_delete(remoteMps, quiet=True)
            if upload_pin:
                sas.file_delete(remoteMst, quiet=True)

            # Store SAS output
            if self.msg:
                self._log = res["LOG"]
                print(self._log)
            self._macro = dict(
                (key.strip(), value.strip())
                for key, value in (
                    pair.split("=") for pair in sas.symget("_OR" + proc + "_").split()
                )
            )

            # Check for error and raise exception
            if self._macro.get("STATUS", "ERROR") != "OK":
                raise PulpSolverError(
                    "PuLP: Error ({err_name}) \
                        while trying to solve the instance: {name}".format(
                        err_name=self._macro.get("STATUS", "ERROR"), name=lp.name
                    )
                )

            # Prepare output
            primal_out = sas.sd2df(f"primalout{postfix}")
            dual_out = sas.sd2df(f"dualout{postfix}")
            status = self._read_solution(lp, primal_out, dual_out, proc)

            return status


class SASCAS(SASsolver):
    name = "SASCAS"

    try:
        global swat
        import swat  # type: ignore[import-not-found, import-untyped, unused-ignore]

    except ImportError:

        def available(self):
            """True if SASCAS is available."""
            return False

        def actualSolve(self, lp, callback=None):
            """Solves a well-formulated lp problem."""
            raise PulpSolverError("SASCAS : Not Available")

    else:

        def __init__(
            self,
            mip=True,
            msg=True,
            keepFiles=False,
            warmStart=False,
            timeLimit=None,
            **solverParams,
        ):
            """
            :param bool mip: if False, assume LP even if integer variables
            :param bool msg: if False, no log is shown
            :param bool keepFiles: if False, mps and mst files will not be saved
            :param bool warmStart: if False, no warmstart or initial primal solution provided
            :param solverParams: SAS proc OPTMILP or OPTLP parameters
            """
            self.cas = None

            # Extract cas_options connection options
            self._cas_options = {}
            for option in SWAT_OPTIONS:
                value = solverParams.pop(option, None)
                if value:
                    self._cas_options[option] = value

            if self._cas_options == {}:
                self._cas_options["hostname"] = os.getenv("CAS_SERVER")
                self._cas_options["port"] = os.getenv("CAS_PORT")
                self._cas_options["authinfo"] = os.getenv("CAS_AUTHINFO")

            SASsolver.__init__(
                self,
                mip=mip,
                msg=msg,
                keepFiles=keepFiles,
                warmStart=warmStart,
                timeLimit=timeLimit,
                **solverParams,
            )

            # Try to connect to SWAT, if this fails, don't throw the error here.
            # Instead we return False on available() to be consistent with
            # other interfaces.
            try:
                self.cas = swat.CAS(**self._cas_options)
            except Exception:
                pass

        def __del__(self):
            if self.cas:
                self.cas.close()

        def available(self):
            if not self.cas:
                return False
            else:
                return True

        def actualSolve(self, lp):  # type: ignore[misc]
            """Solve a well formulated lp problem"""
            log.debug("Running SAS")

            if not self.cas:
                raise PulpSolverError(
                    "SASCAS: Cannot connect to a CAS. Try setting CAS connection options."
                )

            s = self.cas
            if len(lp.sos1) or len(lp.sos2):
                raise PulpSolverError(
                    "SASCAS: Currently SAS doesn't support SOS1 and SOS2."
                )

            warmStart = self.optionsDict.pop("warmStart", False)

            # Figure out if the problem has integer variables
            with_opt = self.optionsDict.pop("with", None)
            if with_opt == "lp":
                proc = "OPTLP"
            elif with_opt == "milp":
                proc = "OPTMILP"
            else:
                proc = "OPTMILP" if (lp.isMIP() and self.mip) else "OPTLP"

            # The options are exactly what it's left in the optionsDict
            solverOptions = self.optionsDict

            # Get Obj Sense
            if lp.sense == constants.LpMaximize:
                solverOptions["objsense"] = "max"
            elif lp.sense == constants.LpMinimize:
                solverOptions["objsense"] = "min"
            else:
                raise PulpSolverError("SASCAS : Objective sense should be min or max.")

            # Get timeLimit. SAS solvers use MAXTIME instead of timeLimit as a parameter.
            if self.timeLimit:
                solverOptions["MAXTIME"] = self.timeLimit

            status = None
            with redirect_stdout(SASLogWriter(self.msg)) as self._log_writer:
                # Load the optimization action set
                s.loadactionset("optimization")

                # Used for naming the data structure in SAS.
                postfix = uuid4().hex[:16]
                tmpMps, tmpMpsCsv, tmpMstCsv = self.create_tmp_files(
                    lp.name, "mps", "mps.csv", "mst.csv"
                )
                vs = lp.writeMPS(tmpMps, with_objsense=False)

                nameLen = self._get_max_upload_len(tmpMps)
                if nameLen > MAX_NAME_LENGTH:
                    raise PulpSolverError(
                        f"SASCAS: The lengths of the variable or constraint names \
                                        (including indices) should not exceed {MAX_NAME_LENGTH}."
                    )
                try:
                    if lp.isMIP() and not self.mip:
                        warnings.warn(
                            "SASCAS will solve the relaxed problem of the MILP instance."
                        )
                    # load_mps
                    self._load_mps(s, tmpMps, tmpMpsCsv, postfix, nameLen)

                    if warmStart and (proc == "OPTMILP"):
                        self._write_sol(filename=tmpMstCsv, vs=vs)
                        # Upload warmstart file to CAS
                        s.upload_file(
                            tmpMstCsv,
                            casout={"name": f"primalin{postfix}", "replace": True},
                            importoptions={"filetype": "CSV"},
                        )
                        solverOptions["primalin"] = f"primalin{postfix}"
                    # Delete the temp files.
                    self.delete_tmp_files(tmpMps, tmpMstCsv, tmpMpsCsv)

                    # Solve the problem in CAS
                    if proc == "OPTMILP":
                        r = s.optimization.solveMilp(
                            data={"name": f"mpsdata{postfix}"},
                            primalOut={"name": f"primalout{postfix}", "replace": True},
                            **solverOptions,
                        )
                    else:
                        r = s.optimization.solveLp(
                            data={"name": f"mpsdata{postfix}"},
                            primalOut={"name": f"primalout{postfix}", "replace": True},
                            dualOut={"name": f"dualout{postfix}", "replace": True},
                            **solverOptions,
                        )
                    if r:
                        primal_out, dual_out = self._get_output(lp, s, r, proc, postfix)
                        status = self._read_solution(lp, primal_out, dual_out, proc)
                finally:
                    self.delete_tmp_files(tmpMps, tmpMstCsv, tmpMpsCsv)

            if status:
                return status
            else:
                raise PulpSolverError(
                    f"PuLP: Error while trying to solve the instance: \
                                {lp.name} via SASCAS."
                )

        def _get_output(self, lp, s, r, proc, postfix):
            self._macro = {
                "STATUS": r.get("status", "ERROR").upper(),
                "SOLUTION_STATUS": r.get("solutionStatus", "ERROR").upper(),
            }
            if self._macro.get("STATUS", "ERROR") != "OK":
                raise PulpSolverError(
                    "PuLP: Error ({err_name}) while trying to solve the instance: {name}".format(
                        err_name=self._macro.get("STATUS", "ERROR"), name=lp.name
                    )
                )
            # If we get solution successfully.
            if proc == "OPTMILP":
                primal_out = s.CASTable(name=f"primalout{postfix}").to_frame()
                primal_out = primal_out[["_VAR_", "_VALUE_"]]
                dual_out = None
            else:
                primal_out = s.CASTable(name=f"primalout{postfix}").to_frame()
                primal_out = primal_out[["_VAR_", "_VALUE_", "_STATUS_", "_R_COST_"]]
                dual_out = s.CASTable(name=f"dualout{postfix}").to_frame()
                dual_out = dual_out[["_ROW_", "_VALUE_", "_STATUS_", "_ACTIVITY_"]]
            return primal_out, dual_out

        def _load_mps(self, s, tmpMps, tmpMpsCsv, postfix, nameLen):
            if os.stat(tmpMps).st_size >= 2 * 1024**3:
                # For large files, use convertMPS, first create file for upload
                with open(tmpMpsCsv, "w") as mpsWithId:
                    mpsWithId.write("_ID_\tText\n")
                    with open(tmpMps, "r") as f:
                        id = 0
                        for line in f:
                            id += 1
                            mpsWithId.write(str(id) + "\t" + line.rstrip() + "\n")

                # Upload .mps.csv file
                s.upload_file(
                    tmpMpsCsv,
                    casout={"name": f"mpscsv{postfix}", "replace": True},
                    importoptions={"filetype": "CSV", "delimiter": "\t"},
                )

                # Convert .mps.csv file to .mps
                s.optimization.convertMps(
                    data=f"mpscsv{postfix}",
                    casOut={"name": f"mpsdata{postfix}", "replace": True},
                    format="FREE",
                    maxLength=min(nameLen, MAX_NAME_LENGTH),
                )
            else:
                # For small files, use loadMPS
                with open(tmpMps, "r") as mps_file:
                    s.optimization.loadMps(
                        mpsFileString=mps_file.read(),
                        casout={"name": f"mpsdata{postfix}", "replace": True},
                        format="FREE",
                        maxLength=min(nameLen, MAX_NAME_LENGTH),
                    )


class SASLogWriter:
    # Helper class to take the log from stdout and put it also in a StringIO.
    def __init__(self, tee):
        # Set up the two outputs.
        self.tee = tee
        self._log = StringIO()
        self.stdout = sys.stdout

    def write(self, message):
        # If the tee options is specified, write to both outputs.
        if self.tee:
            self.stdout.write(message)
        self._log.write(message)

    def flush(self):
        # Do nothing since we flush right away
        pass

    def log(self):
        # Get the log as a string.
        return self._log.getvalue()

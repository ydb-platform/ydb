from collections import defaultdict
import json
import random
import subprocess
import urllib.request
import allure
import logging
import os
import time as time_module
from typing import Optional
import yatest.common
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from ydb.tests.library.stability.utils.collect_errors import create_cluster_issue
from ydb.tests.library.stability.utils.remote_execution import patch_max_suffix, copy_file, execute_command, deploy_binaries_to_hosts
from ydb.tests.library.stability.utils.upload_results import test_event_report
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.library.stability.utils.results_models import StressUtilDeployResult


# Default orchestrator port from nemesis Settings.app_port
_NEMESIS_ORCHESTRATOR_PORT = 31434
# Health polling parameters
_HEALTH_POLL_INTERVAL_S = 10
_HEALTH_POLL_TIMEOUT_S = 300


class StressUtilDeployer:
    binaries_deploy_path: str
    nemesis_started: bool
    nemesis_installed: bool
    hosts: list[str]

    def __init__(self, binaries_deploy_path: str, cluster_path: str, yaml_config: str,
                 static_location: Optional[str] = None):
        self.binaries_deploy_path = binaries_deploy_path
        self.nemesis_started = False
        self.nemesis_installed = False
        self._orchestrator_endpoint: Optional[str] = None
        self.hosts = []
        self.cluster_path = cluster_path
        self.yaml_config = yaml_config
        self.static_location = static_location
        self.nodes = YdbCluster.get_cluster_nodes()
        patch_max_suffix(1000000)

        # Collect unique hosts and their corresponding nodes
        unique_hosts = set(node.host for node in self.nodes)
        self.hosts = list(filter(lambda h: h != 'localhost', unique_hosts))

    def prepare_stress_execution(
        self,
        workload_params: dict,
        nodes_percentage: Optional[int] = None,
    ):
        """
        PHASE 1: Prepare for workload execution

        Args:
            load_test_instance: Load test instance
            workload_params: Dictionary of workload parameters
            nodes_percentage: Percentage of cluster nodes to run workload on (1-100)

        Returns:
            Dictionary with preparation results:
            {
                "deployed_nodes": dict of deployed nodes by workload,
                "total_hosts": set of all hosts used,
                "workload_start_time": timestamp when workload started
            }
        """

        with allure.step("Phase 1: Prepare workload execution"):
            logging.info(
                f"Preparing execution: Nodes_percentage={nodes_percentage}%, mode=parallel"
            )

            # Ensure nemesis services are installed and orchestrator is healthy.
            # This makes the warden API available for diagnostics even when
            # nemesis chaos is not enabled.  Chaos schedules are disabled here;
            # they will be enabled later only if nemesis_enabled=True.
            try:
                logging.info("Ensuring nemesis services are installed and stopping chaos schedules")

                # Create summary log for Allure
                prep_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                nemesis_log = [f"Workload preparation started at {prep_time}"]

                # Install services (idempotent) and wait for orchestrator health
                self.ensure_nemesis_installed(nemesis_log)

                # Disable chaos schedules for a clean start
                self._disable_nemesis(nemesis_log)

                logging.info("Nemesis services ready, chaos schedules disabled")

                allure.attach(
                    "\n".join(nemesis_log),
                    "Nemesis Preparation Summary",
                    attachment_type=allure.attachment_type.TEXT,
                )

            except Exception as e:
                error_msg = f"Error during nemesis preparation: {e}"
                logging.error(error_msg)
                try:
                    allure.attach(
                        error_msg,
                        "Preparation - Nemesis Error",
                        attachment_type=allure.attachment_type.TEXT,
                    )
                except Exception:
                    pass

            # Get unique cluster hosts
            with allure.step("Get all unique cluster hosts"):
                nodes = YdbCluster.get_cluster_nodes()
                if not nodes:
                    raise Exception("No cluster nodes found")

                # Collect unique hosts and their corresponding nodes
                unique_hosts = set(node.host for node in nodes)
                self.hosts = list(filter(lambda h: h != 'localhost', unique_hosts))
                self.nodes = list(filter(lambda n: n.host != 'localhost', nodes))

                allure.attach(
                    str(self.hosts),
                    "Target Hosts",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info(
                    f"Got hosts {self.hosts} for deployment"
                )

            deployed_nodes = {}
            # Deploy to selected percentage of nodes
            deploy_futures = []

            processed_binaries = defaultdict(list)
            with ThreadPoolExecutor(max_workers=10) as tpe:
                for workload_name, workload_info in workload_params.items():
                    if workload_info['local_path'] in processed_binaries:
                        processed_binaries[workload_info['local_path']].append(workload_name)
                        with allure.step(f"Deploy {workload_name} binary"):
                            allure.attach(
                                "Skipping deployment, binary already deployed",
                                "Summary",
                                attachment_type=allure.attachment_type.TEXT,
                            )
                        continue
                    processed_binaries[workload_info['local_path']].append(workload_name)
                    deploy_futures.append(
                        (
                            tpe.submit(self._deploy_workload_binary, workload_name, workload_info['local_path'], nodes_percentage or workload_info.get('nodes_percentage', 1)),
                            workload_name,
                            workload_info['local_path'],
                        )
                    )

            total_hosts = []
            for deploy_future, future_workload_name, future_binary_path in deploy_futures:
                result = StressUtilDeployResult()
                result.nodes = deploy_future.result()
                deployed_hosts = list(map(lambda node: node['node'].host, result.nodes))
                result.hosts = deployed_hosts
                deployed_nodes[future_workload_name] = result

                for stress_util_name in processed_binaries[future_binary_path]:
                    deployed_nodes[stress_util_name] = result

                total_hosts += deployed_hosts

            logging.info(
                f"Preparation completed: {deployed_nodes} nodes in parallel mode"
            )

            return {
                "deployed_nodes": deployed_nodes,
                "total_hosts": set(total_hosts),
                "workload_start_time": time_module.time(),
            }

    def _deploy_workload_binary(self, workload_name: str, workload_path: str, nodes_percentage: int = 100) -> list[YdbCluster.Node]:
        """
        Deploys workload binary to specified percentage of cluster nodes

        Args:
            workload_name: Workload name for reports
            workload_path: Path to workload binary
            nodes_percentage: Percentage of cluster nodes to deploy to (1-100)

        Returns:
            List of dictionaries with info about deployed nodes:
            [{
                'node': node_object,
                'binary_path': path_to_binary
            }, ...]

        Raises:
            Exception: If deployment fails on all target nodes
        """
        with allure.step(f"Deploy {workload_name} binary"):
            logging.info(
                f"Starting deployment for {workload_name} on {nodes_percentage}% of nodes"
            )

            # Get binary file
            with allure.step("Get workload binary"):
                logging.info(f"Binary path from: {workload_path}")
                binary_files = [
                    yatest.common.binary_path(workload_path)
                ]
                allure.attach(
                    f"Binary path: {binary_files[0]}",
                    "Binary Path",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info(f"Binary path resolved: {binary_files[0]}")

            # Get unique cluster hosts
            with allure.step("Select unique cluster hosts"):

                # Select first N nodes from unique list
                unique_hosts = {}
                for node in self.nodes:
                    if node.host not in unique_hosts:
                        unique_hosts[node.host] = node

                unique_nodes = list(unique_hosts.values())

                # Determine number of nodes to deploy based on percentage
                num_nodes = max(
                    1, int(
                        len(unique_nodes) * nodes_percentage / 100))
                selected_nodes = random.sample(unique_nodes, num_nodes)

                allure.attach(
                    f"Selected {
                        len(selected_nodes)} / {
                        len(self.hosts)} unique hosts ({nodes_percentage} %)",
                    "Target Hosts",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info(
                    f"Selected {
                        len(selected_nodes)} / {
                        len(self.hosts)} unique hosts for deployment"
                )

            # Deploy binary to selected nodes
            with allure.step(
                f"Deploy {
                    workload_name} to {
                    len(selected_nodes)} hosts"
            ):
                logging.info(f"Starting deployment to hosts: {selected_nodes}")

                deploy_results = deploy_binaries_to_hosts(
                    binary_files, [node.host for node in selected_nodes], self.binaries_deploy_path
                )
                logging.info(f"Deploy results: {deploy_results}")

                # Collect deployment results info
                deployed_nodes = []
                failed_nodes = []

                for node in selected_nodes:
                    binary_result = deploy_results.get(node.host, {}).get(os.path.basename(binary_files[0]), {})
                    success = binary_result.get("success", False)

                    if success:
                        deployed_nodes.append(
                            {"node": node, "binary_path": binary_result["path"]}
                        )
                        logging.info(
                            f"Deployment successful on {
                                node.host}: {
                                binary_result['path']}"
                        )
                    else:
                        failed_nodes.append(
                            {
                                "node": node,
                                "error": binary_result.get("error", "Unknown error"),
                            }
                        )
                        logging.error(
                            f"Deployment failed on {
                                node.host}: {
                                binary_result.get(
                                    'error',
                                    'Unknown error')}"
                        )

                # Attach deployment details
                allure.attach(
                    f"Successful deployments: {
                        len(deployed_nodes)} / {
                        len(selected_nodes)}\n"
                    + f"Failed deployments: {len(failed_nodes)}/{len(selected_nodes)}",
                    "Deployment Summary",
                    attachment_type=allure.attachment_type.TEXT,
                )

                # Verify at least one node was successfully deployed
                if not deployed_nodes:
                    # Create detailed deployment error message
                    deploy_error_details = []
                    deploy_error_details.append(
                        f"DEPLOYMENT FAILED: {workload_name}"
                    )
                    deploy_error_details.append(
                        f"Target hosts: {selected_nodes}")
                    deploy_error_details.append(
                        f"Target directory: {self.binaries_deploy_path}"
                    )
                    deploy_error_details.append(
                        f"Local binary path: {binary_files[0]}")

                    # Error details
                    deploy_error_details.append("\nDeployment errors:")
                    for failed in failed_nodes:
                        deploy_error_details.append(
                            f"  {failed['node'].host}: {failed['error']}"
                        )

                    detailed_deploy_error = "\n".join(deploy_error_details)

                    cluster_issue = create_cluster_issue(
                        "deployment_failed",
                        f"Binary deployment failed on all {len(selected_nodes)} nodes: {detailed_deploy_error}",
                        0
                    )

                    test_event_report(
                        workload_names=[workload_name],
                        nemesis_enabled=self.nemesis_started,
                        event_kind='ClusterCheck',
                        verification_phase="workload_deployment",
                        check_type="deployment_failure",
                        cluster_issue=cluster_issue
                    )

                    logging.error(detailed_deploy_error)
                    raise Exception(detailed_deploy_error)

                logging.info(
                    f"Binary deployed successfully to {
                        len(deployed_nodes)} unique hosts"
                )
                return deployed_nodes

    # ------------------------------------------------------------------
    # Nemesis orchestrator HTTP helpers
    # ------------------------------------------------------------------

    def _get_orchestrator_endpoint(self) -> str:
        """Return cached ``http://<orchestrator_host>:<port>`` base URL.

        The orchestrator always runs on the first host (sorted) from the
        cluster, matching the convention used by ``install_on_hosts``.
        """
        if self._orchestrator_endpoint is not None:
            return self._orchestrator_endpoint

        if not self.hosts:
            raise RuntimeError(
                "Cannot determine orchestrator host: self.hosts is empty"
            )

        orchestrator_host = sorted(self.hosts)[0]
        self._orchestrator_endpoint = (
            f"http://{orchestrator_host}:{_NEMESIS_ORCHESTRATOR_PORT}"
        )
        logging.info(f"Nemesis orchestrator endpoint: {self._orchestrator_endpoint}")
        return self._orchestrator_endpoint

    def _poll_health(self, nemesis_log: list[str]) -> bool:
        """Poll ``GET /health`` until the orchestrator responds with ``{"status": "ok"}``.

        Returns:
            ``True`` if the orchestrator became healthy within the timeout,
            ``False`` otherwise.
        """
        endpoint = self._get_orchestrator_endpoint()
        url = f"{endpoint}/health"
        deadline = time_module.time() + _HEALTH_POLL_TIMEOUT_S
        attempt = 0

        nemesis_log.append(
            f"Polling orchestrator health at {url} "
            f"(interval={_HEALTH_POLL_INTERVAL_S}s, timeout={_HEALTH_POLL_TIMEOUT_S}s)"
        )
        logging.info(f"Polling nemesis orchestrator health: {url}")

        while time_module.time() < deadline:
            attempt += 1
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    body = json.loads(resp.read().decode())
                    if body.get("status") == "ok":
                        nemesis_log.append(
                            f"Orchestrator healthy after {attempt} attempt(s)"
                        )
                        logging.info(
                            f"Nemesis orchestrator healthy after {attempt} attempt(s)"
                        )
                        return True
                    nemesis_log.append(
                        f"Health attempt {attempt}: unexpected response {body}"
                    )
            except Exception as exc:
                nemesis_log.append(f"Health attempt {attempt}: {exc}")
                logging.debug(f"Health poll attempt {attempt} failed: {exc}")

            time_module.sleep(_HEALTH_POLL_INTERVAL_S)

        nemesis_log.append(
            f"Orchestrator did not become healthy within {_HEALTH_POLL_TIMEOUT_S}s "
            f"({attempt} attempts)"
        )
        logging.error(
            f"Nemesis orchestrator health timeout after {attempt} attempts"
        )
        return False

    def _set_schedules_enabled(
        self, enabled: bool, nemesis_log: list[str]
    ) -> bool:
        """Call ``POST /api/schedule/all`` to enable or disable all nemesis schedules.

        Returns:
            ``True`` on success, ``False`` on failure.
        """
        endpoint = self._get_orchestrator_endpoint()
        url = f"{endpoint}/api/schedule/all"
        payload = json.dumps({"enabled": enabled}).encode()

        action = "Enabling" if enabled else "Disabling"
        nemesis_log.append(f"{action} all nemesis schedules via {url}")
        logging.info(f"{action} nemesis schedules: POST {url}")

        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
                if body.get("status") == "ok":
                    nemesis_log.append(
                        f"Schedules {'enabled' if enabled else 'disabled'} successfully: {body}"
                    )
                    logging.info(f"Nemesis schedules response: {body}")
                    return True
                nemesis_log.append(f"Unexpected schedule response: {body}")
                logging.warning(f"Unexpected /api/schedule/all response: {body}")
                return False
        except Exception as exc:
            nemesis_log.append(f"Failed to {action.lower()} schedules: {exc}")
            logging.error(f"Error calling /api/schedule/all: {exc}")
            return False

    # ------------------------------------------------------------------
    # Nemesis install (subprocess)
    # ------------------------------------------------------------------

    def ensure_nemesis_installed(self, nemesis_log: list[str] = None) -> bool:
        """Ensure nemesis services are installed and the orchestrator is healthy.

        This method is safe to call unconditionally — it installs services
        (if not already installed) and polls the orchestrator health endpoint.
        It does **not** enable chaos schedules.

        This makes the orchestrator warden API available for diagnostics
        even when ``nemesis_enabled=False``.

        Args:
            nemesis_log: Optional list to append log messages to.

        Returns:
            ``True`` if the orchestrator is installed and healthy,
            ``False`` otherwise.
        """
        if nemesis_log is None:
            nemesis_log = []

        if not self._install_nemesis_services(nemesis_log):
            logging.error("Failed to install nemesis services")
            return False

        if not self._poll_health(nemesis_log):
            logging.error("Nemesis orchestrator did not become healthy")
            return False

        nemesis_log.append("Nemesis services installed and orchestrator healthy (schedules not enabled)")
        logging.info("Nemesis orchestrator ready for warden checks (schedules not enabled)")
        return True

    def _install_nemesis_services(self, nemesis_log: list[str]) -> bool:
        """Deploy nemesis binary and start ``nemesis-agent`` on all cluster hosts.

        Runs ``./nemesis install --yaml-config-location <path>`` as a subprocess.
        Skips if services are already installed (``self.nemesis_installed``).

        Returns:
            ``True`` if services are installed (or were already), ``False`` on failure.
        """
        if self.nemesis_installed:
            nemesis_log.append("Nemesis services already installed, skipping install")
            return True

        nemesis_binary_path = os.getenv("NEMESIS_BINARY")
        if nemesis_binary_path:
            nemesis_binary = yatest.common.binary_path(nemesis_binary_path)
        else:
            nemesis_binary = yatest.common.binary_path(
                "ydb/tests/stability/nemesis/nemesis"
            )

        yaml_config_location = self.cluster_path or self.yaml_config
        if not yaml_config_location:
            nemesis_log.append(
                "No cluster config path available for nemesis "
                "(neither cluster_path nor yaml_config set)"
            )
            logging.error(nemesis_log[-1])
            return False

        # When both paths are provided, cluster_path is the cluster YAML
        # (config.hosts, config.bridge_config) and yaml_config is the
        # database template (domains).  The nemesis CLI expects:
        #   --yaml-config-location  → cluster YAML (cluster_path / config.yaml)
        #   --database-config-location → database template (yaml_config / databases.yaml)
        database_config_location = self.yaml_config if self.yaml_config and self.cluster_path else None

        cmd = (
            f"{nemesis_binary} install "
            f"--yaml-config-location {yaml_config_location}"
        )
        if database_config_location:
            cmd += f" --database-config-location {database_config_location}"
        if self.static_location:
            cmd += f" --static-location {self.static_location}"
        nemesis_log.append(f"Nemesis binary: {nemesis_binary}")
        nemesis_log.append(f"Cluster config: {yaml_config_location}")
        nemesis_log.append(f"Running: {cmd}")
        logging.info(f"Installing nemesis via: {cmd}")

        start_time = time_module.time()
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=os.path.dirname(nemesis_binary),
        )
        elapsed = time_module.time() - start_time

        nemesis_log.append(f"Exit code: {result.returncode}")
        nemesis_log.append(f"Execution time: {elapsed:.2f}s")
        if result.stdout:
            nemesis_log.append(f"stdout:\n{result.stdout}")
        if result.stderr:
            nemesis_log.append(f"stderr:\n{result.stderr}")

        if result.returncode != 0:
            nemesis_log.append(
                f"Nemesis install failed with exit code {result.returncode}"
            )
            return False

        nemesis_log.append("Nemesis install completed successfully")
        self.nemesis_installed = True
        return True

    # ------------------------------------------------------------------
    # Enable / disable nemesis chaos
    # ------------------------------------------------------------------

    def _enable_nemesis(
        self, stress_util_names: list[str], nemesis_log: list[str]
    ) -> None:
        """Install services (if needed), wait for health, enable chaos schedules.

        Updates ``self.nemesis_started`` based on the outcome.
        """
        # Step 1: install services
        if not self._install_nemesis_services(nemesis_log):
            self.nemesis_started = False
            cluster_issue = create_cluster_issue(
                "nemesis_startup_failed",
                nemesis_log[-1],
                0,
            )
            test_event_report(
                nemesis_enabled=True,
                workload_names=stress_util_names,
                event_kind="ClusterCheck",
                verification_phase="nemesis_management",
                check_type="nemesis_install_failure",
                cluster_issue=cluster_issue,
            )
            return

        # Step 2: poll /health until orchestrator is ready
        if not self._poll_health(nemesis_log):
            self.nemesis_started = False
            error_msg = "Nemesis orchestrator did not become healthy in time"
            nemesis_log.append(error_msg)
            cluster_issue = create_cluster_issue(
                "nemesis_health_timeout",
                error_msg,
                0,
            )
            test_event_report(
                nemesis_enabled=True,
                workload_names=stress_util_names,
                event_kind="ClusterCheck",
                verification_phase="nemesis_management",
                check_type="nemesis_health_timeout",
                cluster_issue=cluster_issue,
            )
            return

        # Step 3: enable all chaos schedules
        if self._set_schedules_enabled(True, nemesis_log):
            self.nemesis_started = True
            nemesis_log.append("Nemesis chaos enabled successfully")
        else:
            self.nemesis_started = False
            nemesis_log.append("Failed to enable nemesis chaos schedules")

    def _disable_nemesis(self, nemesis_log: list[str]) -> None:
        """Disable chaos schedules.  Services keep running for fast re-enable.

        Updates ``self.nemesis_started`` to ``False``.
        """
        if self.nemesis_installed:
            if self._set_schedules_enabled(False, nemesis_log):
                nemesis_log.append("Nemesis chaos disabled successfully")
            else:
                nemesis_log.append("Failed to disable nemesis chaos schedules")
        else:
            nemesis_log.append(
                "Nemesis services not installed, nothing to disable"
            )
        self.nemesis_started = False

    def _stop_nemesis_services(self, nemesis_log: list[str]) -> bool:
        """Stop ``nemesis-agent`` systemd services on all cluster hosts.

        Runs ``./nemesis stop --yaml-config-location <path>`` as a subprocess.
        Resets ``self.nemesis_installed`` to ``False`` on success.

        Returns:
            ``True`` if services were stopped successfully, ``False`` on failure.
        """
        if not self.nemesis_installed:
            nemesis_log.append("Nemesis services not installed, nothing to stop")
            return True

        nemesis_binary_path = os.getenv("NEMESIS_BINARY")
        if nemesis_binary_path:
            nemesis_binary = yatest.common.binary_path(nemesis_binary_path)
        else:
            nemesis_binary = yatest.common.binary_path(
                "ydb/tests/stability/nemesis/nemesis"
            )

        yaml_config_location = self.cluster_path or self.yaml_config
        if not yaml_config_location:
            nemesis_log.append(
                "No cluster config path available for nemesis stop "
                "(neither cluster_path nor yaml_config set)"
            )
            logging.error(nemesis_log[-1])
            return False

        cmd = (
            f"{nemesis_binary} stop "
            f"--yaml-config-location {yaml_config_location}"
        )
        nemesis_log.append(f"Stopping nemesis services: {cmd}")
        logging.info(f"Stopping nemesis services via: {cmd}")

        try:
            start_time = time_module.time()
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=120,
                cwd=os.path.dirname(nemesis_binary),
            )
            elapsed = time_module.time() - start_time

            nemesis_log.append(f"Exit code: {result.returncode}")
            nemesis_log.append(f"Execution time: {elapsed:.2f}s")
            if result.stdout:
                nemesis_log.append(f"stdout:\n{result.stdout}")
            if result.stderr:
                nemesis_log.append(f"stderr:\n{result.stderr}")

            if result.returncode != 0:
                nemesis_log.append(
                    f"Nemesis stop failed with exit code {result.returncode}"
                )
                return False

            nemesis_log.append("Nemesis services stopped successfully")
            self.nemesis_installed = False
            return True

        except Exception as exc:
            nemesis_log.append(f"Error stopping nemesis services: {exc}")
            logging.error(f"Error stopping nemesis services: {exc}")
            return False

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def _manage_nemesis(
        self,
        enable_nemesis: bool,
        stress_util_names: list[str] = [],
        operation_context: str = None,
        existing_log: list = None,
    ):
        """Manage nemesis chaos schedules via the orchestrator HTTP API.

        * **enable** — install → health-poll → ``POST /api/schedule/all``
        * **disable** — ``POST /api/schedule/all {"enabled": false}``

        Args:
            enable_nemesis: True to enable chaos, False to disable
            stress_util_names: List of stress utility names (for event reporting)
            operation_context: Operation context for logging
            existing_log: Existing log to append info to

        Returns:
            List of strings with operation log
        """
        nemesis_log = existing_log if existing_log is not None else []
        action_name = "Enabling" if enable_nemesis else "Disabling"

        try:
            if operation_context:
                nemesis_log.append(f"{operation_context}: {action_name} nemesis")
            else:
                nemesis_log.append(f"{action_name} nemesis")

            nemesis_log.append(
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

            if enable_nemesis:
                self._enable_nemesis(stress_util_names, nemesis_log)
            else:
                self._disable_nemesis(nemesis_log)

            allure.attach(
                "\n".join(nemesis_log),
                f"Nemesis {action_name} Summary",
                attachment_type=allure.attachment_type.TEXT,
            )
            return nemesis_log

        except subprocess.TimeoutExpired:
            error_msg = "Nemesis install command timed out after 300s"
            nemesis_log.append(error_msg)
            logging.error(error_msg)
            self.nemesis_started = False
            allure.attach(
                "\n".join(nemesis_log),
                f"Nemesis {action_name} Timeout",
                attachment_type=allure.attachment_type.TEXT,
            )
            return nemesis_log

        except Exception as e:
            cluster_issue = create_cluster_issue(
                f"nemesis_{action_name.lower()}_exception",
                f"Exception during nemesis {action_name.lower()}: {e}",
                0,
            )
            test_event_report(
                nemesis_enabled=enable_nemesis,
                workload_names=stress_util_names,
                event_kind="ClusterCheck",
                verification_phase="nemesis_management",
                check_type=f"nemesis_{action_name.lower()}_exception",
                cluster_issue=cluster_issue,
            )
            error_msg = f"Error managing nemesis: {e}"
            logging.error(error_msg)
            allure.attach(
                str(e), "Nemesis Error", attachment_type=allure.attachment_type.TEXT
            )
            return nemesis_log + [error_msg]

    def delayed_nemesis_start(self, delay_seconds: int, stress_util_names: list[str]):
        """
        Starts nemesis with delay after workload begins

        Args:
            delay_seconds: Delay in seconds before starting nemesis

        Note:
            Creates a separate thread to handle the delayed start
            and attaches detailed timing information to Allure report
        """
        try:
            # Create log for Allure
            nemesis_log = []
            start_time = datetime.now()
            nemesis_log.append(
                f"Nemesis scheduled start at {
                    start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            nemesis_log.append(f"Delay: {delay_seconds} seconds")

            logging.info(f"Nemesis will start in {delay_seconds} seconds...")

            # Add information about planned start time
            planned_start_time = start_time + timedelta(seconds=delay_seconds)
            nemesis_log.append(
                f"Planned start time: {
                    planned_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Add preliminary log to Allure
            allure.attach(
                "\n".join(nemesis_log),
                "Nemesis Scheduled Start",
                attachment_type=allure.attachment_type.TEXT,
            )

            # Wait specified time
            time_module.sleep(delay_seconds)

            # Update log
            actual_start_time = datetime.now()
            nemesis_log.append(
                f"Actual start time: {
                    actual_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            logging.info(
                f"Starting nemesis after {delay_seconds}s delay at {
                    actual_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            with allure.step(f"Start nemesis after {delay_seconds}s delay"):
                # Start nemesis using class common method
                allure.attach(
                    "\n".join(nemesis_log),
                    "Nemesis Delayed Start Info",
                    attachment_type=allure.attachment_type.TEXT,
                )
                logging.info("Calling _manage_nemesis(True) to start nemesis service")
                self._manage_nemesis(
                    True, stress_util_names, f"Delayed start after {delay_seconds}s", nemesis_log
                )

            logging.info("Nemesis started successfully after delay")

        except Exception as e:
            error_msg = f"Error starting nemesis after delay: {e}"
            logging.error(error_msg)
            allure.attach(
                f"{error_msg}\n\nDelay: {delay_seconds} seconds",
                "Nemesis Delayed Start Error",
                attachment_type=allure.attachment_type.TEXT,
            )

    def _copy_cluster_config(self, host: str, host_log: list[str]) -> dict:
        """Copies cluster configuration to host

        Args:
            host: Target hostname
            host_log: List to append log messages to

        Returns:
            dict: Operation result with keys:
                - success: bool
                - log: list[str]
                - error: str (if failed)
        """
        logging.info(f"Cluster path for {host}: {self.cluster_path}")
        logging.info(f"YAML config for {host}: {self.yaml_config}")

        # Copy cluster.yaml (if cluster_path is specified)
        cluster_result = self._copy_single_config(
            host, self.cluster_path, "/Berkanavt/nemesis/cluster.yaml",
            "cluster config", None, host_log
        )
        if not cluster_result["success"]:
            return cluster_result

        # Copy databases.yaml (if yaml_config is specified)
        if self.yaml_config:
            databases_result = self._copy_single_config(
                host, self.yaml_config, "/Berkanavt/kikimr/cfg/databases.yaml",
                "databases config", None, host_log
            )
            if not databases_result["success"]:
                return databases_result

        return {"host": host, "success": True, "log": host_log}

    def _copy_single_config(
        self,
        host: str,
        config_path: str,
        remote_path: str,
        config_name: str,
        fallback_source: str,
        host_log: list[str]
    ) -> dict:
        """Copies a single configuration file

        Args:
            host: Target hostname
            config_path: Local path to config file
            remote_path: Destination path on host
            config_name: Name of config for logging
            fallback_source: Fallback source path if config_path not specified
            host_log: List to append log messages to

        Returns:
            dict: Operation result with keys:
                - success: bool
                - log: list[str]
                - error: str (if failed)
        """
        if config_path:
            # Copy external file
            if not os.path.exists(config_path):
                error_msg = f"{config_name} file does not exist: {config_path}"
                host_log.append(error_msg)
                logging.error(error_msg)
                return {"host": host, "success": False, "error": error_msg, "log": host_log}

            source = config_path
            success_msg = f"Copied external {config_name} from {config_path}"
        elif fallback_source:
            # Use local fallback
            source = fallback_source
            success_msg = f"Copied local {config_name}"
        else:
            # Don't copy anything
            return {"host": host, "success": True, "log": host_log}

        # Perform the copy operation
        host_log.append(f"Copying {config_name} from {source}")

        if config_path:
            result = copy_file(
                local_path=config_path,
                host=host,
                remote_path=remote_path,
                raise_on_error=False
            )
        else:
            result = execute_command(
                host=host,
                cmd=f"sudo cp {fallback_source} {remote_path}",
                raise_on_error=False,
                timeout=90
            )

        # Check the result
        if config_path and not result:
            error_msg = f"Failed to copy {config_name} to {remote_path} on {host}"
            host_log.append(error_msg)
            return {"host": host, "success": False, "error": error_msg, "log": host_log}
        elif not config_path and result.stderr and "error" in result.stderr.lower():
            error_msg = f"Error copying {config_name} on {host}: {result.stderr}"
            host_log.append(error_msg)
            return {"host": host, "success": False, "error": error_msg, "log": host_log}

        host_log.append(success_msg)
        logging.info(f"Successfully copied {config_name} to {host}")
        return {"host": host, "success": True, "log": host_log}

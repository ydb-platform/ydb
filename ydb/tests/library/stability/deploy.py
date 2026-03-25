from collections import defaultdict
import random
import allure
import logging
import os
import time as time_module
from typing import Optional
import yatest.common
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from ydb.tests.library.stability.utils.collect_errors import create_cluster_issue
from ydb.tests.library.stability.utils.remote_execution import patch_max_suffix, copy_file, execute_command, deploy_binaries_to_hosts
from ydb.tests.library.stability.utils.upload_results import test_event_report
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.library.stability.utils.results_models import StressUtilDeployResult


class StressUtilDeployer:
    binaries_deploy_path: str
    nemesis_started: bool
    hosts: list[str]

    def __init__(self, binaries_deploy_path: str, cluster_path: str, yaml_config: str):
        self.binaries_deploy_path = binaries_deploy_path
        self.nemesis_started = False
        self.hosts = []
        self.cluster_path = cluster_path
        self.yaml_config = yaml_config
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

            # Stop nemesis before each workload run for clean start
            try:
                logging.info("Stopping nemesis service before workload execution for clean start")

                # Create summary log for Allure
                prep_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                nemesis_log = [f"Workload preparation started at {prep_time}"]

                # Stop nemesis using common method
                self._manage_nemesis(False, list(workload_params.keys()), "Stopping nemesis before workload execution", nemesis_log)

                logging.info("Nemesis stopped successfully before workload execution")

            except Exception as e:
                error_msg = f"Error stopping nemesis before workload execution: {e}"
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

    def _manage_nemesis(
        self,
        enable_nemesis: bool,
        stress_util_names: list[str] = [],
        operation_context: str = None,
        existing_log: list = None,
    ):
        """
        Manages nemesis service on all unique cluster hosts (parallel execution)

        Args:
            enable_nemesis: True to start, False to stop
            operation_context: Operation context for logging
            existing_log: Existing log to append info to

        Returns:
            List of strings with operation log containing:
            - Operation status per host
            - Timing information
            - Error details if any

        Note:
            This method handles both binary deployment and service management
            when starting nemesis
        """
        # Create summary log for Allure
        nemesis_log = existing_log if existing_log is not None else []

        try:
            # Get all unique cluster hosts
            nodes = self.nodes
            unique_hosts = list(sorted(set(filter(lambda h: h != 'localhost', [node.host for node in nodes]))))[:1]

            if enable_nemesis:
                action = "restart"
                action_name = "Starting"
                logging.info(
                    f"Starting nemesis on {
                        len(unique_hosts)} hosts in parallel"
                )

                # Deploy nemesis binary to all nodes
                nemesis_log.append(
                    f"Deploying nemesis binary to {len(unique_hosts)} hosts"
                )

                # Get path to nemesis binary
                nemesis_binary_path = os.getenv("NEMESIS_BINARY")
                if nemesis_binary_path:
                    nemesis_binary = yatest.common.binary_path(
                        nemesis_binary_path)
                else:
                    # Используем путь по умолчанию
                    nemesis_binary = yatest.common.binary_path(
                        "ydb/tests/tools/nemesis/driver/nemesis"
                    )

                nemesis_log.append(f"Nemesis binary path: {nemesis_binary}")

                # Create summary log for Allure for file operations
                file_ops_log = []
                file_ops_log.append(
                    f"Preparing nemesis configuration and binary on {
                        len(unique_hosts)} hosts in parallel"
                )

                # Add operation timing info
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                file_ops_log.append(f"Time: {current_time}")

                # Deploy nemesis binary using
                # deploy_binaries_to_hosts
                nemesis_binaries = [nemesis_binary]
                nemesis_deploy_path = "/Berkanavt/nemesis/bin/"

                file_ops_log.append(
                    f"Deploying nemesis binary to {len(unique_hosts)} hosts"
                )
                deploy_results = deploy_binaries_to_hosts(
                    nemesis_binaries, list(unique_hosts), nemesis_deploy_path
                )

                # Analyze deployment results
                successful_deploys = 0
                failed_deploys = 0
                deploy_errors = []

                for host in unique_hosts:
                    binary_result = deploy_results.get(
                        host, {}).get("nemesis", {})
                    success = binary_result.get("success", False)

                    if success:
                        successful_deploys += 1
                        file_ops_log.append(
                            f"  {host}: Binary deployed successfully to {
                                binary_result['path']}"
                        )
                    else:
                        failed_deploys += 1
                        error = binary_result.get("error", "Unknown error")
                        deploy_errors.append(f"{host}: {error}")
                        file_ops_log.append(
                            f"  {host}: Deployment failed - {error}")

                file_ops_log.append("\n--- Binary Deployment Summary ---")
                file_ops_log.append(
                    f"Successful deployments: {successful_deploys}/{
                        len(unique_hosts)}"
                )
                file_ops_log.append(
                    f"Failed deployments: {failed_deploys}/{len(unique_hosts)}"
                )

                if deploy_errors:
                    file_ops_log.append("\nDeployment errors:")
                    for error in deploy_errors:
                        file_ops_log.append(f"- {error}")

                # Function to perform file operations on single host
                def prepare_nemesis_config(host):
                    host_log = []
                    host_log.append(f"\n--- {host} ---")
                    logging.info(
                        f"Starting nemesis config preparation for {host}")

                    try:
                        # 1. Set execute permissions for nemesis
                        chmod_cmd = "sudo chmod +x /Berkanavt/nemesis/bin/nemesis"
                        chmod_result = execute_command(
                            host=host, cmd=chmod_cmd, raise_on_error=False, timeout=90
                        )

                        chmod_stderr = (
                            chmod_result.stderr if chmod_result.stderr else ""
                        )
                        if chmod_stderr and "error" in chmod_stderr.lower():
                            error_msg = f"Error setting executable permissions on {host}: {chmod_stderr}"
                            host_log.append(error_msg)
                            return {
                                "host": host,
                                "success": False,
                                "error": error_msg,
                                "log": host_log,
                            }
                        else:
                            host_log.append(
                                "Set executable permissions for nemesis")

                        # 2. Delete cluster.yaml
                        delete_cmd = "sudo rm -f /Berkanavt/kikimr/cfg/cluster.yaml"
                        delete_result = execute_command(
                            host=host, cmd=delete_cmd, raise_on_error=False
                        )

                        delete_stderr = (
                            delete_result.stderr if delete_result.stderr else ""
                        )
                        if delete_stderr and "error" in delete_stderr.lower():
                            error_msg = f"Error deleting cluster.yaml on {host}: {delete_stderr}"
                            host_log.append(error_msg)
                            return {
                                "host": host,
                                "success": False,
                                "error": error_msg,
                                "log": host_log,
                            }
                        else:
                            host_log.append("Deleted cluster.yaml")

                        # 3. Copy config.yaml to cluster.yaml
                        copy_result = self._copy_cluster_config(host, host_log)
                        if not copy_result["success"]:
                            return copy_result

                        logging.info(f"Completed nemesis config preparation for {host}")
                        return {"host": host, "success": True, "log": host_log}

                    except Exception as e:
                        error_msg = f"Exception on {host}: {e}"
                        host_log.append(error_msg)
                        logging.error(
                            f"Exception during nemesis config preparation for {host}: {e}"
                        )
                        return {
                            "host": host,
                            "success": False,
                            "error": error_msg,
                            "log": host_log,
                        }

                # Execute file operations in parallel
                file_ops_start_time = time_module.time()
                success_count = 0
                error_count = 0
                errors = []

                with ThreadPoolExecutor(
                    max_workers=min(len(unique_hosts), 20)
                ) as executor:
                    future_to_host = {
                        executor.submit(prepare_nemesis_config, host): host
                        for host in unique_hosts
                    }

                    for future in as_completed(future_to_host):
                        try:
                            result = future.result()
                            host_log = result["log"]
                            file_ops_log.extend(host_log)

                            if result["success"]:
                                success_count += 1
                            else:
                                error_count += 1
                                errors.append(result["error"])

                        except Exception as e:
                            host = future_to_host[future]
                            error_msg = f"Exception processing {host}: {e}"
                            file_ops_log.append(f"\n--- {host} ---")
                            file_ops_log.append(error_msg)
                            errors.append(error_msg)
                            error_count += 1

                file_ops_time = time_module.time() - file_ops_start_time

                # Add final file operations statistics
                file_ops_log.append("\n--- File Operations Summary ---")
                file_ops_log.append(
                    f"Successful hosts: {success_count}/{len(unique_hosts)}"
                )
                file_ops_log.append(
                    f"Failed hosts: {error_count}/{len(unique_hosts)}")
                file_ops_log.append(f"Execution time: {file_ops_time:.2f}s")

                if errors:
                    file_ops_log.append("\nErrors:")
                    for error in errors:
                        file_ops_log.append(f"- {error}")

                # Add file operations summary log to Allure
                allure.attach(
                    "\n".join(file_ops_log),
                    "Nemesis Config and Binary Preparation (Parallel)",
                    attachment_type=allure.attachment_type.TEXT,
                )
            else:
                action = "stop"
                action_name = "Stopping"
                logging.info(
                    f"Stopping nemesis on {
                        len(unique_hosts)} hosts in parallel"
                )

            # Add operation info to log
            if operation_context:
                nemesis_log.append(
                    f"{operation_context}: {action_name} nemesis service on {
                        len(unique_hosts)} hosts in parallel"
                )
            else:
                nemesis_log.append(
                    f"{action_name} nemesis service on {
                        len(unique_hosts)} hosts in parallel"
                )

            # Add start/stop timing info
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            nemesis_log.append(f"Time: {current_time}")

            # Function to execute service command on single host
            def execute_service_command(host):
                host_log = []
                host_log.append(f"\n--- {host} ---")

                try:
                    cmd = f"sudo service nemesis {action}"
                    result = execute_command(
                        host=host, cmd=cmd, raise_on_error=False, timeout=90)

                    stdout = result.stdout if result.stdout else ""
                    stderr = result.stderr if result.stderr else ""

                    if stderr and "error" in stderr.lower():
                        error_msg = f"Error on {host}: {stderr}"
                        host_log.append(error_msg)

                        # Only create separate step and log for errors
                        with allure.step(
                            f"Error {action.lower()}ing nemesis on {host}"
                        ):
                            allure.attach(
                                f"Command: {cmd}\nstdout: {stdout}\nstderr: {stderr}",
                                f"Nemesis {action} error",
                                attachment_type=allure.attachment_type.TEXT,
                            )
                            logging.warning(
                                f"Error during nemesis {action} on {host}: {stderr}"
                            )

                        return {
                            "host": host,
                            "success": False,
                            "error": error_msg,
                            "log": host_log,
                        }
                    else:
                        host_log.append("Success")
                        return {"host": host, "success": True, "log": host_log}

                except Exception as e:
                    error_msg = f"Exception on {host}: {e}"
                    host_log.append(error_msg)
                    return {
                        "host": host,
                        "success": False,
                        "error": error_msg,
                        "log": host_log,
                    }

            # Execute service commands in parallel
            service_start_time = time_module.time()
            success_count = 0
            error_count = 0
            errors = []

            with ThreadPoolExecutor(max_workers=min(len(unique_hosts), 10)) as executor:
                future_to_host = {
                    executor.submit(execute_service_command, host): host
                    for host in unique_hosts
                }

                for future in as_completed(future_to_host):
                    try:
                        result = future.result()
                        host_log = result["log"]
                        nemesis_log.extend(host_log)

                        if result["success"]:
                            success_count += 1
                        else:
                            error_count += 1
                            errors.append(result["error"])

                    except Exception as e:
                        host = future_to_host[future]
                        error_msg = f"Exception processing {host}: {e}"
                        nemesis_log.append(f"\n--- {host} ---")
                        nemesis_log.append(error_msg)
                        errors.append(error_msg)
                        error_count += 1

            service_time = time_module.time() - service_start_time

            # Add final statistics
            nemesis_log.append("\n--- Summary ---")
            nemesis_log.append(
                f"Successful hosts: {success_count}/{len(unique_hosts)}")
            nemesis_log.append(
                f"Failed hosts: {error_count}/{len(unique_hosts)}")
            nemesis_log.append(f"Service operations time: {service_time:.2f}s")

            if errors:
                nemesis_log.append("\nErrors:")
                for error in errors:
                    nemesis_log.append(f"- {error}")

            # Set nemesis startup flag
            if enable_nemesis:
                if error_count == 0:
                    # Complete success
                    self.nemesis_started = True
                    nemesis_log.append("Nemesis service started successfully on all hosts")
                elif error_count < len(unique_hosts):
                    # Partial success - create ClusterCheck record with warning
                    self.nemesis_started = True  # Consider nemesis as partially working
                    nemesis_log.append(f"Nemesis service started partially: {success_count}/{len(unique_hosts)} hosts")

                    cluster_issue = create_cluster_issue(
                        "nemesis_partial_startup",
                        f"Nemesis started on {success_count}/{len(unique_hosts)} hosts. Failed hosts: {error_count}. Errors: {'; '.join(errors[:3])}{'...' if len(errors) > 3 else ''}",
                        success_count
                    )

                    test_event_report(
                        nemesis_enabled=enable_nemesis,
                        workload_names=stress_util_names,
                        event_kind='ClusterCheck',
                        verification_phase="nemesis_management",
                        check_type="nemesis_partial_failure",
                        cluster_issue=cluster_issue
                    )
                else:
                    # Complete failure - already handled in except block above via raise Exception
                    self.nemesis_started = False
                    nemesis_log.append("Nemesis service failed to start on all hosts")
            else:
                self.nemesis_started = False
                nemesis_log.append("Nemesis service stopped successfully")

            # Add summary log to Allure
            allure.attach(
                "\n".join(nemesis_log),
                f"Nemesis {action_name} Summary (Parallel)",
                attachment_type=allure.attachment_type.TEXT,
            )

            return nemesis_log

        except Exception as e:
            # Create problem information and report it
            cluster_issue = create_cluster_issue(
                f"nemesis_{action_name.lower()}_exception",
                f"Exception during nemesis {action_name.lower()}: {e}",
                0
            )

            test_event_report(
                nemesis_enabled=enable_nemesis,
                workload_names=stress_util_names,
                event_kind='ClusterCheck',
                verification_phase="nemesis_management",
                check_type=f"nemesis_{action_name.lower()}_exception",
                cluster_issue=cluster_issue
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
            host, self.cluster_path, "/Berkanavt/nemesis/cfg/config.yaml",
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

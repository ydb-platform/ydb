import traceback
import uuid
import allure
import logging
import time as time_module
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from ydb.tests.library.stability.aggregate_results import (
    StressUtilDeployResult,
    StressUtilTestResults,
    StressUtilNodeResult,
    StressUtilResult,
    StressUtilRunResult
)
from ydb.tests.library.stability.deploy import StressUtilDeployer

from ydb.tests.library.stability import deploy
from ydb.tests.olap.lib.utils import external_param_is_true
from ydb.tests.olap.lib.remote_execution import execute_command


class StressRunExecutor:
    def __init__(self):
        self._ignore_stderr_content = external_param_is_true('ignore_stderr_content')

    def __substitute_variables_in_template(
        self,
        command_args_template: str,
        target_node,
        run_config: dict
    ) -> str:
        """
        Substitutes variables in command_args template

        Supported variables:
        - {node_host} - node host
        - {iteration_num} - iteration number
        - {thread_id} - thread ID (usually node host)
        - {run_id} - unique run ID
        - {timestamp} - run timestamp
        - {uuid} - short UUID

        Args:
            command_args_template: Command line arguments template
            target_node: Target node for execution
            run_config: Run configuration

        Returns:
            String with substituted variables
        """

        # Get variable values
        node_host = target_node.host
        iteration_num = run_config.get("iteration_num", 1)
        thread_id = run_config.get("thread_id", node_host)
        timestamp = int(time_module.time())
        short_uuid = uuid.uuid4().hex[:8]

        # Create unique run_id
        run_id = f"{node_host}_{iteration_num}_{timestamp}"

        # Substitution dictionary
        substitutions = {
            "{node_host}": node_host,
            "{iteration_num}": str(iteration_num),
            "{thread_id}": str(thread_id),
            "{run_id}": run_id,
            "{timestamp}": str(timestamp),
            "{uuid}": short_uuid,
        }

        # Perform substitutions
        result = command_args_template
        for placeholder, value in substitutions.items():
            result = result.replace(placeholder, value)

        return result

    def execute_stress_runs(
        self,
        stress_deployer: StressUtilDeployer,
        workload_params: dict,
        duration_value: float,
        preparation_result: dict[str, StressUtilDeployResult],
        nemesis: bool = False,
    ) -> StressUtilTestResults:
        """
        PHASE 2: Parallel workload execution on all nodes

        Args:
            stress_deployer: Stress test deployer instance
            workload_params: Dictionary of workload configurations
            duration_value: Execution duration in seconds
            preparation_result: Preparation phase results
            nemesis: Whether to start nemesis service

        Returns:
            StressUtilTestResults object containing:
            - overall_result: Aggregated test results
            - successful_runs: Count of successful runs
            - total_runs: Total count of runs
            - total_execution_time: Total execution time
            - workload_start_time: Timestamp when workload started
            - deployed_nodes: Nodes where workload was deployed
            - node_results: Individual node results
        """

        with allure.step("Phase 2: Execute workload runs in parallel"):
            deployed_nodes = preparation_result["deployed_nodes"]
            execution_result = StressUtilTestResults()

            execution_result.start_time = time_module.time()
            if not deployed_nodes:
                logging.error("No deployed nodes available for execution")
                return {
                    # "overall_result": overall_result,
                    "successful_runs": 0,
                    "total_runs": 0,
                    "total_execution_time": 0,
                    "workload_start_time": time_module.time(),
                    "deployed_nodes": [],
                }

            # Parallel execution on all nodes
            with allure.step(
                f"Execute workload in parallel on {preparation_result['total_hosts']} nodes"
            ):
                workload_start_time = time_module.time()

                # Start nemesis 15 seconds after workload
                # execution begins
                if nemesis:
                    # Set flag immediately when nemesis thread starts
                    deploy.nemesis_started = True
                    logging.info("Nemesis flag set to True - will start in 15 seconds")

                    nemesis_thread = threading.Thread(
                        target=stress_deployer.delayed_nemesis_start,
                        args=(15,),  # 15 секунд задержки
                        daemon=False,  # Remove daemon=True so thread isn't interrupted
                    )
                    nemesis_thread.start()
                    logging.info("Scheduled nemesis to start in 15 seconds")

                # Function to execute workload on a single node
                def execute_on_node(workload_config, stress_name, node) -> StressUtilNodeResult:
                    node_host = node['node'].host
                    deployed_binary_path = node['binary_path']

                    node_result = StressUtilNodeResult()
                    node_result.stress_name = stress_name
                    node_result.node = node['node']
                    node_result.host = node_host
                    node_result.successful_runs = 0
                    node_result.total_runs = 0
                    node_result.runs = []
                    start_time = time_module.time()
                    node_result.start_time = time_module.time()
                    node_result.total_execution_time = 0

                    logging.info(f"Starting execution on node {node_host}")

                    planned_end_time = node_result.start_time + duration_value

                    run_duration = duration_value
                    current_iteration = 0
                    # Execute plan for this node
                    while time_module.time() < planned_end_time:

                        # Use iter_N format without adding iter_ prefix in _execute_single_workload_run
                        # since it will be added there
                        run_name = f"{stress_name}_{node_host}_iter_{current_iteration}"

                        # Set flag that iter_ prefix is already added
                        run_config_copy = {}
                        run_config_copy["iteration_num"] = current_iteration
                        run_config_copy["node_host"] = node_host
                        run_config_copy["duration"] = round(run_duration)
                        run_config_copy["node_role"] = node['node'].role
                        run_config_copy["thread_id"] = (
                            node_host  # Идентификатор потока - хост ноды
                        )

                        # Execute one run
                        success, execution_time, stdout, stderr, is_timeout = (
                            self._execute_single_workload_run(
                                deployed_binary_path,
                                node['node'],
                                run_name,
                                ' '.join(workload_config['args']),
                                '--duration',
                                run_config_copy,
                            )
                        )

                        # Save run result
                        run_result = StressUtilRunResult()
                        run_result.run_config = run_config_copy
                        run_result.iteration_number = current_iteration
                        run_result.is_success = success
                        run_result.is_timeout = is_timeout
                        run_result.execution_time = execution_time
                        run_result.start_time = start_time
                        run_result.end_time = time_module.time()
                        run_result.stdout = stdout
                        run_result.stderr = stderr

                        start_time = time_module.time()
                        node_result.runs.append(run_result)

                        # Update node statistics
                        node_result.total_execution_time += execution_time
                        if success:
                            logging.info(
                                f"Run {current_iteration} on {node_host} completed successfully"
                            )
                        else:
                            logging.warning(
                                f"Run {current_iteration} on {node_host} failed")
                        current_iteration += 1
                        run_duration = planned_end_time - time_module.time()
                    node_result.end_time = time_module.time()
                    logging.info(
                        f"Execution on {node_host} completed: "
                        f"{node_result.get_successful_runs()}/{node_result.get_total_runs()} successful"
                    )

                    return node_result

                # Start parallel execution on all nodes
                with ThreadPoolExecutor(
                    max_workers=len(workload_params) * len(preparation_result["total_hosts"])
                ) as executor:
                    future_to_node = {}
                    for name, stress_config in workload_params.items():
                        execution_result.stress_util_runs[name] = StressUtilResult()
                        execution_result.stress_util_runs[name].execution_args = stress_config['args']
                        execution_result.stress_util_runs[name].start_time = time_module.time()
                        for node in deployed_nodes[name].nodes:
                            future_to_node[executor.submit(execute_on_node, stress_config, name, node)] = (name, node)

                    for future in as_completed(future_to_node):
                        try:
                            node_result = future.result()
                            execution_result.stress_util_runs[node_result.stress_name].end_time = time_module.time()
                            execution_result.stress_util_runs[node_result.stress_name].node_runs[node_result.host] = node_result
                        except Exception as e:
                            node_plan = future_to_node[future]
                            node_host = node_plan[1]['node'].host
                            logging.error(
                                f"Error executing on {node_host}: {e}")
                            logging.error(traceback.format_exc())
                            # Add error information
                            error_result = StressUtilNodeResult()
                            error_result.node = node_plan[1]['node']
                            error_result.host = node_host
                            error_result.stress_name = node_plan[0]
                            error_result.runs = []
                            error_result.total_execution_time = 0
                            execution_result.stress_util_runs[node_plan[0]].node_runs[node_host] = error_result

                # Process results from all nodes
                successful_runs = sum(execution_result.get_stress_successful_runs(stress_name) for stress_name in execution_result.stress_util_runs.keys())
                total_runs = sum(execution_result.get_stress_total_runs(stress_name) for stress_name in execution_result.stress_util_runs.keys())
                total_execution_time = time_module.time() - workload_start_time

                logging.info(
                    f"Parallel execution completed: {successful_runs}/{total_runs} successful runs "
                    f"on {max(len(nodes.nodes) for nodes in deployed_nodes.values())} nodes"
                )
                execution_result.end_time = time_module.time()
                return {
                    "overall_result": execution_result,
                    "successful_runs": successful_runs,
                    "total_runs": total_runs,
                    "total_execution_time": total_execution_time,
                    "workload_start_time": workload_start_time,
                    "deployed_nodes": deployed_nodes,
                }

    def _execute_single_workload_run(
        self,
        deployed_binary_path: str,
        target_node,
        run_name: str,
        command_args_template: str,
        duration_param: str,
        run_config: dict,
    ):
        """
        Executes a single workload run with detailed logging and timeout handling

        Args:
            deployed_binary_path: Path to workload binary on target node
            target_node: Node object where workload will run
            run_name: Base name for the run (used in logging)
            command_args_template: Command arguments with placeholders
            duration_param: CLI parameter name for duration (e.g. '--duration')
            run_config: Dictionary containing:
                - duration: Planned run duration in seconds
                - iteration_num: Current iteration number
                - node_host: Target node hostname
                - node_role: Node role/type
                - thread_id: Worker thread identifier

        Returns:
            Tuple containing:
            - success: Boolean indicating if run succeeded
            - execution_time: Actual run duration in seconds
            - stdout: Standard output from command
            - stderr: Standard error from command
            - is_timeout: Boolean indicating if run timed out

        Note:
            - Automatically adds buffer time to timeout
            - Attaches full execution details to Allure report
            - Handles command output buffering properly
        """
        # Substitute variables in command_args_template for unique paths
        command_args = self.__substitute_variables_in_template(
            command_args_template, target_node, run_config
        )

        # Build command
        if duration_param is None:
            # Use command_args as-is (for backward compatibility)
            pass
        else:
            # Add duration parameter
            command_args = (
                f"{command_args} {duration_param} {
                    run_config['duration']}"
            )

        run_start_time = time_module.time()

        try:
            with allure.step(f"Execute {run_name}"):
                allure.attach(
                    f"Run config: {run_config}",
                    "Run Info",
                    attachment_type=allure.attachment_type.TEXT,
                )
                allure.attach(
                    command_args,
                    "Command Arguments",
                    attachment_type=allure.attachment_type.TEXT,
                )
                allure.attach(
                    f"Target host: {target_node.host}",
                    "Execution Target",
                    attachment_type=allure.attachment_type.TEXT,
                )

                # Build and execute command
                with allure.step("Execute workload command"):
                    # Disable buffering to ensure output capture
                    cmd = f"stdbuf -o0 -e0 {deployed_binary_path} {command_args}"

                    run_timeout = (
                        run_config["duration"] + 150
                    )  # Add buffer for completion

                    allure.attach(
                        cmd, "Full Command", attachment_type=allure.attachment_type.TEXT
                    )
                    allure.attach(
                        f"Timeout: {int(run_timeout)}s",
                        "Execution Timeout",
                        attachment_type=allure.attachment_type.TEXT,
                    )

                    execution_result = execute_command(
                        target_node.host,
                        cmd,
                        raise_on_error=False,
                        timeout=int(run_timeout),
                        raise_on_timeout=False,
                    )

                    stdout = execution_result.stdout
                    stderr = execution_result.stderr
                    is_timeout = execution_result.is_timeout

                    # Attach command execution results
                    if stdout:
                        allure.attach(
                            stdout,
                            "Command Stdout",
                            attachment_type=allure.attachment_type.TEXT,
                        )
                    else:
                        allure.attach(
                            "(empty)",
                            "Command Stdout",
                            attachment_type=allure.attachment_type.TEXT,
                        )

                    if stderr:
                        allure.attach(
                            stderr,
                            "Command Stderr",
                            attachment_type=allure.attachment_type.TEXT,
                        )
                    else:
                        allure.attach(
                            "(empty)",
                            "Command Stderr",
                            attachment_type=allure.attachment_type.TEXT,
                        )

                    if self._ignore_stderr_content:
                        success = not is_timeout
                    else:
                        # success=True only if stderr is empty (excluding SSH
                        # warnings) AND no timeout
                        success = not bool(stderr.strip()) and not is_timeout

                    execution_time = time_module.time() - run_start_time

                    logging.info(
                        f"{run_name} completed in {
                            execution_time: .1f}s, success: {success}, timeout: {is_timeout}"
                    )

                    return success, execution_time, stdout, stderr, is_timeout

        except Exception as e:
            execution_time = time_module.time() - run_start_time
            error_msg = f"Exception in {run_name}: {e}"
            logging.error(error_msg)
            return False, execution_time, "", error_msg, False

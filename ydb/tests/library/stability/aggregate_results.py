from typing import Optional

from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class RunConfigInfo:
    test_start_time: int = None
    nodes_percentage: float = None
    nemesis_enabled: bool = None
    table_type: str = None
    stres_util_args: str = None
    duration: float = None
    all_hosts: list[str] = None


class StressUtilRunResult:
    run_config: dict = None
    is_success: bool = None
    is_timeout: bool = None
    iteration_number: int = None
    stdout: str = None
    stderr: str = None
    start_time: float = None
    end_time: float = None
    execution_time: str = None


class StressUtilNodeResult:
    """Aggregates results of multiple runs for a single stress test

    Attributes:
        stress_name: Name of stress util
        node: Node identifier where test ran
        host: Hostname where test ran
        start_time: Timestamp when test started
        end_time: Timestamp when test ended
        total_execution_time: Total duration across all runs
        successful_runs: Count of successful runs
        total_runs: Total count of runs
        runs: List of individual run results
    """
    stress_name: str = None
    node: str = None
    host: str = None
    start_time: float = None
    end_time: float = None
    total_execution_time: str = None
    runs: list[StressUtilRunResult] = []

    def __init__(self):
        """Initialize StressUtilNodeResult with empty runs list"""
        self.runs = []

    def __repr__(self):
        return (f"StressUtilNodeResult(stress_name={self.stress_name}, "
                f"node={self.node}, host={self.host}, "
                f"start_time={self.start_time}, "
                f"end_time={self.end_time}, "
                f"total_execution_time={self.total_execution_time}, "
                f"runs={self.runs})")

    def get_successful_runs(self) -> int:
        """Get count of successful runs

        Returns:
            int: Total count of successful runs
        """
        return sum(run_result.is_success for run_result in self.runs)

    def get_total_runs(self) -> int:
        """Get total count of runs

        Returns:
            int: Total count of runs
        """
        return len(self.runs)

    def is_all_success(self) -> bool:
        """Check if all runs across all nodes were successful

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.get_successful_runs() == self.get_total_runs()


class StressUtilResult:
    """Aggregates results of multiple runs for a single stress test

    Attributes:
        start_time: Timestamp when test started
        end_time: Timestamp when test ended
        total_execution_time: Total duration across all runs
        runs: List of individual run results
    """
    start_time: float = None
    end_time: float = None
    execution_args: list[str] = None
    node_runs: dict[str, StressUtilNodeResult] = dict()

    def __init__(self):
        """Initialize StressUtilResult with empty runs list"""
        self.node_runs = dict()
        self.execution_args = []

    def __repr__(self):
        return f"StressUtilResult(start_time={self.start_time}, end_time={self.end_time}, node_runs={self.node_runs})"

    def get_successful_runs(self) -> int:
        """Get count of successful runs

        Returns:
            int: Total count of successful runs
        """
        return sum(run_result.get_successful_runs() for run_result in self.node_runs.values())

    def get_total_runs(self) -> int:
        """Get total count of runs

        Returns:
            int: Total count of runs
        """
        return sum(run_result.get_total_runs() for run_result in self.node_runs.values())

    def is_all_success(self) -> bool:
        """Check if all runs across all nodes were successful

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.get_total_runs() == self.get_successful_runs()


class StressUtilDeployResult:
    """Stores deployment results for a stress test

    Attributes:
        stress_name: Name of the stress test
        nodes: List of nodes where test was deployed
        hosts: List of hostnames where test was deployed
    """
    stress_name: str = None
    nodes: list[YdbCluster.Node] = None
    hosts: list[str] = None

    def __init__(self):
        """Initialize StressUtilDeployResult with empty nodes/hosts"""
        self.nodes = None
        self.hosts = None


class StressUtilTestResults:
    """Aggregates results from all stress test runs

    Attributes:
        start_time: Timestamp when testing started
        end_time: Timestamp when testing ended
        stress_util_runs: Dictionary mapping test names to their results
        nemesis_deploy_results: Dictionary of nemesis deployment results
        errors: List of error messages
        error_message: Combined error message string
    """
    start_time: float = None
    end_time: float = None
    stress_util_runs: dict[str, StressUtilResult] = dict()
    nemesis_deploy_results: dict[str, list[str]] = dict()
    errors: list[str] = []
    error_message: str = ''

    def __init__(self):
        """Initialize StressUtilTestResults with empty collections"""
        self.start_time = None
        self.end_time = None
        self.stress_util_runs = dict()
        self.nemesis_deploy_results = dict()
        self.errors = []
        self.error_message = ''

    def add_error(self, msg: Optional[str]) -> bool:
        """Add an error message to the test results

        Args:
            msg: Error message to add (can be None)

        Returns:
            bool: True if message was added, False if msg was None
        """
        if msg:
            self.errors.append(msg)
            if len(self.error_message) > 0:
                self.error_message += f'\n\n{msg}'
            else:
                self.error_message = msg
            return True
        return False

    def get_stress_successful_runs(self, stress_name: str) -> int:
        """Get count of successful runs for a specific stress test

        Args:
            stress_name: Name of the stress test to query

        Returns:
            int: Total count of successful runs
        """
        return self.stress_util_runs[stress_name].get_successful_runs()

    def get_stress_total_runs(self, stress_name: str) -> int:
        """Get total count of runs for a specific stress test

        Args:
            stress_name: Name of the stress test to query

        Returns:
            int: Total count of runs
        """
        return self.stress_util_runs[stress_name].get_total_runs()

    def is_success_for_a_stress(self, stress_name: str) -> bool:
        """Check if all runs in a stress test were successful

        Args:
            stress_name: Name of the stress test to query

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.stress_util_runs[stress_name].is_all_success()

    def is_all_success(self) -> bool:
        """Check if all runs across all stress tests were successful

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return all(result.is_all_success() for result in self.stress_util_runs.values())

    def get_successful_runs(self) -> int:
        """Get count of successful runs

        Returns:
            int: Total count of successful runs
        """
        return sum(stress_info.get_successful_runs() for stress_info in self.stress_util_runs.values())

    def get_total_runs(self) -> int:
        """Get total count of runs

        Returns:
            int: Total count of runs
        """
        return sum(stress_info.get_total_runs() for stress_info in self.stress_util_runs.values())

    def __repr__(self):
        return (
            f"StressUtilTestResults(start_time={self.start_time}, "
            f"end_time={self.end_time}, "
            f"stress_util_runs={self.stress_util_runs}, "
            f"nemesis_deploy_results={self.nemesis_deploy_results}, "
            f"errors={self.errors})"
        )

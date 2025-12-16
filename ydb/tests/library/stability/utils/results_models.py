from typing import Optional, Self
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class RunConfigInfo:
    """Configuration information for test runs.

    Attributes:
        test_start_time: Test start timestamp
        nodes_percentage: Percentage of nodes used (1-100)
        nemesis_enabled: Whether nemesis was enabled
        table_type: Type of table used in test
        duration: Test duration in seconds
        all_hosts: List of all hostnames used
        stress_util_names: List of stress utility names
    """
    test_start_time: Optional[int] = None
    nodes_percentage: Optional[float] = None
    nemesis_enabled: Optional[bool] = None
    table_type: Optional[str] = None
    duration: Optional[float] = None
    all_hosts: Optional[list[str]] = None
    stress_util_names: Optional[list[str]] = None


class StressUtilRunResult:
    """Results of a single stress test run iteration.

    Attributes:
        run_config: Run configuration dictionary
        is_success: Whether run succeeded
        is_timeout: Whether run timed out
        iteration_number: Iteration number
        stdout: Standard output
        stderr: Standard error
        start_time: Run start timestamp
        end_time: Run end timestamp
        execution_time: Run duration in seconds
    """
    run_config: Optional[dict] = None
    is_success: Optional[bool] = None
    is_timeout: Optional[bool] = None
    iteration_number: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    execution_time: Optional[float] = None


class StressUtilNodeResult:
    """Aggregates results of multiple runs for a single stress test on a node.

    Attributes:
        stress_name: Name of stress utility
        node: Node identifier where test ran
        host: Hostname where test ran
        start_time: Test start timestamp
        end_time: Test end timestamp
        total_execution_time: Total duration across all runs (seconds)
        runs: List of individual run results
    """
    stress_name: Optional[str] = None
    node: Optional[str] = None
    host: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    total_execution_time: Optional[float] = None
    runs: list[StressUtilRunResult] = []

    def __init__(self) -> None:
        """Initialize StressUtilNodeResult with empty runs list."""
        self.runs = []

    def __repr__(self):
        return (f"StressUtilNodeResult(stress_name={self.stress_name}, "
                f"node={self.node}, host={self.host}, "
                f"start_time={self.start_time}, "
                f"end_time={self.end_time}, "
                f"total_execution_time={self.total_execution_time}, "
                f"runs={self.runs})")

    def get_successful_runs(self) -> int:
        """Count successful runs across all nodes.

        Returns:
            int: Total successful runs
        """
        return sum(run_result.is_success for run_result in self.runs)

    def get_total_runs(self) -> int:
        """Count total runs across all nodes.

        Returns:
            int: Total runs
        """
        return len(self.runs)

    def is_all_success(self) -> bool:
        """Check if all runs across all nodes succeeded.

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.get_successful_runs() == self.get_total_runs()


class StressUtilResult:
    """Aggregates results of multiple runs for a single stress test utility.

    Attributes:
        start_time: Test start timestamp
        end_time: Test end timestamp
        execution_args: List of execution arguments
        node_runs: Dictionary of node results by hostname
    """
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    execution_args: Optional[list[str]] = None
    node_runs: dict[str, StressUtilNodeResult] = dict()

    def __init__(self) -> None:
        """Initialize StressUtilResult with empty collections."""
        self.node_runs = dict()
        self.execution_args = []

    def __repr__(self):
        return f"StressUtilResult(start_time={self.start_time}, end_time={self.end_time}, node_runs={self.node_runs})"

    def get_successful_runs(self) -> int:
        """Count successful runs on this node.

        Returns:
            int: Number of successful runs
        """
        return sum(run_result.get_successful_runs() for run_result in self.node_runs.values())

    def get_total_runs(self) -> int:
        """Count total runs on this node.

        Returns:
            int: Total number of runs
        """
        return sum(run_result.get_total_runs() for run_result in self.node_runs.values())

    def is_all_success(self) -> bool:
        """Check if all runs on this node succeeded.

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.get_total_runs() == self.get_successful_runs()


class StressUtilDeployResult:
    """Stores deployment results for a stress test utility.

    Attributes:
        stress_name: Name of stress utility
        nodes: List of nodes where deployed
        hosts: List of hostnames where deployed
    """
    stress_name: Optional[str] = None
    nodes: Optional[list[YdbCluster.Node]] = None
    hosts: Optional[list[str]] = None

    def __init__(self) -> None:
        """Initialize StressUtilDeployResult with empty collections."""
        self.nodes = None
        self.hosts = None


class StressUtilTestResults:
    """Aggregates results from all stress test utility runs.

    Attributes:
        start_time: Test start timestamp
        end_time: Test end timestamp
        stress_util_runs: Dictionary of results by utility name
        nemesis_deploy_results: Nemesis deployment results
        errors: List of error messages
        error_message: Combined error message
    """
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    stress_util_runs: dict[str, StressUtilResult] = dict()
    nemesis_deploy_results: dict[str, list[str]] = dict()
    errors: list[str] = []
    error_message: str = ''
    recoverability_result: Optional[Self] = None

    def __init__(self) -> None:
        """Initialize StressUtilTestResults with empty collections."""
        self.start_time = None
        self.end_time = None
        self.stress_util_runs = dict()
        self.nemesis_deploy_results = dict()
        self.errors = []
        self.error_message = ''

    def add_error(self, msg: Optional[str]) -> bool:
        """Add error message to test results.

        Args:
            msg: Error message to add (can be None)

        Returns:
            bool: True if message added, False if None
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
        """Count successful runs for specific stress utility.

        Args:
            stress_name: Name of stress utility

        Returns:
            int: Number of successful runs
        """
        return self.stress_util_runs[stress_name].get_successful_runs()

    def get_stress_total_runs(self, stress_name: str) -> int:
        """Count total runs for specific stress utility.

        Args:
            stress_name: Name of stress utility

        Returns:
            int: Total number of runs
        """
        return self.stress_util_runs[stress_name].get_total_runs()

    def is_success_for_a_stress(self, stress_name: str) -> bool:
        """Check if all runs succeeded for specific stress utility.

        Args:
            stress_name: Name of stress utility

        Returns:
            bool: True if all runs succeeded, False otherwise
        """
        return self.stress_util_runs[stress_name].is_all_success()

    def is_all_success(self) -> bool:
        """Check if all runs across all stress utilities succeeded.

        Returns:
            bool: True if all succeeded, False otherwise
        """
        return all(result.is_all_success() for result in self.stress_util_runs.values())

    def get_successful_runs(self) -> int:
        """Count successful runs across all stress utilities.

        Returns:
            int: Total successful runs
        """
        return sum(stress_info.get_successful_runs() for stress_info in self.stress_util_runs.values())

    def get_total_runs(self) -> int:
        """Count total runs across all stress utilities.

        Returns:
            int: Total runs
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

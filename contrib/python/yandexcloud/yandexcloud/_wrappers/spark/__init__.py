# pylint: disable=no-member
# pylint: disable=duplicate-code
# mypy: ignore-errors
import logging
import random
from dataclasses import dataclass
from typing import Dict, List, Optional

import yandex.cloud.spark.v1.cluster_pb2 as cluster_pb
import yandex.cloud.spark.v1.cluster_service_pb2 as cluster_service_pb
import yandex.cloud.spark.v1.cluster_service_pb2_grpc as cluster_service_grpc_pb
import yandex.cloud.spark.v1.job_pb2 as job_pb
import yandex.cloud.spark.v1.job_service_pb2 as job_service_pb
import yandex.cloud.spark.v1.job_service_pb2_grpc as job_service_grpc_pb
import yandex.cloud.spark.v1.maintenance_pb2 as maintenance_pb


@dataclass
class SparkClusterParameters:
    """
    Spark Cluster Parameters.

    Constructor arguments:
    :param folder_id: ID of the folder in which cluster should be created.
    :type folder_id: str
    :param service_account_id: Service account that will be used to access
        cloud resources.
    :type service_account_id: str
    :param name: Name of the Spark cluster. The name must be unique within
        the folder.
    :type name: str, optional
    :param description: Text description of the Spark cluster.
    :type description: str, optional
    :param labels: Cluster labels as key:value pairs.
    :type labels: Dict[str, str], optional
    :param subnet_ids: Network subnets.
    :type subnet_ids: List[str]
    :param security_group_ids: Network security groups.
    :type security_group_ids: List[str]
    :param deletion_protection: Deletion Protection inhibits deletion of
        the cluster.
    :type deletion_protection: bool
    :param driver_pool_resource_preset: Resource preset ID for Driver pool.
    :type driver_pool_resource_preset: str
    :param driver_pool_size: Node count for Driver pool with fixed size.
    :type driver_pool_size: int, optional
    :param driver_pool_min_size: Minimum node count for Driver pool with
        autoscaling.
    :type driver_pool_min_size: int, optional
    :param driver_pool_max_size: Maximum node count for Driver pool with
        autoscaling.
    :type driver_pool_max_size: int, optional
    :param executor_pool_resource_preset: Resource preset ID for Executor
        pool.
    :type executor_pool_resource_preset: str
    :param executor_pool_size: Node count for Executor pool with fixed size.
    :type executor_pool_size: int, optional
    :param executor_pool_min_size: Minimum node count for Executor pool
        with autoscaling.
    :type executor_pool_min_size: int, optional
    :param executor_pool_max_size: Maximum node count for Executor pool
        with autoscaling.
    :type executor_pool_max_size: int, optional
    :param logging_enabled: Enable sending logs to Cloud Logging.
    :type logging_enabled: bool, optional
    :param log_group_id: Log Group ID in Cloud Logging to store cluster
        logs.
    :type log_group_id: str, optional
    :param log_folder_id: Folder ID to store cluster logs in default log
        group.
    :type log_folder_id: str, optional
    :param history_server_enabled: Enable Spark History Server.
    :type history_server_enabled: bool, optional
    :param pip_packages: Python packages that need to be installed using pip
        (in pip requirement format).
    :type pip_packages: List[str], optional
    :param deb_packages: Deb-packages that need to be installed using system
        package manager.
    :type deb_packages: List[str], optional
    :param metastore_cluster_id: Metastore cluster ID for default spark
        configuration.
    :type metastore_cluster_id: str, optional
    :param maintenance_weekday: Weekday number for maintenance operations.
        From 1 - Monday to 7 - Sunday.
    :type maintenance_weekday: int, optional
    :param maintenance_hour: Hour of the day for maintenance oprerations.
        From 1 to 24.
    :type maintenance_hour: int, optional
    """

    # pylint: disable=too-many-instance-attributes

    folder_id: str
    service_account_id: str
    name: Optional[str] = None
    description: str = ""
    labels: Optional[Dict[str, str]] = None
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None
    deletion_protection: bool = False
    driver_pool_resource_preset: str = ""
    driver_pool_size: int = 0
    driver_pool_min_size: int = 0
    driver_pool_max_size: int = 0
    executor_pool_resource_preset: str = ""
    executor_pool_size: int = 0
    executor_pool_min_size: int = 0
    executor_pool_max_size: int = 0
    logging_enabled: bool = True
    log_group_id: Optional[str] = None
    log_folder_id: Optional[str] = None
    history_server_enabled: bool = True
    pip_packages: Optional[List[str]] = None
    deb_packages: Optional[List[str]] = None
    metastore_cluster_id: str = ""
    maintenance_weekday: Optional[int] = None
    maintenance_hour: Optional[int] = None


@dataclass
class SparkJobParameters:
    """
    Spark Job Parameters.

    Constructor arguments:
    :param name: Name of the job.
    :type name: str, optional
    :param main_jar_file_uri: URI of the jar file that contains the main
        class.
    :type main_jar_file_uri: str
    :param main_class: Name of the main class of the job.
    :type main_class: str, optional
    :param args: Arguments to be passed to the job.
    :type args: List[str], optional
    :param properties: Spark properties for the job.
    :type properties: Dist[str, str], optional
    :param packages: List of maven coordinates of jars to include on the
        driver and executor classpaths.
    :type packages: List[str], optional
    :param file_uris: URIs of files to be placed in the working directory.
    :type file_uris: List[str], optional
    :param jar_file_uris: URIs of JAR dependencies to be added to the
        CLASSPATH.
    :type jar_archive_uris: List[str], optional
    :param archive_uris: URIs of archives to be extracted into the working
        directory.
    :type archive_uris: List[str], optional
    :param repositories: List of additional remote repositories to search
        for the maven coordinates given with --packages.
    :type repositories: List[str], optional
    :param exclude_packages: List of groupId:artifactId, to exclude while
        resolving the dependencies provided in --packages to avoid
        dependency conflicts.
    :type exclude_packages: List[str], optional
    """

    # pylint: disable=too-many-instance-attributes

    name: str = ""
    main_jar_file_uri: str = ""
    main_class: str = ""
    args: Optional[List[str]] = None
    properties: Optional[Dict[str, str]] = None
    packages: Optional[List[str]] = None
    file_uris: Optional[List[str]] = None
    jar_file_uris: Optional[List[str]] = None
    archive_uris: Optional[List[str]] = None
    repositories: Optional[List[str]] = None
    exclude_packages: Optional[List[str]] = None


@dataclass
class PysparkJobParameters:
    """
    Pyspark Job Parameters.

    Constructor arguments:
    :param name: Name of the job.
    :type name: str, optional
    :param main_python_file_uri: URI of the main Python file.
    :type main_python_file_uri: str
    :param args: Arguments to be passed to the job.
    :type args: List[str], optional
    :param properties: Spark properties for the job.
    :type properties: Dist[str, str], optional
    :param packages: List of maven coordinates of jars to include on the
        driver and executor classpaths.
    :type packages: List[str], optional
    :param file_uris: URIs of files to be placed in the working directory.
    :type file_uris: List[str], optional
    :param python_file_uris: URIs of python files used in the job.
    :type python_file_uris: List[str], optional
    :param jar_file_uris: URIs of JAR dependencies to be added to the
        CLASSPATH.
    :type jar_file_uris: List[str], optional
    :param archive_uris: URIs of archives to be extracted into the working
        directory.
    :type archive_uris: List[str], optional
    :param repositories: List of additional remote repositories to search
        for the maven coordinates given with --packages.
    :type repositories: List[str], optional
    :param exclude_packages: List of groupId:artifactId, to exclude while
        resolving the dependencies provided in --packages to avoid
        dependency conflicts.
    :type exclude_packages: List[str], optional
    """

    # pylint: disable=too-many-instance-attributes

    name: str = ""
    main_python_file_uri: str = ""
    args: Optional[List[str]] = None
    properties: Optional[Dict[str, str]] = None
    packages: Optional[List[str]] = None
    file_uris: Optional[List[str]] = None
    python_file_uris: Optional[List[str]] = None
    jar_file_uris: Optional[List[str]] = None
    archive_uris: Optional[List[str]] = None
    repositories: Optional[List[str]] = None
    exclude_packages: Optional[List[str]] = None


class Spark:
    """
    A base hook for Yandex.Cloud Managed Service for Apache Spark

    :param logger: Logger object
    :type logger: Optional[logging.Logger]
    :param sdk: SDK object. Normally is being set by Wrappers constructor
    :type sdk: yandexcloud.SDK
    """

    def __init__(self, logger=None, sdk=None):
        self.sdk = sdk or self.sdk
        self.log = logger
        if not self.log:
            self.log = logging.getLogger()
            self.log.addHandler(logging.NullHandler())
        self.cluster_id = None

    def create_cluster(self, spec: SparkClusterParameters) -> str:
        """
        Create cluster.

        :param spec: Cluster parameters.
        :type spec: SparkClusterParameters

        :return: Operation result
        :rtype: OperationResult
        """

        # pylint: disable=too-many-branches

        if not spec.folder_id:
            raise RuntimeError("Folder ID must be specified to create cluster.")

        if not spec.name:
            random_int = random.randint(0, 999)
            spec.name = f"spark-{random_int}"

        if spec.driver_pool_max_size > 0:
            driver_pool = cluster_pb.ResourcePool(
                resource_preset_id=spec.driver_pool_resource_preset,
                scale_policy=cluster_pb.ScalePolicy(
                    auto_scale=cluster_pb.ScalePolicy.AutoScale(
                        min_size=spec.driver_pool_min_size,
                        max_size=spec.driver_pool_max_size,
                    ),
                ),
            )
        elif spec.driver_pool_size > 0:
            driver_pool = cluster_pb.ResourcePool(
                resource_preset_id=spec.driver_pool_resource_preset,
                scale_policy=cluster_pb.ScalePolicy(
                    fixed_scale=cluster_pb.ScalePolicy.FixedScale(
                        size=spec.driver_pool_size,
                    ),
                ),
            )
        else:
            raise RuntimeError("Driver pool size is not specified.")

        if spec.executor_pool_max_size > 0:
            executor_pool = cluster_pb.ResourcePool(
                resource_preset_id=spec.executor_pool_resource_preset,
                scale_policy=cluster_pb.ScalePolicy(
                    auto_scale=cluster_pb.ScalePolicy.AutoScale(
                        min_size=spec.executor_pool_min_size,
                        max_size=spec.executor_pool_max_size,
                    ),
                ),
            )
        elif spec.executor_pool_size > 0:
            executor_pool = cluster_pb.ResourcePool(
                resource_preset_id=spec.executor_pool_resource_preset,
                scale_policy=cluster_pb.ScalePolicy(
                    fixed_scale=cluster_pb.ScalePolicy.FixedScale(
                        size=spec.executor_pool_size,
                    ),
                ),
            )
        else:
            raise RuntimeError("Executor pool size is not specified.")

        if spec.log_group_id is not None:
            logging_config = cluster_pb.LoggingConfig(
                enabled=True,
                log_group_id=spec.log_group_id,
            )
        elif spec.log_folder_id is not None:
            logging_config = cluster_pb.LoggingConfig(
                enabled=True,
                folder_id=spec.log_folder_id,
            )
        elif spec.logging_enabled:
            logging_config = cluster_pb.LoggingConfig(
                enabled=True,
                folder_id=spec.folder_id,
            )
        else:
            logging_config = cluster_pb.LoggingConfig(
                enabled=False,
            )

        if spec.maintenance_hour is not None and spec.maintenance_weekday is not None:
            if not 1 <= spec.maintenance_hour <= 24:
                raise RuntimeError("Maintenance hour is not valid.")

            if not 0 <= spec.maintenance_weekday <= 7:
                raise RuntimeError("Maintenance weekday is not valid.")

            maintenance_window = maintenance_pb.MaintenanceWindow(
                weekly_maintenance_window=maintenance_pb.WeeklyMaintenanceWindow(
                    day=spec.maintenance_weekday,
                    hour=spec.maintenance_hour,
                ),
            )
        else:
            maintenance_window = maintenance_pb.MaintenanceWindow(
                anytime=maintenance_pb.AnytimeMaintenanceWindow(),
            )

        request = cluster_service_pb.CreateClusterRequest(
            folder_id=spec.folder_id,
            name=spec.name,
            description=spec.description,
            labels=spec.labels,
            config=cluster_pb.ClusterConfig(
                resource_pools=cluster_pb.ResourcePools(
                    driver=driver_pool,
                    executor=executor_pool,
                ),
                history_server=cluster_pb.HistoryServerConfig(
                    enabled=spec.history_server_enabled,
                ),
                dependencies=cluster_pb.Dependencies(
                    pip_packages=spec.pip_packages,
                    deb_packages=spec.deb_packages,
                ),
                metastore=cluster_pb.Metastore(
                    cluster_id=spec.metastore_cluster_id,
                ),
            ),
            network=cluster_pb.NetworkConfig(
                subnet_ids=spec.subnet_ids,
                security_group_ids=spec.security_group_ids,
            ),
            deletion_protection=spec.deletion_protection,
            service_account_id=spec.service_account_id,
            logging=logging_config,
            maintenance_window=maintenance_window,
        )

        self.log.info("Creating Spark cluster. Request: %s.", request)

        result = self.sdk.create_operation_and_get_result(
            request,
            service=cluster_service_grpc_pb.ClusterServiceStub,
            method_name="Create",
            response_type=cluster_pb.Cluster,
            meta_type=cluster_service_pb.CreateClusterMetadata,
        )

        self.cluster_id = result.response.id
        return result

    def delete_cluster(self, cluster_id: Optional[str] = None):
        """
        Delete cluster.

        :param cluster_id: ID of the cluster to remove.
        :type cluster_id: str, optional

        :return: Operation result
        :rtype: OperationResult
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        request = cluster_service_pb.DeleteClusterRequest(cluster_id=cluster_id)

        self.log.info("Deleting Spark cluster. Request: %s.", request)

        return self.sdk.create_operation_and_get_result(
            request,
            service=cluster_service_grpc_pb.ClusterServiceStub,
            method_name="Delete",
            meta_type=cluster_service_pb.DeleteClusterMetadata,
        )

    def stop_cluster(self, cluster_id: Optional[str] = None):
        """
        Stop cluster.

        :param cluster_id: ID of the cluster to stop.
        :type cluster_id: str, optional

        :return: Operation result
        :rtype: OperationResult
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        request = cluster_service_pb.StopClusterRequest(cluster_id=cluster_id)

        self.log.info("Stopping Spark cluster. Request: %s.", request)

        return self.sdk.create_operation_and_get_result(
            request,
            service=cluster_service_grpc_pb.ClusterServiceStub,
            method_name="Stop",
            meta_type=cluster_service_pb.StopClusterMetadata,
        )

    def start_cluster(self, cluster_id: Optional[str] = None):
        """
        Start cluster.

        :param cluster_id: ID of the cluster to start.
        :type cluster_id: str, optional

        :return: Operation result
        :rtype: OperationResult
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        request = cluster_service_pb.StartClusterRequest(cluster_id=cluster_id)

        self.log.info("Starting Spark cluster. Request: %s.", request)

        return self.sdk.create_operation_and_get_result(
            request,
            service=cluster_service_grpc_pb.ClusterServiceStub,
            method_name="Start",
            meta_type=cluster_service_pb.StartClusterMetadata,
        )

    def create_spark_job(self, spec: SparkJobParameters, cluster_id: Optional[str] = None):
        """
        Run spark job.

        :param cluster_id: ID of the cluster.
        :type cluster_id: str, optional
        :param spec: Job parameters.
        :type spec: SparkJobParameters

        :return: Operation result
        :rtype: OperationResult
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        request = job_service_pb.CreateJobRequest(
            cluster_id=cluster_id,
            name=spec.name,
            spark_job=job_pb.SparkJob(
                main_jar_file_uri=spec.main_jar_file_uri,
                main_class=spec.main_class,
                args=spec.args,
                properties=spec.properties,
                packages=spec.packages,
                file_uris=spec.file_uris,
                jar_file_uris=spec.jar_file_uris,
                archive_uris=spec.archive_uris,
                repositories=spec.repositories,
                exclude_packages=spec.exclude_packages,
            ),
        )

        self.log.info("Running Spark job. Request: %s.", request)

        return self.sdk.create_operation_and_get_result(
            request,
            service=job_service_grpc_pb.JobServiceStub,
            method_name="Create",
            response_type=job_pb.Job,
            meta_type=job_service_pb.CreateJobMetadata,
        )

    def create_pyspark_job(self, spec: PysparkJobParameters, cluster_id: Optional[str] = None):
        """
        Run Pyspark job on the cluster.

        :param cluster_id: ID of the cluster.
        :type cluster_id: str, optional
        :param spec: Job parameters.
        :type spec: PysparkJobParameters

        :return: Operation result
        :rtype: OperationResult
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        request = job_service_pb.CreateJobRequest(
            cluster_id=cluster_id,
            name=spec.name,
            pyspark_job=job_pb.PysparkJob(
                main_python_file_uri=spec.main_python_file_uri,
                args=spec.args,
                properties=spec.properties,
                packages=spec.packages,
                file_uris=spec.file_uris,
                python_file_uris=spec.python_file_uris,
                jar_file_uris=spec.jar_file_uris,
                archive_uris=spec.archive_uris,
                repositories=spec.repositories,
                exclude_packages=spec.exclude_packages,
            ),
        )

        self.log.info("Running Pyspark job. Request: %s.", request)

        return self.sdk.create_operation_and_get_result(
            request,
            service=job_service_grpc_pb.JobServiceStub,
            method_name="Create",
            response_type=job_pb.Job,
            meta_type=job_service_pb.CreateJobMetadata,
        )

    def get_job(self, job_id: str, cluster_id: Optional[str] = None):
        """
        Get job info.

        :param cluster_id: ID of the cluster.
        :type cluster_id: str, optional
        :param job_id: ID of the job.
        :type job_id: str

        :return: Job info, job status name
        :rtype: Tuple[Job, str]
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        request = job_service_pb.GetJobRequest(
            cluster_id=cluster_id,
            job_id=job_id,
        )
        job = self.sdk.client(job_service_grpc_pb.JobServiceStub).Get(request)
        return job, job_pb.Job.Status.Name(job.status)

    def get_job_log(self, job_id: str, cluster_id: Optional[str] = None):
        """
        Get job log.

        :param cluster_id: ID of the cluster.
        :type cluster_id: str, optional
        :param job_id: ID of the job.
        :type job_id: str

        :return: Job log
        :rtype: List[str]
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            raise RuntimeError("Cluster id must be specified.")

        result = []
        page_token = ""

        while True:
            request = job_service_pb.ListJobLogRequest(
                cluster_id=cluster_id,
                job_id=job_id,
                page_token=page_token,
            )
            page = self.sdk.client(job_service_grpc_pb.JobServiceStub).ListLog(request)
            if page.content:
                result.extend(page.content.split("\n"))
            if page.next_page_token and page_token != page.next_page_token:
                page_token = page.next_page_token
            else:
                break
        return result

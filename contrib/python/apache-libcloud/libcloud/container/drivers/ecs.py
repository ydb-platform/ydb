# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.common.aws import AWSJsonResponse, SignedAWSConnection
from libcloud.container.base import Container, ContainerImage, ContainerDriver, ContainerCluster
from libcloud.container.types import ContainerState
from libcloud.container.utils.docker import RegistryClient

__all__ = ["ElasticContainerDriver"]


ECS_VERSION = "2014-11-13"
ECR_VERSION = "2015-09-21"
ECS_HOST = "ecs.%s.amazonaws.com"
ECR_HOST = "ecr.%s.amazonaws.com"
ROOT = "/"
ECS_TARGET_BASE = "AmazonEC2ContainerServiceV%s" % (ECS_VERSION.replace("-", ""))
ECR_TARGET_BASE = "AmazonEC2ContainerRegistry_V%s" % (ECR_VERSION.replace("-", ""))


class ECSJsonConnection(SignedAWSConnection):
    version = ECS_VERSION
    host = ECS_HOST
    responseCls = AWSJsonResponse
    service_name = "ecs"


class ECRJsonConnection(SignedAWSConnection):
    version = ECR_VERSION
    host = ECR_HOST
    responseCls = AWSJsonResponse
    service_name = "ecr"


class ElasticContainerDriver(ContainerDriver):
    name = "Amazon Elastic Container Service"
    website = "https://aws.amazon.com/ecs/details/"
    ecr_repository_host = "%s.dkr.ecr.%s.amazonaws.com"
    connectionCls = ECSJsonConnection
    ecrConnectionClass = ECRJsonConnection
    supports_clusters = False
    status_map = {"RUNNING": ContainerState.RUNNING}

    def __init__(self, access_id, secret, region):
        super().__init__(access_id, secret)
        self.region = region
        self.region_name = region
        self.connection.host = ECS_HOST % (region)

        # Setup another connection class for ECR
        conn_kwargs = self._ex_connection_class_kwargs()
        self.ecr_connection = self.ecrConnectionClass(access_id, secret, **conn_kwargs)
        self.ecr_connection.host = ECR_HOST % (region)
        self.ecr_connection.driver = self
        self.ecr_connection.connect()

    def _ex_connection_class_kwargs(self):
        return {"signature_version": "4"}

    def list_images(self, ex_repository_name):
        """
        List the images in an ECR repository

        :param  ex_repository_name: The name of the repository to check
            defaults to the default repository.
        :type   ex_repository_name: ``str``

        :return: a list of images
        :rtype: ``list`` of :class:`libcloud.container.base.ContainerImage`
        """
        request = {}
        request["repositoryName"] = ex_repository_name
        list_response = self.ecr_connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_ecr_headers("ListImages"),
        ).object
        repository_id = self.ex_get_repository_id(ex_repository_name)
        host = self._get_ecr_host(repository_id)
        return self._to_images(list_response["imageIds"], host, ex_repository_name)

    def list_clusters(self):
        """
        Get a list of potential locations to deploy clusters into

        :param  location: The location to search in
        :type   location: :class:`libcloud.container.base.ClusterLocation`

        :rtype: ``list`` of :class:`libcloud.container.base.ContainerCluster`
        """
        listdata = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps({}),
            headers=self._get_headers("ListClusters"),
        ).object
        request = {"clusters": listdata["clusterArns"]}
        data = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("DescribeClusters"),
        ).object
        return self._to_clusters(data)

    def create_cluster(self, name, location=None):
        """
        Create a container cluster

        :param  name: The name of the cluster
        :type   name: ``str``

        :param  location: The location to create the cluster in
        :type   location: :class:`libcloud.container.base.ClusterLocation`

        :rtype: :class:`libcloud.container.base.ContainerCluster`
        """
        request = {"clusterName": name}
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("CreateCluster"),
        ).object
        return self._to_cluster(response["cluster"])

    def destroy_cluster(self, cluster):
        """
        Delete a cluster

        :return: ``True`` if the destroy was successful, otherwise ``False``.
        :rtype: ``bool``
        """
        request = {"cluster": cluster.id}
        data = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("DeleteCluster"),
        ).object
        return data["cluster"]["status"] == "INACTIVE"

    def list_containers(self, image=None, cluster=None):
        """
        List the deployed container images

        :param image: Filter to containers with a certain image
        :type  image: :class:`libcloud.container.base.ContainerImage`

        :param cluster: Filter to containers in a cluster
        :type  cluster: :class:`libcloud.container.base.ContainerCluster`

        :rtype: ``list`` of :class:`libcloud.container.base.Container`
        """
        request = {"cluster": "default"}
        if cluster is not None:
            request["cluster"] = cluster.id
        if image is not None:
            request["family"] = image.name
        list_response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("ListTasks"),
        ).object
        if len(list_response["taskArns"]) == 0:
            return []
        containers = self.ex_list_containers_for_task(list_response["taskArns"])
        return containers

    def deploy_container(
        self,
        name,
        image,
        cluster=None,
        parameters=None,
        start=True,
        ex_cpu=10,
        ex_memory=500,
        ex_container_port=None,
        ex_host_port=None,
    ):
        """
        Creates a task definition from a container image that can be run
        in a cluster.

        :param name: The name of the new container
        :type  name: ``str``

        :param image: The container image to deploy
        :type  image: :class:`libcloud.container.base.ContainerImage`

        :param cluster: The cluster to deploy to, None is default
        :type  cluster: :class:`libcloud.container.base.ContainerCluster`

        :param parameters: Container Image parameters
        :type  parameters: ``str``

        :param start: Start the container on deployment
        :type  start: ``bool``

        :rtype: :class:`libcloud.container.base.Container`
        """
        data = {}
        if ex_container_port is None and ex_host_port is None:
            port_maps = []
        else:
            port_maps = [{"containerPort": ex_container_port, "hostPort": ex_host_port}]
        data["containerDefinitions"] = [
            {
                "mountPoints": [],
                "name": name,
                "image": image.name,
                "cpu": ex_cpu,
                "environment": [],
                "memory": ex_memory,
                "portMappings": port_maps,
                "essential": True,
                "volumesFrom": [],
            }
        ]
        data["family"] = name
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(data),
            headers=self._get_headers("RegisterTaskDefinition"),
        ).object
        if start:
            return self.ex_start_task(response["taskDefinition"]["taskDefinitionArn"])[0]
        else:
            return Container(
                id=None,
                name=name,
                image=image,
                state=ContainerState.RUNNING,
                ip_addresses=[],
                extra={"taskDefinitionArn": response["taskDefinition"]["taskDefinitionArn"]},
                driver=self.connection.driver,
            )

    def get_container(self, id):
        """
        Get a container by ID

        :param id: The ID of the container to get
        :type  id: ``str``

        :rtype: :class:`libcloud.container.base.Container`
        """
        containers = self.ex_list_containers_for_task([id])
        return containers[0]

    def start_container(self, container, count=1):
        """
        Start a deployed task

        :param container: The container to start
        :type  container: :class:`libcloud.container.base.Container`

        :param count: Number of containers to start
        :type  count: ``int``

        :rtype: :class:`libcloud.container.base.Container`
        """
        return self.ex_start_task(container.extra["taskDefinitionArn"], count)

    def stop_container(self, container):
        """
        Stop a deployed container

        :param container: The container to stop
        :type  container: :class:`libcloud.container.base.Container`

        :rtype: :class:`libcloud.container.base.Container`
        """
        request = {"task": container.extra["taskArn"]}
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("StopTask"),
        ).object
        containers = []
        containers.extend(
            self._to_containers(response["task"], container.extra["taskDefinitionArn"])
        )
        return containers

    def restart_container(self, container):
        """
        Restart a deployed container

        :param container: The container to restart
        :type  container: :class:`libcloud.container.base.Container`

        :rtype: :class:`libcloud.container.base.Container`
        """
        self.stop_container(container)
        return self.start_container(container)

    def destroy_container(self, container):
        """
        Destroy a deployed container

        :param container: The container to destroy
        :type  container: :class:`libcloud.container.base.Container`

        :rtype: :class:`libcloud.container.base.Container`
        """
        return self.stop_container(container)

    def ex_start_task(self, task_arn, count=1):
        """
        Run a task definition and get the containers

        :param task_arn: The task ARN to Run
        :type  task_arn: ``str``

        :param count: The number of containers to start
        :type  count: ``int``

        :rtype: ``list`` of :class:`libcloud.container.base.Container`
        """
        request = None
        request = {"count": count, "taskDefinition": task_arn}
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("RunTask"),
        ).object
        containers = []
        for task in response["tasks"]:
            containers.extend(self._to_containers(task, task_arn))
        return containers

    def ex_list_containers_for_task(self, task_arns):
        """
        Get a list of containers by ID collection (ARN)

        :param task_arns: The list of ARNs
        :type  task_arns: ``list`` of ``str``

        :rtype: ``list`` of :class:`libcloud.container.base.Container`
        """
        describe_request = {"tasks": task_arns}
        descripe_response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(describe_request),
            headers=self._get_headers("DescribeTasks"),
        ).object
        containers = []
        for task in descripe_response["tasks"]:
            containers.extend(self._to_containers(task, task["taskDefinitionArn"]))
        return containers

    def ex_create_service(self, name, cluster, task_definition, desired_count=1):
        """
        Runs and maintains a desired number of tasks from a specified
        task definition. If the number of tasks running in a service
        drops below desired_count, Amazon ECS spawns another
        instantiation of the task in the specified cluster.

        :param  name: the name of the service
        :type   name: ``str``

        :param  cluster: The cluster to run the service on
        :type   cluster: :class:`libcloud.container.base.ContainerCluster`

        :param  task_definition: The task definition name or ARN for the
            service
        :type   task_definition: ``str``

        :param  desired_count: The desired number of tasks to be running
            at any one time
        :type   desired_count: ``int``

        :rtype: ``object`` The service object
        """
        request = {
            "serviceName": name,
            "taskDefinition": task_definition,
            "desiredCount": desired_count,
            "cluster": cluster.id,
        }
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("CreateService"),
        ).object
        return response["service"]

    def ex_list_service_arns(self, cluster=None):
        """
        List the services

        :param cluster: The cluster hosting the services
        :type  cluster: :class:`libcloud.container.base.ContainerCluster`

        :rtype: ``list`` of ``str``
        """
        request = {}
        if cluster is not None:
            request["cluster"] = cluster.id
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("ListServices"),
        ).object
        return response["serviceArns"]

    def ex_describe_service(self, service_arn):
        """
        Get the details of a service

        :param  cluster: The hosting cluster
        :type   cluster: :class:`libcloud.container.base.ContainerCluster`

        :param  service_arn: The service ARN to describe
        :type   service_arn: ``str``

        :return: The service object
        :rtype: ``object``
        """
        request = {"services": [service_arn]}
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("DescribeServices"),
        ).object
        return response["services"][0]

    def ex_destroy_service(self, service_arn):
        """
        Deletes a service

        :param  cluster: The target cluster
        :type   cluster: :class:`libcloud.container.base.ContainerCluster`

        :param  service_arn: The service ARN to destroy
        :type   service_arn: ``str``
        """
        request = {"service": service_arn}
        response = self.connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_headers("DeleteService"),
        ).object
        return response["service"]

    def ex_get_registry_client(self, repository_name):
        """
        Get a client for an ECR repository

        :param  repository_name: The unique name of the repository
        :type   repository_name: ``str``

        :return: a docker registry API client
        :rtype: :class:`libcloud.container.utils.docker.RegistryClient`
        """
        repository_id = self.ex_get_repository_id(repository_name)
        token = self.ex_get_repository_token(repository_id)
        host = self._get_ecr_host(repository_id)
        return RegistryClient(host=host, username="AWS", password=token)

    def ex_get_repository_token(self, repository_id):
        """
        Get the authorization token (12 hour expiry) for a repository

        :param  repository_id: The ID of the repository
        :type   repository_id: ``str``

        :return: A token for login
        :rtype: ``str``
        """
        request = {"RegistryIds": [repository_id]}
        response = self.ecr_connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_ecr_headers("GetAuthorizationToken"),
        ).object
        return response["authorizationData"][0]["authorizationToken"]

    def ex_get_repository_id(self, repository_name):
        """
        Get the ID of a repository

        :param  repository_name: The unique name of the repository
        :type   repository_name: ``str``

        :return: The repository ID
        :rtype: ``str``
        """
        request = {"repositoryNames": [repository_name]}
        list_response = self.ecr_connection.request(
            ROOT,
            method="POST",
            data=json.dumps(request),
            headers=self._get_ecr_headers("DescribeRepositories"),
        ).object
        repository_id = list_response["repositories"][0]["registryId"]
        return repository_id

    def _get_ecr_host(self, repository_id):
        return self.ecr_repository_host % (repository_id, self.region)

    def _get_headers(self, action):
        """
        Get the default headers for a request to the ECS API
        """
        return {
            "x-amz-target": "{}.{}".format(ECS_TARGET_BASE, action),
            "Content-Type": "application/x-amz-json-1.1",
        }

    def _get_ecr_headers(self, action):
        """
        Get the default headers for a request to the ECR API
        """
        return {
            "x-amz-target": "{}.{}".format(ECR_TARGET_BASE, action),
            "Content-Type": "application/x-amz-json-1.1",
        }

    def _to_clusters(self, data):
        clusters = []
        for cluster in data["clusters"]:
            clusters.append(self._to_cluster(cluster))
        return clusters

    def _to_cluster(self, data):
        return ContainerCluster(
            id=data["clusterArn"],
            name=data["clusterName"],
            driver=self.connection.driver,
        )

    def _to_containers(self, data, task_definition_arn):
        clusters = []
        for cluster in data["containers"]:
            clusters.append(self._to_container(cluster, task_definition_arn))
        return clusters

    def _to_container(self, data, task_definition_arn):
        return Container(
            id=data["containerArn"],
            name=data["name"],
            image=ContainerImage(
                id=None,
                name=data["name"],
                path=None,
                version=None,
                driver=self.connection.driver,
            ),
            ip_addresses=None,
            state=self.status_map.get(data["lastStatus"], None),
            extra={
                "taskArn": data["taskArn"],
                "taskDefinitionArn": task_definition_arn,
            },
            driver=self.connection.driver,
        )

    def _to_images(self, data, host, repository_name):
        images = []
        for image in data:
            images.append(self._to_image(image, host, repository_name))
        return images

    def _to_image(self, data, host, repository_name):
        path = "{}/{}:{}".format(host, repository_name, data["imageTag"])
        return ContainerImage(
            id=None,
            name=path,
            path=path,
            version=data["imageTag"],
            driver=self.connection.driver,
        )

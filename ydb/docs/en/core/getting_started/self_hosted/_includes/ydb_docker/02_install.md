## Installation {#install}

Download the current public version of the Docker image:

```bash
docker pull {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
```

Make sure the Docker image has been pulled:

```bash
docker image list | grep {{ ydb_local_docker_image }}
```

Output:

```bash
{{ ydb_local_docker_image }}           {{ ydb_local_docker_image_tag }}   b73c5c1441af   2 months ago   793MB
```


## Docker stop

To stop {{ydb-short-name }} in Docker, run the following command:

```bash
docker stop ydb-local
```

If the `--rm` flag was specified at startup, the container will be deleted after stopping.

## Kill Docker Container with {{ ydb-short-name }}

To delete a Docker container with {{ ydb-short-name }}, run the following command:

```bash
docker kill ydb-local
```

If you want to clean up the file system, you can delete your work directory using the `rm -rf ~/ydbd` command, and all data inside the local YDB cluster will be lost.

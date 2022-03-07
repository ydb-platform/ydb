1. Subfolder tar.gz archives must be created for each of the subfolder's contents, stripping folder name from the archive contents:

  ```
  tar -czf <subfolder>.tar.gz --strip-components=1 * -C <subfolder>
  ```

  ```
  tar -czf linux.tar.gz --strip-components=1 * -C linux
  ```  

2. Archives to be published to binary.ydb.tech bucket in the `local_binary` subfolder.
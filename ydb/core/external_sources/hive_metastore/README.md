# Description
The build environment does not support automatic generation of thrift files, unlike protobufs. Therefore, these files need to be generated manually.

1. To do this, the thrift compiler needs to be installed. Full information about the compiler can be found at https://thrift.apache.org/. If you are using the Ubuntu operating system and the apt package manager, you can run the following commands:
```
sudo apt-get update
sudo apt-get -y install thrift-compiler
```

2. After installing the compiler, run the command `thrift -r --gen cpp hive_metastore.thrift` in the directory `ydb/core/external_sources` hive_metastore.

3. If there are any compilation problems after generating the files, please fix them manually
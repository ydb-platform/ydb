## AWS C Http

C99 implementation of the HTTP/1.1 and HTTP/2 specifications

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building

CMake 3.1+ is required to build.

`<install-path>` must be an absolute path in the following instructions.

#### Linux-Only Dependencies

If you are building on Linux, you will need to build aws-lc and s2n-tls first.

```
git clone git@github.com:awslabs/aws-lc.git
cmake -S aws-lc -B aws-lc/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-lc/build --target install

git clone git@github.com:aws/s2n-tls.git
cmake -S s2n-tls -B s2n-tls/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build s2n-tls/build --target install
```

#### Building aws-c-http and Remaining Dependencies

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-c-cal.git
cmake -S aws-c-cal -B aws-c-cal/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-cal/build --target install

git clone git@github.com:awslabs/aws-c-io.git
cmake -S aws-c-io -B aws-c-io/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-io/build --target install

git clone git@github.com:awslabs/aws-c-compression.git
cmake -S aws-c-compression -B aws-c-compression/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-compression/build --target install

git clone git@github.com:awslabs/aws-c-http.git
cmake -S aws-c-http -B aws-c-http/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-http/build --target install
```

#### Run Integration Tests with localhost

To run some of the integration tests (start with localhost_integ_*), you need to set up a localhost that echo the request headers from `/echo` back first.

To do that, check [localhost](./tests/py_localhost/) script we have.

After that, configure and build your cmake project with `-DENABLE_LOCALHOST_INTEGRATION_TESTS=true` to build the tests with localhost and run them from `ctest --output-on-failure -R localhost_integ_*`.

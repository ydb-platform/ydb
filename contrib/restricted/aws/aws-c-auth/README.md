## AWS C Auth

C99 library implementation of AWS client-side authentication: standard credentials providers and signing.

From a cryptographic perspective, only functions with the suffix "_constant_time" should be considered constant
time.

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

#### Building aws-c-auth and Remaining Dependencies

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

git clone git@github.com:awslabs/aws-c-auth.git
cmake -S aws-c-auth -B aws-c-auth/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-auth/build --target install
```

### Testing
Certain tests require a specific environment setup in order to run successfully.  This may be a specific execution
environment (EC2, ECS, etc...) or it may require certain environment variables to be set that configure properties
(often sensitive materials, like keys).  Whether or not these tests are enabled is controlled by certain CMAKE 
properties:
* AWS_BUILDING_ON_EC2 - indicates real IMDS credentials provider test(s) should run
* AWS_BUILDING_ON_ECS - indciates real ECS credentials provider tests(s) should run
* AWS_HAS_CI_ENVIRONMENT - indicates that all tests that require environmentally injected secrets/properties should run

Environment properties are injected by CRT builder process via the custom builder step defined in `./.builder/action/aws-c-auth-test.py`

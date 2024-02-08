## AWS C SDKUTILS

C99 library implementing AWS SDK specific utilities. Includes utilities for ARN
parsing, reading AWS profiles, etc...

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building

CMake 3.0+ is required to build.

`<install-path>` must be an absolute path in the following instructions.


#### Building aws-c-sdkutils

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-c-sdkutils.git
cmake -S aws-c-sdkutils -B aws-c-sdkutils/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-sdkutils/build --target install
```
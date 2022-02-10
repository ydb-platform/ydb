## AWS C Event Stream

C99 implementation of the vnd.amazon.event-stream content-type.

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building
Note that aws-c-event-stream has several dependencies:

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -DCMAKE_PREFIX_PATH=<install-path> -DCMAKE_INSTALL_PREFIX=<install-path> -S aws-c-common -B aws-c-common/build
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-checksums.git
cmake -DCMAKE_PREFIX_PATH=<install-path> -DCMAKE_INSTALL_PREFIX=<install-path> -S aws-checksums -B aws-checksums/build
cmake --build aws-checksums/build --target install

git clone git@github.com:awslabs/aws-c-event-stream.git
cmake -DCMAKE_PREFIX_PATH=<install-path> -DCMAKE_INSTALL_PREFIX=<install-path> -S aws-c-event-stream -B aws-c-event-stream/build
cmake --build aws-c-event-stream/build --target install
```

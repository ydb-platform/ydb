## Private API development guide

All cloud control plane API definition based on [gRPC](https://grpc.io).

### Repository setup
#### Prerequisites

- make
- on Mac OS X, protoc (should be >= 3.5) or Homebrew (protoc will be installed using brew)
- on Linux, protoc (should be >= 3.5) or sudo access to install it from github

#### Steps

Example:

```
git clone https://bb.yandex-team.ru/scm/cloud/private-api.git
cd private-api

// ...Hack-hack-hack...

make lint
```

### Checking proto compilation locally

Just run `make lint` inside src root.
To build proto tools from source set 'BUILD_TOOLS' variable to 1.
Also you may check compilation of swagger docs out of cloud API: run `make generate` for that.

For Pull Request build both successful `lint` and `generate` required.


#### Validation

Normally, all the fields of all messages received from users must be validated:
- Request messages;
- Value objects used in request messages;
- Value objects used in value objects and so on.

Output messages does not require validation.

Syntax and examples can be found in ``yandex/cloud/priv/example/v1alpha/validation_example.proto``

For Java developers there is ``java`` module, ``mvn clean package`` in this directory does following:
 - builds and packages all proto-files in the repository
 - tests all validators for syntax and applicability
 - includes yandex.cloud.proto.ProtoValidator which helps validating messages in grpc interceptor.

The `java/do_local_install.sh` script is useful for local development of a feature branch.
The script will run `mvn versions:set && mvn clean install` and install built artifacts 
in the local maven repo with snapshot version. Please run `versions:revert` manually if script failed at compilation.

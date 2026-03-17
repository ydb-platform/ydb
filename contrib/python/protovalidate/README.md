[![The Buf logo](.github/buf-logo.svg)][buf] 

# protovalidate-python

[![CI](https://github.com/bufbuild/protovalidate-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/bufbuild/protovalidate-python/actions/workflows/ci.yaml)
[![Conformance](https://github.com/bufbuild/protovalidate-python/actions/workflows/conformance.yaml/badge.svg)](https://github.com/bufbuild/protovalidate-python/actions/workflows/conformance.yaml)
[![PyPI version](https://badge.fury.io/py/protovalidate.svg)](https://badge.fury.io/py/protovalidate)

[Protovalidate][protovalidate] provides standard annotations to validate common constraints on messages and fields, as well as the ability to use [CEL][cel] to write custom constraints. It's the next generation of [protoc-gen-validate][protoc-gen-validate], the only widely used validation library for Protobuf.

With Protovalidate, you can annotate your Protobuf messages with both standard and custom validation rules:

```protobuf
syntax = "proto3";

package banking.v1;

import "buf/validate/validate.proto";

message MoneyTransfer {
  string to_account_id = 1 [
    // Standard rule: `to_account_id` must be a UUID
    (buf.validate.field).string.uuid = true
  ];

  string from_account_id = 2 [
    // Standard rule: `from_account_id` must be a UUID
    (buf.validate.field).string.uuid = true
  ];

  // Custom rule: `to_account_id` and `from_account_id` can't be the same.
  option (buf.validate.message).cel = {
    id: "to_account_id.not.from_account_id"
    message: "to_account_id and from_account_id should not be the same value"
    expression: "this.to_account_id != this.from_account_id"
  };
}
```

Once you've added `protovalidate-python` to your project, validation is idiomatic Python:

```python
try:
    protovalidate.validate(message)
except protovalidate.ValidationError as e:
    # Handle failure.
```

## Installation

> [!TIP]
> The easiest way to get started with Protovalidate for RPC APIs are the quickstarts in Buf's documentation. There's one available for [Python and gRPC][grpc-python].

To install the package, use `pip`:

```shell
pip install protovalidate
```

## Documentation

Comprehensive documentation for Protovalidate is available in [Buf's documentation library][protovalidate]. 

Highlights for Python developers include:

* The [developer quickstart][quickstart]
* A comprehensive RPC quickstart for [Python and gRPC][grpc-python]
* A [migration guide for protoc-gen-validate][migration-guide] users

## Additional Languages and Repositories

Protovalidate isn't just for Python! You might be interested in sibling repositories for other languages: 

- [`protovalidate-go`][pv-go] (Go)
- [`protovalidate-java`][pv-java] (Java)
- [`protovalidate-cc`][pv-cc] (C++)
- [`protovalidate-es`][pv-es] (TypeScript and JavaScript)

Additionally, [protovalidate's core repository](https://github.com/bufbuild/protovalidate) provides:

- [Protovalidate's Protobuf API][validate-proto]
- [Conformance testing utilities][conformance] for acceptance testing of `protovalidate` implementations

## Contribution

We genuinely appreciate any help! If you'd like to contribute, check out these resources:

- [Contributing Guidelines][contributing]: Guidelines to make your contribution process straightforward and meaningful
- [Conformance testing utilities](https://github.com/bufbuild/protovalidate/tree/main/docs/conformance.md): Utilities providing acceptance testing of `protovalidate` implementations

## Related Sites

- [Buf][buf]: Enterprise-grade Kafka and gRPC for the modern age
- [Common Expression Language (CEL)][cel]: The open-source technology at the core of Protovalidate

## Legal

Offered under the [Apache 2 license][license].

[buf]: https://buf.build
[cel]: https://cel.dev

[pv-go]: https://github.com/bufbuild/protovalidate-go
[pv-java]: https://github.com/bufbuild/protovalidate-java
[pv-python]: https://github.com/bufbuild/protovalidate-python
[pv-cc]: https://github.com/bufbuild/protovalidate-cc
[pv-es]: https://github.com/bufbuild/protovalidate-es

[buf-mod]: https://buf.build/bufbuild/protovalidate
[license]: LICENSE
[contributing]: .github/CONTRIBUTING.md

[protoc-gen-validate]: https://github.com/bufbuild/protoc-gen-validate

[protovalidate]: https://buf.build/docs/protovalidate
[quickstart]: https://buf.build/docs/protovalidate/quickstart/
[connect-go]: https://buf.build/docs/protovalidate/quickstart/connect-go/
[grpc-go]: https://buf.build/docs/protovalidate/quickstart/grpc-go/
[grpc-java]: https://buf.build/docs/protovalidate/quickstart/grpc-java/
[grpc-python]: https://buf.build/docs/protovalidate/quickstart/grpc-python/
[migration-guide]: https://buf.build/docs/migration-guides/migrate-from-protoc-gen-validate/
[conformance-executable]: ./internal/cmd/protovalidate-conformance-go/README.md
[pkg-go]: https://pkg.go.dev/github.com/bufbuild/protovalidate-go

[validate-proto]: https://buf.build/bufbuild/protovalidate/docs/main:buf.validate
[conformance]: https://github.com/bufbuild/protovalidate/blob/main/docs/conformance.md
[examples]: https://github.com/bufbuild/protovalidate/tree/main/examples
[migrate]: https://buf.build/docs/migration-guides/migrate-from-protoc-gen-validate/

## Common Protos

[![release level](https://img.shields.io/badge/release%20level-general%20availability%20%28GA%29-brightgreen.svg?style&#x3D;flat)](https://cloud.google.com/terms/launch-stages)

This repository is a home for the [protocol buffer][protobuf] types which are
common dependencies throughout the Google API ecosystem, and which are made
available for use as dependencies elsewhere.

### About protocol buffers

[Protocol buffers][protobuf] are Google's language-neutral, platform-neutral,
extensible mechanism for serializing structured data – think XML,
but smaller, faster, and simpler. You define how you want your data to be
structured once, then you can use special generated source code to easily
write and read your structured data to and from a variety of data streams
and using a variety of languages.

One popular use for protocol buffers, both within Google and elsewhere,
is for specifying API design. They serve as a useful representation for
API serivices, methods, and fields. You can read more about API design
philosophy with protocol buffers by consulting our
[API Design Guide][api-style].

  [api-style]: https://cloud.google.com/apis/design/
  [protobuf]: https://developers.google.com/protocol-buffers/

### Why a common protos repository?

When writing protocol buffers, it is common to need to reuse common patterns.
For example, common types show up in many different APIs, but have
a consistent implementation. The same is true elsewhere.

This repository seeks to be a home for such types; protocol buffer authors
may check out this repository and import them into their own work to save
effort.

The protos in the various subdirectories in this repository have different
purposes, and are documented in their respective README files.

## Using these protos

*NOTE* The protos in this repository are not updated automatically and may be 
outdated. Please look to the [protos in googleapis/googleapis](https://github.com/googleapis/googleapis/tree/master/google) instead
which are updated regularly.

These protos are made available under an Apache license (see `LICENSE`) and
you are free to depend on them within your applications. They are
considered stable and will not change in backwards-incompaible ways.

In order to depend on these protos, use proto import statements that
reference the base of this repository, for example:

```protobuf
syntax = "proto3";

import "google/type/color.proto";


// A message representing a paint can.
message PaintCan {
  // The size of the paint can, in gallons.
  float size_gallons = 1;

  // The color of the paint.
  google.type.Color color = 2;
}
```

If you are using `protoc` (or other similar tooling) to compile these
protos yourself, you will likely require a local copy. Clone this repository
to a convenient location and use `--proto_path` to specify the _root_ of
this repository on your machine to the compiler.

## Packages

Additionally, if using these common protos, it is not necessary to ship
the compiled types yourself in many common languages. Google provides
a common protos package in several languages, which can be added as a
dependency, and which makes these types available.

  * **C#**: [Google.Api.CommonProtos](https://www.nuget.org/packages/Google.Api.CommonProtos/)
  * **Java**: [proto-google-common-protos](https://mvnrepository.com/artifact/com.google.api.grpc/proto-google-common-protos)
  * **Go**: [google.golang.org/genproto/googleapis](https://godoc.org/google.golang.org/genproto/googleapis)
  * **Node.js**: [google-proto-files](https://www.npmjs.com/package/google-proto-files)
  * **PHP**: [gax-php](https://github.com/googleapis/gax-php)
  * **Python**: [googleapis-common-protos](https://pypi.org/project/googleapis-common-protos/)
  * **Ruby**: [googleapis-common-protos](https://rubygems.org/gems/googleapis-common-protos/versions/1.3.5)

Note that if using these packages, you will still need a local copy of
these protos when using `protoc`, but you will _not_ need to ship compiled
versions of them. (This is consistent with `protoc`'s default behavior of
only providing compiled output for the files specifically requested, and not
their imports.)

## google.protobuf types (separate from this repo)

There are a small number of types that are _so_ common that they are
included in the actual protocol buffers runtime itself.
These are anything with an import path beginning with `google/protobuf/`,
and notably includes timestamps and durations.

These are _not_ defined in this directory, and you do _not_ need to follow
any of the instructions for including this (as discussed in the root `README`
file) if you want to use those. They are part of protocol buffers, and
an import of those will "just work". These are colloquially referred to
as "well-known types".

## Disclaimer

These protos are made available by Google, but are not considered to be an
official Google product.

## License

These protos are licensed using the Apache 2.0 software license, a permissive,
copyfree license. You are free to use them in your applications provided
the license terms are honored.

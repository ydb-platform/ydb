# Vendored `google.protobuf` schemas

The six `.proto` files here are verbatim copies from [`protocolbuffers/protobuf`](https://github.com/protocolbuffers/protobuf), taken from `src/google/protobuf/` at the release tag recorded in `VERSION`. Do not edit them.

The crate carries them because the stub schemas in the parent directory are empty shells whose content arrives through these imports, so whichever compiler resolves the imports decides the generated code. `protoc` parses schema text and keeps the documentation. `protox` serves its built-in copies from a compiled descriptor table that carries none. Both compilers prefer a file found on an include path over their own built-in copy, so shipping these makes every build read the same bytes and produce the same output.

To refresh, pick the release tag from <https://github.com/protocolbuffers/protobuf/releases>, download the six files from `https://raw.githubusercontent.com/protocolbuffers/protobuf/<tag>/src/google/protobuf/`, overwrite these with them unmodified, and write the new tag into `VERSION`. Then build the crate with `vendored-protoc`, with `vendored-protox`, and with a system `protoc`, and confirm the generated code still comes out the same under all three.

Upstream licenses these files under BSD-3-Clause, and each one carries the license text in its header. The rest of the crate is Apache-2.0.

# Temporal proto files  

This repository contains both the protobuf descriptors and OpenAPI documentation for the Temporal platform.

## How to use

Install as git submodule to the project.

## Contribution

Make your change to the temporal/proto files, and run `make` to update the openapi definitions.
Rust is also required because `make` installs and runs `nex-gen` when regenerating system Nexus WIT files.

## Breaking changes

Sometimes during initial feature development, there will be breaking API changes made. Running `make` will
catch these changes and fail CI. If the breaking change is for a feature not yet released, a temporary `ignore`
line can be added to `buf.yaml` to pass CI. This is
[an example](https://github.com/temporalio/api/pull/608/files#diff-1a5ba9cba93e971f532139f694d7da802776bfe578e3f753b9c3f25968dbf42dL16)
of adding such an exception. A subsequent PR will be needed to then disable to exception once it's been merged in.

## License

MIT License, please see [LICENSE](LICENSE) for details.

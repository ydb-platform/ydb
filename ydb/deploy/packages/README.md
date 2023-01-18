# Packages for YDB

In order to build YDB packages (deb, rpm, apk) used [nFPM](https://nfpm.goreleaser.com) tool. This is zero dependency binary, so only required to install it

## Installation

nFPM can be [installed](https://nfpm.goreleaser.com/install/) with available OS package managers (brew, apt, yum), or directly downloaded from [releases page](https://github.com/goreleaser/nfpm/releases).

## Usage

Each package has it's own configuration spec, located in *.yaml file.
Configuration options can be found [here](https://nfpm.goreleaser.com/configuration/).

There is a script, that will build all supported packages with appropriate version in specified format:

```bash

export YDB_VERSION_STRING="<release version>"
export RELEASE_DIR="<path where compiled binaries and libs located>"
./build.sh --<package type: deb>

```

For building package manually, use following command:

```bash

nfpm package --target "<where package will be located>" --config "<path to package config>" --packager "<package type: deb|rpm|apk>"

```

## Public API development guide

All cloud control plane API definition based on [GRPC](https://grpc.io).

This repo contains:
- public api definition
- public api documentation

Public API definitions used only for:
 - API Gateway
 - Public SDK
 - Docs generation

For other purpose see [private API](https://bb.yandex-team.ru/projects/CLOUD/repos/private-api/browse)

#### API definition style

Layout `yandex/<service>/<version>/*.proto`
 - service in cloud product meaning, for example, compute, mds, microcosm, etc
 - version format for GA releases `v<N>`, for pre-GA releases `v<N><pre-release-name><M>`, ex: `v1tp1`
 
For entity and service definitions use two .proto file:
 - `<entity>.proto`, use for entity structure definition
 - `<entity>_service.proto`, use for grpc service definition

All methods for operations tracking and listing should be placed in `<entity>_service.proto`

For the scalar fields of the message use one of the following types:
 - `int64`, everywhere for integer numbers. This allows not to think about overflow
 - `double`, everywhere for real numbers. This allows not to think about overflow
 - `string`, `bool`, no comments 

If you want to use any other scalar type, first take advice from API design team.

See compute service API as a reference. 

## Setup repository

### POSIX (maxOS or Linux)

#### Prerequisites

- make, curl
- on macOS, protoc (should be >= 3.5) or Homebrew (protoc will be installed using brew)
- on Linux, protoc (should be >= 3.5) or sudo access to install it from github

#### Steps

- clone this repo 

*Example:*
```
mkdir -p ~/cloud
cd ~/cloud
git clone https://bb.yandex-team.ru/scm/cloud/public-api.git
cd ~/public-api
```

### Windows

#### Prerequisites

- Download and install [Git](https://git-scm.com/download/win) with mingw command line
- Download and install [MinGW](https://sourceforge.net/projects/mingw/files/latest/download)
- Configure MinGW: Run `C:\MinGW\bin\mingw-get install mingw32-base`


#### Steps

- Everything should be run under Git Bash (Start Menu -> Git Bash)
- Make sure `mingw32-make` is in your `PATH`. As a backup, just run: `export PATH=$PATH:/c/MinGW/bin`

- clone this repo 

*Example:*
```
mkdir -p ~/cloud
cd ~/cloud
git clone https://bb.yandex-team.ru/scm/cloud/public-api.git
cd ~/public-api
```


## Working with repository
*For Windows use `mingw32-make` instead of `make`*

### Main commands
- `make` to run lint and generate docs
  - Equivalent of running both `make lint` and `make generate`
  - This is run in every Pull Request Build. If `make` is not successful your PR will not be merged.
- `make lint` to run lint
    - Run custom .proto files checks that forces API definition style.
    - To build proto tools from source set 'BUILD_TOOLS' variable to 1.
- `make generate` to generate Open API files that used for docs generation.
  - Output will be in `${repo_root}/generated`


### Utility commands
These commands should not be required.
 - `make clean-generated` to delete all generated docs. In case any bugs happen (doc is not being regenerated), run this command and then run `make generate`
 - `make clean` to delete all binary dependencies (protobuf generator plugins, etc.) and generated docs.

## Questions?
Ask in [YC.API doc](https://t.me/joinchat/BLcQWA1_s-OlyJ_4NMhaEA) chat!


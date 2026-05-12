# Building YDB from sources

From this repository you can build YDB Server and YDB CLI (Command Line Interface utility) executables.

[Yatool](https://github.com/yandex/yatool) is the multilanguage build/test system being used to [build and test YDB](https://ydb.tech/docs/en/development/build-ya). You need to use Yatool and follow the [YDB development process](https://ydb.tech/docs/en/development/suggest-change) to make contributions to the YDB project.

## Create the work directory. 
> :warning: Please make sure you have at least 80Gb of free space. We also recommend placing this directory on SSD to reduce build times.

```bash
mkdir ~/ydbwork && cd ~/ydbwork
```

## Clone the ydb repository

```bash
git clone https://github.com/ydb-platform/ydb.git
```

Change the current working directory to the cloned repository root to perform further commands:

```
cd ydb
```

## Checkout a branch for build

The `main` branch is the primary development branch for YDB.

Build from the development mainline branch may sometimes be broken for short periods of time, and also some tests may fail. So, you may prefer to build the latest stable versions of YDB Server and CLI. As stable versions of the YDB Server and CLI belong to different commits, it is not possible to get both server and CLI stable executables with a single checkout/build. Checkout and build server first, then CLI, or vice versa.


### Checkout the latest development version

By default, the `main` branch is checked out, so you can run Yatool build without executing a checkout command.

### Check out the latest stable YDB Server version

To build the latest stable version of a YDB Server, check out the latest stable Server tag from the repository. To do so, visit the https://github.com/ydb-platform/ydb/releases/latest page and use the provided 'tag' value in the `git checkout <tag>` command.

For example, at the time of writing the latest stable release was [YDB Server 23.2.12](https://github.com/ydb-platform/ydb/releases/tag/23.2.12) with a tag `23.2.12`, so the checkout command looks like this:

```bash
git checkout 23.2.12
```

### Check out the latest stable YDB CLI version

To build a latest stable version of a YDB CLI, check out the latest stable CLI tag from the repository. To do so, visit the https://github.com/ydb-platform/ydb/releases page, scroll down to the top-most 'YDB CLI' release, and use the provided 'tag' value in the `git checkout <tag>` command.

For example, at the time of writing the latest YDB CLI release was [YDB CLI 2.5.0](https://github.com/ydb-platform/ydb/releases/tag/CLI_2.5.0) with a tag `CLI_2.5.0`, so the checkout command looks like this:

```bash
git checkout CLI_2.5.0
```

## Build


Run `ya make` from the repository root, followed by an optional relative path to the build target. By default, `ya make` compiles a `debug` configuration, while you can choose to have `release` or `relwithdebinfo`. The latter is recommended for faster builds, as it leverages the YDB remote build cache.

To build a binary, Yatool downloads and caches relevant toolchains from the YDB S3 storage, so you do not need to install anything on the build machine.

To build YDB server run:

```
./ya make ydb/apps/ydbd --build relwithdebinfo
```

To build YDB CLI run:

```
./ya make ydb/apps/ydb --build relwithdebinfo
```

Upon completion, you will have the `ydbd`/`ydb` executables in the `ydb/apps/ydbd`/`ydb/apps/ydb` directories, respectively.

You can read more about Yatool options in the [YDB documentation](https://ydb.tech/docs/en/development/build-ya).

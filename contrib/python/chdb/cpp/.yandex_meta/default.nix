self: super: with self; {
  chdb = llvmPackages.libcxxStdenv.mkDerivation rec {
    name = "chdb";
    version = "3.3.0";

    src = fetchFromGitHub {
      owner = "chdb-io";
      repo = "chdb";
      rev = "v${version}";
      hash = "sha256-81JwZvvDclv60BXg6fsM0O7Kk2lW5V/0NBu3janbndk=";

      # fetching only needed submodules to speedup import
      leaveDotGit = true;
      fetchSubmodules = false;

      postFetch = ''(
        cd $out/contrib

        git reset -- abseil-cpp
        git reset -- avro
        git reset -- aws  # aws begin
        git reset -- aws-c-auth
        git reset -- aws-c-cal
        git reset -- aws-c-common
        git reset -- aws-c-compression
        git reset -- aws-c-event-stream
        git reset -- aws-c-http
        git reset -- aws-c-io
        git reset -- aws-c-mqtt
        git reset -- aws-c-s3
        git reset -- aws-c-sdkutils
        git reset -- aws-checksums
        git reset -- aws-crt-cpp
        git reset -- aws-s2n-tls  # aws end
        git reset -- base64
        git reset -- boost
        git reset -- c-ares
        git reset -- cctz
        git reset -- croaring
        git reset -- double-conversion
        git reset -- dragonbox
        git reset -- expected
        git reset -- fast_float
        git reset -- fastops
        git reset -- fmtlib
        git reset -- h3
        git reset -- hashidsxx
        git reset -- icu
        git reset -- icudata
        git reset -- incbin
        git reset -- libcpuid
        git reset -- libdivide
        git reset -- libfarmhash
        #git reset -- libfiu
        git reset -- libunwind
        git reset -- llvm-project  # ClickHouse uses libcxx/libcxxabi from this submodule.
        git reset -- lz4
        git reset -- magic_enum
        git reset -- mariadb-connector-c
        git reset -- miniselect
        git reset -- morton-nd
        git reset -- msgpack-c
        git reset -- openssl
        git reset -- poco
        git reset -- pocketfft
        git reset -- rapidjson
        git reset -- re2
        git reset -- replxx
        git reset -- simdjson
        git reset -- snappy
        git reset -- sparsehash-c11
        git reset -- sysroot
        git reset -- wyhash
        git reset -- xxHash
        git reset -- xz
        git reset -- zlib-ng
        git reset -- zstd

        # chdb
        git reset -- arrow
        git reset -- brotli
        git reset -- flatbuffers
        git reset -- google-protobuf
        git reset -- libhdfs3
        git reset -- orc
        git reset -- libprotobuf-mutator
        git reset -- thrift
        git reset -- utf8proc

        ./sparse-checkout/setup-sparse-checkout.sh
        git submodule init
        git submodule sync
        git submodule update --depth=1
        #./update-submodules.sh
        find .. -name .git -exec rm -rf {} +
      )'';
    };

    patches = [
      ./allow-ldflags.patch
      ./allow-cxx-flags.patch
      ./disable-std-exception-has-stack-trace.patch
      ./eliminate-object-libs.patch
      ./disable-fiu.patch
      ./fix-openssl-cmake.patch
      ./use-blake3.patch
      ./chdb-fixes.patch
    ];

    nativeBuildInputs = [
      cmake
      flatbuffers
      ninja
      python3
      python3Packages.pybind11
      python3Packages.setuptools
      python3Packages.build
      python3Packages.tox
      perl
      llvmPackages.lld
    ];

    NIX_LDFLAGS = [ "--unresolved-symbols=ignore-all" ];

    cmakeFlags = [
      # ClickHouse uses MAKE_STATIC_LIBRARIES variable to decide which type of
      # library to build (static/shared).
      # yamaker defines BUILD_SHARED_LIBS=ON by default. ClickHouse ignores it
      # but some contrib libraries do not and it leads to a broken build.
      # Turn it off manually to override yamaker's default value.
      "-DBUILD_SHARED_LIBS=OFF"

      # Building without this option breaks build traces: https://st.yandex-team.ru/DTCC-589.
      "-DCMAKE_CXX_FLAGS=-fno-integrated-cc1"

      # Arcadia has a fixed glibc version. Do not need to use wierd compat hacks.
      "-DGLIBC_COMPATIBILITY=OFF"

      # ClickHouse does not allow to run a build without CCACHE unless it is explicitly disabled.
      "-DCOMPILER_CACHE=disabled"

      # TODO(dakovalkov): enable hyperscan https://st.yandex-team.ru/CHYT-701.
      "-DENABLE_HYPERSCAN=OFF"

      # NOTE(dakovalkov): llvm-project is checked out because clickhouse uses libcxx/libcxxabi from it, but we do no want to use embedded compiler.
      "-DENABLE_EMBEDDED_COMPILER=OFF"

      # TODO(dakovalkov): The option does not work. Disabled by disable-fiu.patch.
      # "-DENABLE_FIU=OFF"
      "-DENABLE_LIBFIU=OFF"

      "-DENABLE_DWARF_PARSER=OFF"
      "-DENABLE_BLAKE3=OFF"

      "-DENABLE_GWP_ASAN=OFF"
      "-DENABLE_JEMALLOC=OFF"
      "-DENABLE_MYSQL=OFF"
      "-DENABLE_NLP=OFF"
      "-DENABLE_NURAFT=OFF"
      "-DENABLE_ODBC=OFF"
      "-DUSE_SENTRY=OFF"
      "-DUSE_YAML_CPP=OFF"

      #"-DENABLE_AWS_S3=ON"

      # Build only server, client and local to speedup build.
      "-DENABLE_CLICKHOUSE_ALL=OFF"
      "-DENABLE_CLICKHOUSE_LOCAL=ON"

      "-DENABLE_TESTS=OFF"
      "-DENABLE_UTILS=OFF"

      # CHDB flags
      "-DENABLE_PYTHON=ON"

      "-DENABLE_PARQUET=1"
      "-DENABLE_PROTOBUF=1"
      "-DENABLE_THRIFT=1"
      "-DARROW_JEMALLOC=0"

      "-DENABLE_HDFS=0"
      "-DENABLE_GSASL_LIBRARY=0"
      "-DENABLE_KRB5=0"

      "-DENABLE_PROMETHEUS_PROTOBUFS=0"
    ];
  };
}

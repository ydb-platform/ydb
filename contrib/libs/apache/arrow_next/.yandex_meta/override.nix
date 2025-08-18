pkgs: attrs: with pkgs; with attrs; rec {
  version = "20.0.0";

  src = fetchFromGitHub {
    owner = "apache";
    repo = "arrow";
    rev = "apache-arrow-${version}";
    sha256 = "sha256-JFPdKraCU+xRkBTAHyY4QGnBVlOjQ1P5+gq9uxyqJtk=";
  };

  patches = [ ];

  preConfigure = "";

  buildInputs = [
    boost
    brotli
    flatbuffers
    gflags
    lz4
    protobuf
    rapidjson
    re2
    snappy
    thrift
    utf8proc
    zlib
    zstd
    python3
    python3Packages.numpy
  ];


  cmakeFlags = [
      # It turns out arrow-cpp CMakeLists doesn"t support default MinSizeRel
      "-DCMAKE_BUILD_TYPE=Release"
      # Python requires compatible version for later usage,
      # setup this when needed for the bindings.
      "-DARROW_PYTHON=ON"
      "-DARROW_FILESYSTEM=OFF"
      "-DARROW_PLASMA=OFF"
      "-DARROW_PLASMA_JAVA_CLIENT=OFF"
      "-DARROW_HDFS=OFF"
      # Before enabling jemalloc see JEMALLOC_MANGLE define inside arrow code
      "-DARROW_JEMALLOC=OFF"
      "-DARROW_MIMALLOC=OFF"
      # TODO: support compatible runtimes on corresponding archs (? like at openal and ffmpeg)
      "-DARROW_SIMD_LEVEL=NONE"
      "-DARROW_RUNTIME_SIMD_LEVEL=NONE"
      # Hack to avoid generation of platform-specific sources and removing them when
      # no runtime is selected (see above todo).
      "-DARROW_CPU_FLAG=none"
      # Needs fixing several compilation errors and googletest provides usage
      "-DARROW_BUILD_TESTS=OFF"
      "-DARROW_COMPUTE=ON"
      "-DARROW_ORC=ON"
      "-DARROW_PARQUET=ON"
      "-DARROW_CSV=ON"
      "-DARROW_USE_GLOG=OFF"
      "-DARROW_WITH_BROTLI=ON"
      "-DARROW_WITH_BACKTRACE=OFF"

      # build only C++ part of the library
      "../cpp"
  ];

   sourceRoot = "source";
}

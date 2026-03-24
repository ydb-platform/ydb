self: super: with self; {
  apache-orc = stdenv.mkDerivation rec {
    name = "orc";
    version = "2.3.0";

    src = fetchFromGitHub {
      owner = "apache";
      repo = "orc";
      rev = "rel/release-${version}";
      sha256 = "sha256-QQdRzwmUF1Qwxg53kJv1Q6yFuHqSrLYwUxKt+6wK9Hs=";
    };

    patches = [];

    nativeBuildInputs = [
      cmake
      ninja
    ];

    buildInputs = [
      lz4
      protobuf
      snappy
      zlib
      zstd
    ];

    cmakeFlags = [
      "-DLZ4_HOME=${lz4.dev}"
      "-DPROTOBUF_HOME=${protobuf}"
      "-DSNAPPY_HOME=${snappy.dev}"
      "-DZLIB_HOME=${zlib.dev}"
      "-DZSTD_HOME=${zstd.dev}"
      "-DBUILD_CPP_TESTS=OFF"
      "-DBUILD_JAVA=OFF"
      "-DBUILD_TOOLS=OFF"
    ];
  };
}

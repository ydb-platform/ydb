self: super: with self; {
  apache-orc = stdenv.mkDerivation rec {
    name = "orc";
    version = "2.2.1";

    src = fetchFromGitHub {
      owner = "apache";
      repo = "orc";
      rev = "rel/release-${version}";
      sha256 = "sha256-H7nowl2pq31RIAmTUz15x48Wc99MljFJboc4F7Ln/zk=";
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
      "-DBUILD_JAVA=OFF"
      "-DBUILD_CPP_TESTS=OFF"
    ];
  };
}

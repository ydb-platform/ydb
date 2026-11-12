pkgs: attrs: with pkgs; with attrs; rec {
  version = "36.2";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-sY9Pmy6KMJ5k/GdQSAF7vsviLJYPaArMKJ3deb++0wM=";
  };

  patches = [];

  cmakeFlags = [
    # protobuf
    "-Dprotobuf_VERSION=${version}"
    "-Dutf8_range_ENABLE_TESTS=OFF"
    "-Dutf8_range_ENABLE_INSTALL=OFF"
  ];
}

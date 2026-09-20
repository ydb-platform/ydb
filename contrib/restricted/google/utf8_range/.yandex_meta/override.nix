pkgs: attrs: with pkgs; with attrs; rec {
  version = "36.0";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-VGXFfqLm7IEJ9MQpMYhdVW5qPZbrYZ6q+0Y1TqQkjks=";
  };

  patches = [];

  cmakeFlags = [
    # protobuf
    "-Dprotobuf_VERSION=${version}"
    "-Dutf8_range_ENABLE_TESTS=OFF"
    "-Dutf8_range_ENABLE_INSTALL=OFF"
  ];
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "34.0";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-qtSP3I7RA7F6tKe+VfGd+oNXMT5HZ/Xl7u4vkefNxX4=";
  };

  patches = [];

  cmakeFlags = [
    # protobuf
    "-Dprotobuf_VERSION=${version}"
    "-Dutf8_range_ENABLE_TESTS=OFF"
    "-Dutf8_range_ENABLE_INSTALL=OFF"
  ];
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "35.1";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-nif9xjd+3ASR2pvvSXkzTEWoKi2oKLzV9gMQ3EevBVk=";
  };

  patches = [];

  cmakeFlags = [
    # protobuf
    "-Dprotobuf_VERSION=${version}"
    "-Dutf8_range_ENABLE_TESTS=OFF"
    "-Dutf8_range_ENABLE_INSTALL=OFF"
  ];
}

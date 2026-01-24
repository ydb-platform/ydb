pkgs: attrs: with pkgs; with attrs; rec {
  version = "33.3";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-why+EKsH19jSj6LmlUySidx6shbvpItVjWq5deH3EXw=";
  };

  patches = [];

  cmakeFlags = [
    # protobuf
    "-Dprotobuf_VERSION=${version}"
    "-Dutf8_range_ENABLE_TESTS=OFF"
    "-Dutf8_range_ENABLE_INSTALL=OFF"
  ];
}

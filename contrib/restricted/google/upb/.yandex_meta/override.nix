pkgs: attrs: with pkgs; with attrs; rec {
  version = "27.5";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-wUXvdlz19VYpFGU9o0pap/PrwE2AkopLZJVUqfEpJVI=";
  };

  buildInputs = [
    abseil-cpp
    zlib
  ];

  cmakeFlags = [
    "-Dprotobuf_ABSL_PROVIDER=package"

    # Building libupb is nested under BUILD_PROTOBUF_BINARIES
    "-Dprotobuf_BUILD_LIBUPB=ON"
    "-Dprotobuf_BUILD_PROTOBUF_BINARIES=ON"

    "-Dprotobuf_BUILD_TESTS=OFF"
    "-Dprotobuf_BUILD_EXAMPLES=OFF"
    "-Dprotobuf_BUILD_PROTOC_BINARIES=OFF"
    "-Dprotobuf_BUILD_LIBPROTOC=OFF"
  ];
}

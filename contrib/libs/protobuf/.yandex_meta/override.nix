pkgs: attrs: with pkgs; with attrs; rec {
  version = "22.5";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-NMEij1eFg9bMVlNSOg1WJbXK0teeSVev9UUNxiC1AC0=";

    # FIXME: import protobuf against abseil-cpp from buildInputs
    fetchSubmodules = true;
  };

  patches = [
    ./unversion-protoc.patch
  ];

  buildInputs = [
    # FIXME: import protobuf against abseil-cpp from buildInputs
    # abseil-cpp
    zlib
  ];

  cmakeFlags = [
    # FIXME: import protobuf against abseil-cpp from buildInputs
    # "-Dprotobuf_ABSL_PROVIDER=package

    # Build shared libs for proper PEERDIR detection
    "-Dprotobuf_BUILD_SHARED_LIBS=ON"
    "-Dprotobuf_BUILD_STATIC_LIBS=OFF"
    "-Dprotobuf_BUILD_TESTS=OFF"
  ];
}


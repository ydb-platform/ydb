pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.22.5";
  passthru.version = version;

  src = fetchFromGitHub {
    owner = "protocolbuffers";
    repo = "protobuf";
    rev = "v${version}";
    hash = "sha256-NMEij1eFg9bMVlNSOg1WJbXK0teeSVev9UUNxiC1AC0=";

    fetchSubmodules = true;
  };

  patches = [
    ./unversion-protoc.patch
  ];
  cmakeFlags = [
    "-Dprotobuf_BUILD_SHARED_LIBS=OFF"
    "-DBUILD_SHARED_LIBS=OFF"
  ];
}


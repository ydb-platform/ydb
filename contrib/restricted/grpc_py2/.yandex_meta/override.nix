pkgs: attrs: with pkgs; rec {
  version = "1.54.3";
  pname = "grpc";
  src = fetchFromGitHub {
    owner = "grpc";
    repo = "grpc";
    rev = "v${version}";
    hash = "sha256-UdQrBTNNfpoFYN6O92aUMhZEdfZZ3hqLp4lJMPjy7tM=";
    fetchSubmodules = true;
  };

  patches = [];

  buildInputs = [
    openssl
    protobuf
    libnsl
  ];

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=ON"

    "-DgRPC_ZLIB_PROVIDER=package"
    "-DgRPC_CARES_PROVIDER=package"
    "-DgRPC_RE2_PROVIDER=package"
    "-DgRPC_SSL_PROVIDER=package"
    "-DgRPC_PROTOBUF_PROVIDER=package"
    "-DgRPC_ABSL_PROVIDER=package"

    # Building gRPC test takes too long.
    # We do not need them in Arcadia, hence there is no sense to build them under nix
    "-DgRPC_BUILD_TESTS=OFF"
    "-DgRPC_BUILD_CSHARP_EXT=OFF"
    "-DgRPC_BACKWARDS_COMPATIBILITY_MODE=OFF"
    "-DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF"
    "-DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF"
    "-DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF"
    "-DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF"
    "-DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF"
  ];
}

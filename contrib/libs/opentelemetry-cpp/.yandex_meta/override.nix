pkgs: attrs: with pkgs; rec {
  version = "1.25.0";

  src = fetchFromGitHub {
    owner = "open-telemetry";
    repo = "opentelemetry-cpp";
    rev = "v${version}";
    hash = "sha256-/iuAv8UcRYkuQjV6Hgs1HHqW0SSVH+By5narF+vy7JU=";
  };

  patches = [];

  nativeBuildInputs = [ cmake ninja pkg-config git cacert];

  buildInputs = [
    curl
    grpc
    nlohmann_json
    openssl
    protobuf
  ];

  cmakeFlags = [
    "-DCMAKE_CXX_STANDARD=20"
    "-DWITH_STL=ON"

    "-DBUILD_TESTING=OFF"
    "-DWITH_EXAMPLES=OFF"
    "-DWITH_BENCHMARK=OFF"

    "-DWITH_OTLP_HTTP=ON"
    "-DWITH_OTLP_GRPC=ON"

    "-DWITH_ABI_VERSION_1=OFF"
    "-DWITH_ABI_VERSION_2=ON"
  ];
}

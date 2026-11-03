pkgs: attrs: with pkgs; rec {
  version = "1.29.0";

  src = fetchFromGitHub {
    owner = "open-telemetry";
    repo = "opentelemetry-cpp";
    rev = "v${version}";
    hash = "sha256-md4JwlqxMM7zAz0/v9/S3hSj1S0cwXXn4sWT8UWK0Dw=";
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
    "-DWITH_OTLP_UTF8_VALIDITY=OFF"

    "-DWITH_ABI_VERSION_1=OFF"
    "-DWITH_ABI_VERSION_2=ON"
  ];
}

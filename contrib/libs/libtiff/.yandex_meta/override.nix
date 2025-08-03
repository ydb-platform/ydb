pkgs: attrs: with pkgs; rec {
  version = "4.7.0";

  src = fetchFromGitLab {
    owner = "libtiff";
    repo = "libtiff";
    rev = "v${version}";
    hash = "sha256-SuK9/a6OUAumEe1kz1itFJGKxJzbmHkBVLMnyXhIwmQ=";
  };

  patches = [];
  postPatch = "";

  nativeBuildInputs = [
    cmake
  ];

  buildInputs = [
    libjpeg
    libwebp
    xz
    zlib
    zstd
  ];

  cmakeFlags = [
    "-Dlzma=ON"
    "-Dlibdeflate=OFF"
  ];
}

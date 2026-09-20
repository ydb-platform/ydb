pkgs: attrs: with pkgs; rec {
  version = "4.7.2";

  src = fetchFromGitLab {
    owner = "libtiff";
    repo = "libtiff";
    rev = "v${version}";
    hash = "sha256-60Lpg5WRfWMzlOoOUA+C6KLlYIZ+3BjXidOVqv4M2GA=";
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

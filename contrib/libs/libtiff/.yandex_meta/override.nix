pkgs: attrs: with pkgs; rec {
  version = "4.6.0";

  src = fetchFromGitLab {
    owner = "libtiff";
    repo = "libtiff";
    rev = "v${version}";
    hash = "sha256-qCg5qjsPPynCHIg0JsPJldwVdcYkI68zYmyNAKUCoyw=";
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
  ];
}

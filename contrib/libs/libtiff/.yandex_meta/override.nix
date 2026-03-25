pkgs: attrs: with pkgs; rec {
  version = "4.7.1";

  src = fetchFromGitLab {
    owner = "libtiff";
    repo = "libtiff";
    rev = "v${version}";
    hash = "sha256-UiC6s86i7UavW86EKm74oPVlEacvoKmwW7KETjpnNaI=";
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

pkgs: attrs: with pkgs; rec {
  version = "4.3.5";

  src = fetchFromGitHub {
    owner = "zeromq";
    repo = "libzmq";
    rev = "v${version}";
    sha256 = "sha256-q2h5y0Asad+fGB9haO4Vg7a1ffO2JSb7czzlhmT3VmI=";
  };

  patches = [];

  cmakeFlags = [
    "-DBUILD_SHARED=OFF"
    "-DENABLE_CURVE=OFF"
    "-DWITH_LIBSODIUM=OFF"
  ];
}

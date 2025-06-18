pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.3";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-LQkva2w2TlEdLFj6xLjCY+2wVWsM3NVpGS9irYVrskA=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

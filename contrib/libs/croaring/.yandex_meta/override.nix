pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "5.1.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-CyjXE4cDPp8pmVqjgJFzLiVgR1I0lv2PEusS659KKP4=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

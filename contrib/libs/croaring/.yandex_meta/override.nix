pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.4.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-e4cNsa8r6/bNCLb4x+c8maNN1IVf8u/yOhHF4Mu7KJk=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-Se/m+qcYwZu1Bp5F2dcWacHYe4awX7EclB1iChTBkYE=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

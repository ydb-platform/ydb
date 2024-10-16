pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.2.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-PzwtQDAsnRGIjeb3Ax6qqXtdEqtwaCWsj6g46J3Oqm0=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.7.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-YXEEiWbbP6G7x/rQiihAq20OEMxJNSgky+JTEaKlNDU=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

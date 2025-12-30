pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.5.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-hzEI/PJSE1B7pQLCUF4aRh+cTgX7wtJYI4QF07OlDoo=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

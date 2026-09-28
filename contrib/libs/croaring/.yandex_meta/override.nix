pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "5.2.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-BDQpqRlle9mjlybOjxctwxkP5rQl2y673EnpthjFbBA=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

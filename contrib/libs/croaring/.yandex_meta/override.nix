pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.4.3";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-1qR6uo/NYxwM99i7Ib9dSbD4k8fN2ZmrWh39pIQNJv4=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

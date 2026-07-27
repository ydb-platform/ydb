pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.7.2";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-WSEMkXkR6diE5CV3gQ3tUAodLqNsWmBmrGyXOKg4CJA=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

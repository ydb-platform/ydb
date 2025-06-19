pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.4";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-w3GU91mOjbU5PzDy7L7Pu4fY2pDloFo6UBllYkLII2k=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

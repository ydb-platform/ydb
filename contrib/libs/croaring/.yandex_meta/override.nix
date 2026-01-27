pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.5.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-o+PLqXMRyCGgMsdg3eAbxu3qD4QBF9NHFBFJ2/BLCXs=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

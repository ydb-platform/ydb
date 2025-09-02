pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.6";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-8kXrPsjNkhx8pvA9A1ZzESoj5GdMSf4INjazU9jT3Ek=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

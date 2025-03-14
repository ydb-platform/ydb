pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.2.3";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-1yklwZj12yeGg8a/oss4EUHj8eezhKuo4PUltVdaXaM=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

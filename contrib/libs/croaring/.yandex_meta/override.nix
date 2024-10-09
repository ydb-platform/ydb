pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.1.7";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-vven8MrN0GRI3lz4zgquPE+5nBYxVaeY9SalKweux90=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

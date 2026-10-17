pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "5.2.2";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-ZipK1pNdxNaEAVkFygH6pwOHXvQWfGKVk1S6onKpvPE=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

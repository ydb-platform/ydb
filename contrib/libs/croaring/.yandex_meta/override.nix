pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.6.0";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-YsNhuQp9pDe4OJTfUz1LsRevQPrfswW8fWRGzDu4FZg=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

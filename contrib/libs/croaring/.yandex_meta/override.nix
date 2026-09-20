pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "5.1.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-I9ioukybY8NXBFmQd9xFyrd9Ye2FuGTewuX1pCQscQM=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

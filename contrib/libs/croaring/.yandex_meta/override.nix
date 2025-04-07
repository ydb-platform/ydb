pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-c4o8AMCtDGLChXxJKJyxkWhuYu7axqLb2ce8IOGk920=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

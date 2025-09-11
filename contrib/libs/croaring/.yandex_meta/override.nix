pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.9";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-1mUMQRC8xQEBozL9T1QbWI/E++Waju6K/9FtBOp9Q4U=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

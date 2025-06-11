pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.2";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-IAQ6w3J/EbkPBGF5AH7jG7JqdCnrRY55+7df33Sbe78=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

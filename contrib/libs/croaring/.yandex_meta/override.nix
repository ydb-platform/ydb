pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.10";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-j+qIuIgBWsAuAST8wstsdW926UZ0GRI3w1zEDFttyIY=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.12";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-A2cWi/clh/mgKa192IqrT/qDL26jf8iN2d1if3ugIY0=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

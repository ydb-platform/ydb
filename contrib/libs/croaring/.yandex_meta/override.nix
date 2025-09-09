pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.3.7";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-GstvfwHnnNYfY+DJvddjmv3SonZYAlH539KSad/W9ZQ=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

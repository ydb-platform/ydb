pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.2.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-qOFkDu0JM+wBIlGGyewojicCp2pmtr643J3dW6el+O4=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

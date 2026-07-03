pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.7.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-FZP+RTV4pcj9pzDq3G2+sWeJnkh9WnW3Atd0CC9zDCk=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

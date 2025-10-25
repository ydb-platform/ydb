pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.4.2";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-ACFcbg+IdpRIQlqsqb1wtIT+N7zOW9fR+faDajSUM8c=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

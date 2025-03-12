pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.2.2";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-QaCYtuUU7hZ03x/bPEGG7jUlzbRNMGwi9s/hIHyd3U4=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

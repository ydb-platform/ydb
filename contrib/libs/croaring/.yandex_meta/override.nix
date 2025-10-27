pkgs: attrs: with pkgs; with attrs; rec {
  pname = "croaring";
  version = "4.4.1";

  src = fetchFromGitHub {
    owner = "RoaringBitmap";
    repo = "CRoaring";
    rev = "v${version}";
    hash = "sha256-IpDmqosQYISfDE0o28bJqe4omRs+MgyJyjQJaLnAEso=";
  };

  patches = [];

  cmakeFlags = [
    "-DENABLE_ROARING_TESTS=OFF"
  ];
}

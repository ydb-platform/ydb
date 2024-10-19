pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.2";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-KGNOc3l+bWifWFnBOLtL0ASJTuYayBOAE6g6gNsl5xk=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

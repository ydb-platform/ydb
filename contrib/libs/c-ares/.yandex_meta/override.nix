pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.3";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-vCVS0kr/l6iRVWRnRM+J8aWheDEqEVVekjF8f4Naj/0=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

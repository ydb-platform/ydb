pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.33.1";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-aLHTvyAcCNY0mr96D68Ubf4RL0zmU7ARSdjQ59+2Pv0=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

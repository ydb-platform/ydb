pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.5";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-MeQ4eqt7QyRD7YVomXR+fwBzraiYe2s2Eozz0sE8Xgo=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

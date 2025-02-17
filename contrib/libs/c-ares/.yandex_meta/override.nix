pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.4";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-6xJSo4ptXAKFwCUBRAji8DSqkxoIL6lpWvnDOM1NQNg=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "6.0.2";

  src = fetchFromGitHub {
    owner = "ArthurSonzogni";
    repo = "FTXUI";
    rev = "v${version}";
    hash = "sha256-VvP1ctFlkTDdrAGRERBxMRpFuM4mVpswR/HO9dzUSUo=";
  };

  patches = [];

  cmakeFlags = [
    "-DFTXUI_BUILD_EXAMPLES=OFF"
    "-DFTXUI_BUILD_DOCS=OFF"
    "-DFTXUI_BUILD_TESTS=OFF"
  ] ++ lib.optionals (!stdenv.isLinux && !stdenv.isDarwin) [
    "-DUNICODE=ON"
    "-D_UNICODE=ON"
  ];
}

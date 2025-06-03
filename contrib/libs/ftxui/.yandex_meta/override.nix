pkgs: attrs: with pkgs; with attrs; rec {
  version = "6.0.2";

  src = fetchFromGitHub {
    owner = "ArthurSonzogni";
    repo = "FTXUI";
    rev = "v${version}";
    hash = "sha256-VvP1ctFlkTDdrAGRERBxMRpFuM4mVpswR/HO9dzUSUo=";
  };

  patches = [];
}

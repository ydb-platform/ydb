pkgs: attrs: with pkgs; rec {
  version = "8.0.2";

  src = fetchFromGitHub {
    owner = "qhull";
    repo = "qhull";
    rev = "v${version}";
    hash = "sha256-djUO3qzY8ch29AuhY3Bn1ajxWZ4/W70icWVrxWRAxRc=";
  };
}

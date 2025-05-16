pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.17";

  src = fetchFromGitHub {
    owner = "mm2";
    repo = "Little-CMS";
    rev = "lcms${version}";
    hash = "sha256-1krm+TvdhWLtbXzgeC/mdOuem7jV9U9nIEz6Nn/G1X0=";
  };

  patches = [];
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "9.2.1";

  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "llhttp";
    rev = "release/v${version}";
    hash = "sha256-cnEp7Ds32bqu3jeUU/rqJOr/VW3KNmJU4pmNNaTpXRs=";
  };

  patches = [];
}

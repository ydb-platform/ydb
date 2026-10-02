pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.13.15";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-/bONx3PzJqt0HgDuFjtKGWPGe6Rr+pfuKqnynNYHc5I=";
  };

  patches = [];
  postPatch = "";
}

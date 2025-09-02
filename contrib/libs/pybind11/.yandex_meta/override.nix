pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.13.6";

  src = fetchFromGitHub {
    owner = "pybind";
    repo = "pybind11";
    rev = "v${version}";
    hash = "sha256-SNLdtrOjaC3lGHN9MAqTf51U9EzNKQLyTMNPe0GcdrU=";
  };

  patches = [];

  postPatch = "";
}

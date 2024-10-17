pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.5.2";

  src = fetchFromGitHub {
    owner = "uclouvain";
    repo = "openjpeg";
    rev = "v${version}";
    sha256 = "sha256-mQ9B3MJY2/bg0yY/7jUJrAXM6ozAHT5fmwES5Q1SGxw=";
  };

  patches = [];
}

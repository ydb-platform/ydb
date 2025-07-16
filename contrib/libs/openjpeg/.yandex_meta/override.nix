pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.5.3";

  src = fetchFromGitHub {
    owner = "uclouvain";
    repo = "openjpeg";
    rev = "v${version}";
    sha256 = "sha256-ONPahcQ80e3ahYRQU+Tu8Z7ZTARjRlpXqPAYpUlX5sY=";
  };

  patches = [];
}

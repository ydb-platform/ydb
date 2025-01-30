pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.16";

  src = fetchFromGitHub {
    owner = "mm2";
    repo = "Little-CMS";
    rev = "lcms${version}";
    hash = "sha256-pI+ZyM9UfiW0/GLk+gsoJuRQ1Nz3WRfSCHnwkFPBtzc=";
  };

  patches = [];
}

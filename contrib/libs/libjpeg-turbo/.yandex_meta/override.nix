pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.1.4";

  src = fetchFromGitHub {
    owner = "libjpeg-turbo";
    repo = "libjpeg-turbo";
    rev = "${version}";
    hash = "sha256-1NRoVIL3zXX1D6iOf2FCrwBEcDW7TYFbdIbCTjY1m8Q=";
  };

  patches = [];
}

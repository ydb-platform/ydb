pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.5.4";

  src = fetchFromGitHub {
    owner = "uclouvain";
    repo = "openjpeg";
    rev = "v${version}";
    sha256 = "sha256-HSXGdpHUbwlYy5a+zKpcLo2d+b507Qf5nsaMghVBlZ8=";
  };

  patches = [];
}

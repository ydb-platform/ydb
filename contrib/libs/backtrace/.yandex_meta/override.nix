pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-04-10";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "793921876c981ce49759114d7bb89bb89b2d3a2d";
    hash = "sha256-jOqr6JfkoAmbeydyEZ/R12W/lqf1wno4kdvqfCqdUmw=";
  };

  patches = [];
}

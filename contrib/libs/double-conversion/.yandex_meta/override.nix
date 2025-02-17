pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.3.0";

  src = fetchFromGitHub {
    owner = "google";
    repo = "double-conversion";
    rev = "v${version}";
    hash = "sha256-DkMoHHoHwV4p40IINEqEPzKsCa0LHrJAFw2Yftw7zHo=";
  };

  patches = [];
}


pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.1.43";

  nativeBuildInputs = [ pkg-config autoreconfHook ];

  src = fetchFromGitLab {
    domain = "gitlab.gnome.org";
    owner = "GNOME";
    repo = "libxslt";
    rev = "v${version}";
    hash = "sha256-hOIkSFY04j49rbdfsEEajnkD0l9UDD677d8aHRhQDJs=";
  };
}

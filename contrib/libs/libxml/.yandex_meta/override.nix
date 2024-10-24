pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.9.13";

  src = fetchFromGitLab {
    domain = "gitlab.gnome.org";
    owner = "GNOME";
    repo = "libxml2";
    rev = "v${version}";
    sha256 = "1rm2bisnvr5h59vbh7cbjpvf6jy8p85zz812bnclhs0wziglh2ha";
  };

  passthru = {};
  patches = [];

  nativeBuildInputs = [ pkg-config autoreconfHook ];
}

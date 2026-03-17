pkgs: attrs: with pkgs; rec {
  # last version, which works with current arcadia glib=2.38:
  version = "1.55.5";

  src = fetchFromGitLab {
    domain = "gitlab.gnome.org";
    owner = "GNOME";
    repo = "pango";
    rev = "${version}";
    hash = "sha256-9x6IWnoJOs4YfdF8YY2c5TwNLlFOxVql42vem/84WTQ=";
  };

  patches = [];

  # do not build against libthai
  buildInputs = [ fribidi ];
}

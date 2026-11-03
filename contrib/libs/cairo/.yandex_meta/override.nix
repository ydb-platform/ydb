pkgs: attrs: with pkgs; rec {
  version = "1.18.2";

  src = fetchFromGitLab {
    domain = "gitlab.freedesktop.org";
    owner = "cairo";
    repo = "cairo";
    rev = version;
    hash = "sha256-raAHyMh6XW0yGA+dGogGjv9y7MlUwVIjJGMH5ErZnLs=";
  };

  patches = [];

  nativeBuildInputs = [
    gtk-doc
    meson
    ninja
    pkg-config
    python3
  ];

  propagatedBuildInputs = attrs.propagatedBuildInputs ++ [ glib ];

  mesonFlags = [
    "-Dgtk_doc=true"
    "-Dsymbol-lookup=disabled"
    "-Dspectre=disabled"

    "-Dglib=enabled"

    "-Dtests=disabled"
    "-Dxcb=disabled"
    "-Dxlib=disabled"
  ];
}

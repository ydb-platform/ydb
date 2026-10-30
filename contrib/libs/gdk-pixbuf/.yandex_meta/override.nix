pkgs: attrs: with pkgs; rec {
  version = "2.44.8";

  src = fetchFromGitLab {
    domain = "gitlab.gnome.org";
    owner = "GNOME";
    repo = "gdk-pixbuf";
    rev = version;
    hash = "sha256-ADR262TyNZjElwklyc8/Bt67F7+0vckLuW7fh8jGzyE=";
  };

  buildInputs = [
    glib
    libpng
    libjpeg
  ];

  mesonFlags = [
    "-Ddefault_library=static"

    "-Dandroid=disabled"
    "-Dglycin=disabled"
    "-Dbuiltin_loaders=png,jpeg"

    "-Dintrospection=disabled"
    "-Dgio_sniffing=false"
    "-Ddocumentation=false"
    "-Dman=false"
    "-Dinstalled_tests=false"
  ];

  patches = [];
}

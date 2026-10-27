pkgs: attrs: with pkgs; rec {
  pname = "glib";
  version = "2.72.2";

  src = fetchFromGitLab {
    domain = "gitlab.gnome.org";
    owner = "GNOME";
    repo = "GLib";
    rev = "${version}";
    hash = "sha256-p0o38Qq3BodArunpdAQ6Hkx794zr1KgqrBu+voiDxtw=";
  };

  buildInputs = [
    libffi
    libiconv
    libelf
    pcre
    zlib
  ];

  mesonFlags = [
    "-Ddefault_library=static"
    "-Dgtk_doc=false"
    "-Dnls=disabled"
    "-Dselinux=disabled"
    "-Dlibmount=disabled"
    "-Diconv=libc"
    "-Dtests=false"
  ];

  patches = [
    ./disable-fuzzing.patch
    ./disable-sched-getattr.patch
    ./disable-nls.patch
    ./disable-auxv.patch
  ];
}

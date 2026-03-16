self: super: with self; rec {
  name = "giflib";
  version = "5.2.2";

  src = fetchurl {
    url = "mirror://sourceforge/giflib/${name}-${version}.tar.gz";
    hash = "sha256-vn/70FfK3r4qoURUL9kMaDjGoIO16KkEi47jtmsp1fs=";
  };
}

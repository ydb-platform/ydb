pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.4.17";

  src = fetchurl {
    url = "mirror://gnu/m4/m4-${version}.tar.bz2";
    sha256 = "sha256-jk4fljkyE27UXc1a+wxuI36Wpvzc0qL6Z1UECFlQDXA=";
  };
}

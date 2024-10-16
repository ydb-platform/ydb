pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.42";

  src = fetchurl {
    url = "mirror://gnu/libidn/${pname}-${version}.tar.gz";
    hash = "sha256-1sGZ3NgG5P4nk2DLSwg0mg05Vg7VSP/RzK3ajN7LRyM=";
  };
}

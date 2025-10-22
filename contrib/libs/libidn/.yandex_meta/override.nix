pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.43";

  src = fetchurl {
    url = "mirror://gnu/libidn/${pname}-${version}.tar.gz";
    hash = "sha256-vcZiwS0EGyU50OY486bnQRMM2zOmRO80lpY6RDSC0WQ=";
  };
}

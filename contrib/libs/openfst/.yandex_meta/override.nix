pkgs: attrs: with pkgs; rec {
  version = "1.8.2";

  src = fetchurl {
    url = "http://www.openfst.org/twiki/pub/FST/FstDownload/openfst-${version}.tar.gz";
    hash = "sha256-3ph782JHIcXVujIa+VdRiY5PS7Qcijbi1k8GJ2Vti0I=";
  };

  patches = [];
}

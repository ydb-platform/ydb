pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.9.5";

  src = fetchurl {
    url = "http://download.osgeo.org/geos/geos-${version}.tar.bz2";
    sha256 = "sha256-xsmu36iGT7RLp4kRQIRCOCv9BpDPLUCRrjgFyGN4kDY=";
  };
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.19";

  src = fetchFromGitHub {
    owner = "mm2";
    repo = "Little-CMS";
    rev = "lcms${version}";
    hash = "sha256-kbzhDXSfYhejE5gu7rd8BLYw/DF4bwNDPLxp5ob9HQI=";
  };

  patches = [];
}

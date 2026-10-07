pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.19.1";

  src = fetchFromGitHub {
    owner = "mm2";
    repo = "Little-CMS";
    rev = "lcms${version}";
    hash = "sha256-srFJSbPmciL2x2NXz8xggaiiulVM6Sm72lb6nHSAhXI=";
  };

  patches = [];
}

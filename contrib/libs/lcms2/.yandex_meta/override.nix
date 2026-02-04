pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.18";

  src = fetchFromGitHub {
    owner = "mm2";
    repo = "Little-CMS";
    rev = "lcms${version}";
    hash = "sha256-y03yCqnHE7sSHyKbE2pl6P7igV5HZzwV6+bqleOAPQA=";
  };

  patches = [];
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.2";

  src = fetchFromGitHub {
    owner = "madler";
    repo = "zlib";
    rev = "v${version}";
    hash = "sha256-Sthd9RsydSLaITNlBp6g1X35WKZdS4h7gr0QhRqdGoI=";
  };
}

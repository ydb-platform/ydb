pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.4.2";

  src = fetchFromGitHub {
    owner = "intel";
    repo = "hyperscan";
    rev = "v${version}";
    hash = "sha256-tzmVc6kJPzkFQLUM1MttQRLpgs0uckbV6rCxEZwk1yk=";
  };

  patches = [
    ./cmake.patch
  ];
}

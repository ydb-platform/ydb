pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.3.1";

  src = fetchFromGitHub {
    owner = "google";
    repo = "double-conversion";
    rev = "v${version}";
    hash = "sha256-M80H+azCzQYa4/gBLWv5GNNhEuHsH7LbJ/ajwmACnrM=";
  };

  patches = [];
}


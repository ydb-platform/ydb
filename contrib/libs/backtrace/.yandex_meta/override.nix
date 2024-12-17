pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-11-29";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "1db85642e3fca189cf4e076f840a45d6934b2456";
    hash = "sha256-hRZqnro0fXFrcyMiC7k5Ztm0qckRV23UbzpmCThQ3Y4=";
  };

  patches = [];
}

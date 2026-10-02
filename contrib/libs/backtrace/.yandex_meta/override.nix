pkgs: attrs: with pkgs; with attrs; rec {
  version = "2026-09-03";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "0b9b49cf4a2c9229fc052d6716e1528b2f23e91a";
    hash = "sha256-F++GLeh70pY+CRHs+Ibi+9Bzh8/b9rs4vQrRef7rDCo=";
  };

  patches = [];
}

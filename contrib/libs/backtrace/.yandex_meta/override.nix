pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-08-05";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "86885d14049fab06ef8a33aac51664230ca09200";
    hash = "sha256-QuskJe9wCVGWF3iSK9GvKLXhXLbcLT9xwKoiKf9aPGs=";
  };

  patches = [];
}

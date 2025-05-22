pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.2.2";

  src = fetchFromGitHub {
    owner = "google";
    repo = "snappy";
    rev = version;
    hash = "sha256-bMZD8EI9dvDGupfos4hi/0ShBkrJlI5Np9FxE6FfrNE=";
  };

  patches = [];
}

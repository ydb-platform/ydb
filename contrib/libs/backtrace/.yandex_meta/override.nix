pkgs: attrs: with pkgs; with attrs; rec {
  version = "2026-06-02";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "549b81b43b46c0f361680561a626bf0e7b79dcbd";
    hash = "sha256-3FRJJPk8gpHzk6gy2sit14X+mFqipZtxWYSJ7b0kPzo";
  };

  patches = [];
}

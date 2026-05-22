pkgs: attrs: with pkgs; with attrs; rec {
  version = "2026-05-04";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "96664e69b1ecdb76e824be1d9e8f475b76dd08cf";
    hash = "sha256-+tV6W8SnFWKweAASvFfb+i6bz73ssVGikNhVpq3YbT4=";
  };

  patches = [];
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-04-08";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "f1104f3270095831df536a2539f4cc408365105c";
    hash = "sha256-+D6oI1EcHh/wMPFBhW5+tjo/8yrqumTv74W90v28BX0=";
  };

  patches = [];
}

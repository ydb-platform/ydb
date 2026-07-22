pkgs: attrs: with pkgs; with attrs; rec {
  version = "2026-07-06";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "6f8310e238fc3ce68f42f391cbe93fd156bb2c23";
    hash = "sha256-3FKgYcZXYYS9d9i7/PFslaDBZZVcr3Ud8pzMpYhsgbc=";
  };

  patches = [];
}

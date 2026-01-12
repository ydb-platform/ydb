pkgs: attrs: with pkgs; with attrs; rec {
  version = "2025-11-06";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "b9e40069c0b47a722286b94eb5231f7f05c08713";
    hash = "sha256-vi33Bhg2LT5uWN63PHkD8CaOjTXBwZhBwFFhaezJ0e4=";
  };

  patches = [];
}

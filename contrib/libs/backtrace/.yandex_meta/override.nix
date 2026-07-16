pkgs: attrs: with pkgs; with attrs; rec {
  version = "2026-06-27";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "5f4a6d4941de23180e7c412cc9ddf5ce440e3829";
    hash = "sha256-j85Wws+/Un4WuAUVXhTQoE9o2bwnBKHvRokWBJhDZCc=";
  };

  patches = [];
}

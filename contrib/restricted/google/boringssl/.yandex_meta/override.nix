pkgs: attrs: with pkgs; with attrs; rec {
  version = "0.20260413.0";

  src = fetchFromGitHub {
    owner = "google";
    repo = "boringssl";
    rev = "${version}";
    hash = "sha256-JFKQleui4nNmEsx4k5L7xhvEFh3Ne3MEPnHDSRqEwPc=";
  };

}

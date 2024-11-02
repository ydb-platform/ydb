pkgs: attrs: with pkgs; with attrs; rec {
  version = "2024-10-18";

  src = fetchFromGitHub {
    owner = "ianlancetaylor";
    repo = "libbacktrace";
    rev = "3d0be558448724ff2618b72249143aa774d594ad";
    hash = "sha256-4+ivxVvoPeEMOUagg0NolzlwHKEY2pKRXViLAAlkC+I=";
  };

  patches = [];
}

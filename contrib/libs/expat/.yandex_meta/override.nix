pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.8.1";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-XHQN6Wd29Furdu8VB1UokZMmIFKq4sCG+Aa8/KfSoi4=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

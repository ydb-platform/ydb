pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.8.0";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-cHELJnsn6S0vky4MK46pBGIgHqfl8c7p/TT58T5o+gI=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

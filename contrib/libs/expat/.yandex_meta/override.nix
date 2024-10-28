pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.6.3";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-xxjUNbkcJkCMzlMt5yNnnUl0pJ/pP3Z9F5qnlYQXLOQ=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

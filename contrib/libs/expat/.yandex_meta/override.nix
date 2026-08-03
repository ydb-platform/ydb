pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.8.2";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-YNrsTYgY4gyRDpYPZPmkCTOF1eKA7rCcyfS0jR/SItY=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

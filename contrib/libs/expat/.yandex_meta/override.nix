pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.7.1";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-fAJgHW3KIe5qtQ0ymRiyB8WBt05bMz8b3+JBibCpzQw=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

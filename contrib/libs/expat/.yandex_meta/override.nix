pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.6.4";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-ek8/3c8bKG+z7fIM+QCNsH7eoVGAt7z3bXBHZ3QjlS8=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

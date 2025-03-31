pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.7.0";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-5is+ZwHM+tmKaVzDgO20wCJKJafnwxxRjNMDsv2qnYY=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

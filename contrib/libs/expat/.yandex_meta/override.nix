pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.7.4";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-0Zi2/KzhMa4hL5n6RwVMU1r7Hbvmomyx3QaGLw7Ub/I=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

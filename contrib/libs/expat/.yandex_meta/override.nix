pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.8.3";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-ac18nAjPKoy5h/57OBNoTqDegvXPTZrx97QImw/LfwM=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

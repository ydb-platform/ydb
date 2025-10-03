pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.7.2";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-9lYSsDezTtyfrB/NzjTedKTjD3c09YoFytf9hcT5tJU=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

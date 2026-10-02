pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.8.4";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-FgHbGNG6MBP21AFUIKP0p/x0ia4tBej09uYyayy87ow=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

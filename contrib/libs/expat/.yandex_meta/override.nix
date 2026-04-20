pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.7.5";
  versionTag = "R_${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner = "libexpat";
    repo = "libexpat";
    rev = "${versionTag}";
    hash = "sha256-N+ECPJ/1X0I2J2JFJT5bpz9NGe1QeCjKnoIRClsQtfo=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  preConfigure = ''
    sh ./buildconf.sh
  '';

  sourceRoot = "source/expat";
}

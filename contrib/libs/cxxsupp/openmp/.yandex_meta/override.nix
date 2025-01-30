pkgs: attrs: with pkgs; with attrs; rec {
  pname = "openmp";
  version = "15.0.7";

  src = fetchFromGitHub {
    owner = "llvm";
    repo = "llvm-project";
    rev = "llvmorg-${version}";
    hash = "sha256-wjuZQyXQ/jsmvy6y1aksCcEDXGBjuhpgngF3XQJ/T4s=";
  };

  # This hack makes message-converter.pl script to not emit time on every build.
  preConfigure = ''
    substituteInPlace "runtime/tools/message-converter.pl" --replace "\" on \" . localtime() . " ""
  '';

  sourceRoot = "source/openmp";
}

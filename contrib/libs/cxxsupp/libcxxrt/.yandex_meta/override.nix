pkgs: attrs: with pkgs; rec {
  version = "2024-10-30";
  revision = "6f2fdfebcd6291d763de8b17740d636f01761890";

  src = fetchFromGitHub {
    owner = "libcxxrt";
    repo = "libcxxrt";
    rev = "${revision}";
    hash = "sha256-iUuIhwFg1Ys9DDoyDFTjEIlCVDdA1TACwtYXSRr5+2g=";
  };

  nativeBuildInputs = [ cmake ];
}

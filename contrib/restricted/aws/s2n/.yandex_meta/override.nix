pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.56";

  src = fetchFromGitHub {
    owner = "aws";
    repo = "s2n-tls";
    rev = "v${version}";
    hash = "sha256-VS/85qu0Dc3HSeD0DYm2f4ur+ZRPhb1Srf7BeK7Pdfk=";
  };

  patches = [];
}

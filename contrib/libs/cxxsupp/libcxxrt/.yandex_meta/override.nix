pkgs: attrs: with pkgs; rec {
  version = "2024-10-14";
  revision = "76435c4451aeb5e04e9500b090293347a38cef8d";

  src = fetchFromGitHub {
    owner = "libcxxrt";
    repo = "libcxxrt";
    rev = "${revision}";
    hash = "sha256-U7mq79/0xbyRr2+KUMKgEvyd2lfr3Q5GrByt/8J9sC8=";
  };

  nativeBuildInputs = [ cmake ];
}

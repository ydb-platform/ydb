self: super: with self; rec {
  version = "2.11.3";

  src = fetchFromGitHub {
    owner = "JuliaStrings";
    repo = "utf8proc";
    rev = "v${version}";
    hash = "sha256-DF2//R8Oc/+IEJuiG9+rTxQ7nltPcPqdCkzR4T7pUes=";
  };
}

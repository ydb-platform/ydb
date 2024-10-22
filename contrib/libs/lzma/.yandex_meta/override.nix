pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.6.3";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-2bxTxgDGlA0zPlfFs69bkuBGL44Se1ktSZCJ1Pt75I0=";
  };

  nativeBuildInputs = [ autoreconfHook ];
}

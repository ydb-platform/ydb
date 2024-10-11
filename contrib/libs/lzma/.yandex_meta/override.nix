pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.6.2";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-NB6EHOSAL4eMyhgSJqyJ10H9HgTUv5NFJpEQTEzUofo=";
  };

  nativeBuildInputs = [ autoreconfHook ];
}

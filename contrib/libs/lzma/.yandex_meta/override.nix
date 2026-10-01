pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.8.4";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-C4D4MB/1Pj57IUrLFkrf+cLs2rjtVO0V5uIRFORyidg=";
  };

  nativeBuildInputs = [ autoreconfHook ];

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
  ];
}

replace_date() {
	sed -e 's|// The file was generated from en_US.txt by message-converter.py on .* //|// The file was generated from en_US.txt by message-converter.py on Fri Jul 11 21:54:37 2025 (fixed date by patch) //|' -i $1	
}


replace_date "kmp_i18n_id.inc"
replace_date "kmp_i18n_default.inc"
